#!/usr/bin/env python3
"""
Сервер синхронизации Media Overlay.

Что делает: хозяин комнаты ("хост") присылает, какой трек играет и на какой он секунде;
остальные ("зрители") получают это и показывают у себя те же субтитры в такт. Сам звук и тексты
песен через сервер НЕ проходят - зрители берут тексты у себя (lrclib.net).

Только стандартная библиотека Python 3.8+, ничего устанавливать не нужно.

Запуск:   python server.py            (порт берётся из переменной окружения PORT, иначе 8080)

Протокол (JSON):
  POST /r/<комната>/host   заголовок X-Token: <секрет хоста>  - состояние трека от хоста
  POST /r/<комната>/bye    заголовок X-Token                  - хост ушёл
  GET  /r/<комната>/state?v=<id>&since=<версия>&wait=<сек>    - состояние для зрителя; "долгий опрос":
                           если версия не менялась, сервер держит ответ до wait секунд -
                           перемотка, пауза и смена трека доходят мгновенно, а в тишине трафика почти нет
  GET  /ping               - для замера задержки
  GET  /health             - для проверок хостинга
"""
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

MAX_ROOMS = 100            # комнат одновременно (защита от мусора на открытом сервере)
MAX_VIEWERS = 40           # зрителей в одной комнате
HOST_ALIVE = 10.0          # столько секунд без вестей от хоста - считаем, что его нет
HOST_TAKEOVER = 15.0       # чужой хост может занять комнату, только если прежний молчит дольше
VIEWER_ALIVE = 15.0        # зритель считается "в комнате", если опрашивал не позже этого
ROOM_TTL = 1800.0          # комната без активности удаляется через 30 минут
MAX_BODY = 4096            # байт в теле запроса хоста
MAX_WAIT = 20.0            # дольше этого долгий опрос не держим
SEEK_TOLERANCE = 0.8       # на столько секунд позиция может разойтись с ожидаемой, не считаясь перемоткой

ROOM_RE = re.compile(r"^[a-z0-9_-]{4,32}$")
TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
VID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

COND = threading.Condition()       # одна на всех: защищает ROOMS и будит долгие опросы
ROOMS = {}


class Room:
    def __init__(self, token):
        self.token = token
        self.state = None          # последнее состояние от хоста
        self.t_srv = 0.0           # когда оно получено (monotonic сервера)
        self.ver = 0               # растёт при каждом "событии": смена трека, пауза, перемотка, уход хоста
        self.last_host = 0.0
        self.bye = False
        self.viewers = {}          # id зрителя -> когда последний раз опрашивал
        self.touched = time.monotonic()

    def viewer_count(self, now):
        return sum(1 for t in self.viewers.values() if now - t < VIEWER_ALIVE)

    def host_alive(self, now):
        return (not self.bye) and self.state is not None and now - self.last_host < HOST_ALIVE


def _clean_state(d):
    def text(key, limit=200):
        v = d.get(key, "")
        return v[:limit] if isinstance(v, str) else ""

    def num(key, hi):
        try:
            v = float(d.get(key, 0.0))
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(hi, v)) if v == v else 0.0      # v == v отсекает NaN

    return {"artist": text("artist"), "title": text("title"), "album": text("album"), "text": text("text"),
            "duration": num("duration", 36000.0), "pos": num("pos", 36000.0),
            "playing": bool(d.get("playing"))}


def _purge(now):
    """Выбрасывает давно неактивные комнаты (вызывается под COND)."""
    for name in [n for n, r in ROOMS.items() if now - r.touched > ROOM_TTL and r.viewer_count(now) == 0]:
        del ROOMS[name]


def host_update(name, token, body):
    """-> (http_status, json). Принимает состояние от хоста."""
    now = time.monotonic()
    with COND:
        _purge(now)
        room = ROOMS.get(name)
        if room is None:
            if len(ROOMS) >= MAX_ROOMS:
                return 503, {"ok": False, "error": "server_full"}
            room = ROOMS[name] = Room(token)
        elif room.token != token:
            if not room.bye and now - room.last_host < HOST_TAKEOVER:
                return 403, {"ok": False, "error": "room_busy"}
            room.token = token                              # прежний хост пропал - комнату занимает новый
            room.state = None

        st = _clean_state(body)
        try:
            lat = max(0.0, min(2.0, float(body.get("lat", 0.0))))
        except (TypeError, ValueError):
            lat = 0.0
        if st["playing"]:
            st["pos"] = min(36000.0, st["pos"] + lat)       # к моменту прихода сюда позиция уже ушла вперёд

        prev = room.state
        changed = prev is None or room.bye
        if not changed:
            if any(prev[k] != st[k] for k in ("artist", "title", "album", "duration", "playing")):
                changed = True
            else:
                expected = prev["pos"] + ((now - room.t_srv) if prev["playing"] else 0.0)
                changed = abs(expected - st["pos"]) > SEEK_TOLERANCE
        room.state, room.t_srv, room.last_host, room.bye = st, now, now, False
        room.touched = now
        if changed:
            room.ver += 1
            COND.notify_all()
        return 200, {"ok": True, "ver": room.ver, "viewers": room.viewer_count(now)}


def host_bye(name, token):
    now = time.monotonic()
    with COND:
        room = ROOMS.get(name)
        if room is None or room.token != token:
            return 200, {"ok": True}
        room.bye = True
        room.ver += 1
        COND.notify_all()
        return 200, {"ok": True}


def viewer_state(name, vid, since, wait):
    """Состояние для зрителя; если ничего не изменилось с версии since - ждёт до wait секунд."""
    deadline = time.monotonic() + max(0.0, min(wait, MAX_WAIT))
    with COND:
        while True:
            now = time.monotonic()
            room = ROOMS.get(name)
            if room is not None and vid:
                if vid in room.viewers or len(room.viewers) < MAX_VIEWERS \
                        or room.viewer_count(now) < MAX_VIEWERS:
                    room.viewers[vid] = now
                room.touched = now
            ver = room.ver if room else 0
            if ver != since or now >= deadline:
                alive = room is not None and room.host_alive(now)
                return 200, {"ok": True, "exists": room is not None, "host": alive, "ver": ver,
                             "viewers": room.viewer_count(now) if room else 0,
                             "age": (now - room.t_srv) if alive else 0.0,
                             "state": room.state if alive else None}
            COND.wait(deadline - now)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    timeout = 60                       # закрывать простаивающие keep-alive-соединения
    server_version = "MediaOverlaySync/1"

    def log_message(self, *args):      # без шума в логах хостинга
        pass

    def _send(self, code, payload, ctype="application/json; charset=utf-8"):
        data = payload if isinstance(payload, bytes) else \
            (payload.encode("utf-8") if isinstance(payload, str) else json.dumps(payload).encode("utf-8"))
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _route(self):
        url = urlparse(self.path)
        parts = [p for p in url.path.split("/") if p]
        return parts, parse_qs(url.query)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        parts, q = self._route()
        if not parts:
            return self._send(200, "Media Overlay sync server: OK\n", "text/plain; charset=utf-8")
        if parts == ["health"]:
            return self._send(200, "ok", "text/plain; charset=utf-8")
        if parts == ["ping"]:
            return self._send(200, {"ok": True, "now": time.monotonic()})
        if len(parts) == 3 and parts[0] == "r" and parts[2] == "state":
            name = parts[1].lower()
            if not ROOM_RE.match(name):
                return self._send(400, {"ok": False, "error": "bad_room"})
            vid = (q.get("v", [""])[0])
            if vid and not VID_RE.match(vid):
                vid = ""
            try:
                since = int(q.get("since", ["-1"])[0])
                wait = float(q.get("wait", ["0"])[0])
            except ValueError:
                return self._send(400, {"ok": False, "error": "bad_params"})
            code, payload = viewer_state(name, vid, since, wait)
            return self._send(code, payload)
        self._send(404, {"ok": False, "error": "not_found"})

    def do_POST(self):
        parts, _ = self._route()
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if length < 0 or length > MAX_BODY:
            self.close_connection = True
            return self._send(413, {"ok": False, "error": "too_large"})
        raw = self.rfile.read(length) if length else b""          # тело всегда вычитываем до конца (keep-alive)
        if len(parts) == 3 and parts[0] == "r" and parts[2] in ("host", "bye"):
            name = parts[1].lower()
            token = self.headers.get("X-Token", "")
            if not ROOM_RE.match(name):
                return self._send(400, {"ok": False, "error": "bad_room"})
            if not TOKEN_RE.match(token):
                return self._send(400, {"ok": False, "error": "bad_token"})
            if parts[2] == "bye":
                return self._send(*host_bye(name, token))
            try:
                body = json.loads(raw.decode("utf-8") or "{}")
                if not isinstance(body, dict):
                    raise ValueError
            except ValueError:
                return self._send(400, {"ok": False, "error": "bad_json"})
            return self._send(*host_update(name, token, body))
        self._send(404, {"ok": False, "error": "not_found"})


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64


def make_server(port, host="0.0.0.0"):
    return Server((host, port), Handler)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    srv = make_server(port)
    print(f"Media Overlay sync server слушает порт {port}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
