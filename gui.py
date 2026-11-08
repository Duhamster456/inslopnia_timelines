#!/usr/bin/env python3
"""Browser GUI for timeline SQLite DBs.

    python gui.py [PATH ...] [--port 8780] [--open]

PATH is a folder (scanned recursively for *.db made by `timeline.py render sqlite`) or a single
timeline file: a `render sqlite` DB, a JSONL journal, or an inslopnia_harness `ledger.sqlite3`
(the last two are rendered into a cached DB on the fly). Default: current dir. More folders and
files can be opened from the page itself (path field, folder browser, system dialog, drag & drop).
Serves gui.html (a single-file page) on 127.0.0.1. Every render is produced by timeline.py itself
(imported as a module): HTML, text, validation, journal export. Stdlib only.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import string
import subprocess
import sys
import tempfile
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import timeline as TL  # noqa: E402  the utility itself

PAGE = HERE / "gui.html"
SKIP_DIRS = {".git", "node_modules", "__pycache__", "sessions", "pi-sessions", "workspace",
             ".venv", "venv", "site-packages"}
DB_EXT = (".db", ".sqlite", ".sqlite3")
FILE_EXT = DB_EXT + (".jsonl",)
LEDGER = "ledger.sqlite3"
MAX_ROWS = 2000
MAX_SCAN_DIRS = 20000          # per folder source: guards against opening a whole drive
MAX_UPLOAD = 512 * 1024 * 1024
CACHE = Path(tempfile.gettempdir()).resolve() / "timeline-gui"
DERIVED, UPLOADS = CACHE / "derived", CACHE / "uploads"
KIND_LABEL = {"db": "таймлайн-БД", "journal": "журнал", "harness": "прогон харнесса"}


# ----------------------------------------------------------------------------
# what a file is, and turning journals / harness ledgers into timeline DBs
# ----------------------------------------------------------------------------
def ro(p: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{p.resolve().as_posix()}?mode=ro", uri=True, timeout=5)


def sniff(p: Path) -> str | None:
    """'db' (made by `render sqlite`), 'journal' (JSONL), 'harness' (ledger.sqlite3) or None."""
    try:
        if TL.is_sqlite(p):
            con = ro(p)
            try:
                names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            finally:
                con.close()
            if {"sessions", "records"} <= names:
                return "db"
            if {"steps", "events", "kv"} <= names:
                return "harness"
            return None
        if p.suffix.lower() == ".jsonl":
            with open(p, encoding="utf-8-sig", errors="replace") as fh:
                for line in fh:
                    if line.strip():
                        rec = json.loads(line)
                        # pi transcripts also start with {"type":"session"}, but carry no numeric "t"
                        ok = isinstance(rec, dict) and rec.get("type") and isinstance(rec.get("t"), (int, float))
                        return "journal" if ok else None
    except (OSError, sqlite3.Error, ValueError):
        return None
    return None


_derive_lock = threading.Lock()


def derive(src: Path, kind: str) -> Path:
    """A timeline DB rendered from a journal or a harness ledger, cached by the source's mtime/size."""
    src = src.resolve()
    parts = [src]
    if kind == "harness":  # everything import_harness reads: ledger (+WAL), config snapshot, pi-sessions
        run = src.parent
        parts += [src.with_name(src.name + "-wal"), run / "config.snapshot.json",
                  *sorted((run / "pi-sessions").glob("*.jsonl"))]
    sig = "|".join(f"{q.name}:{q.stat().st_mtime_ns}:{q.stat().st_size}" for q in parts if q.exists())
    pk = hashlib.sha1(str(src).lower().encode()).hexdigest()[:10]
    out = DERIVED / f"{pk}-{hashlib.sha1(sig.encode()).hexdigest()[:10]}.db"
    with _derive_lock:
        if out.exists():
            return out
        recs = TL.import_harness(src.parent) if kind == "harness" else TL.Journal(src).records
        if not recs:
            raise ValueError("пустой журнал")
        m = TL.build_model(recs)
        if kind == "harness":
            sid = m["session"].get("task") or src.parent.name
        else:
            generic = src.stem.lower() in ("timeline", "journal", "session")
            sid = m["session"].get("task") or (src.parent.name if generic else src.stem)
        DERIVED.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".tmp")
        tmp.unlink(missing_ok=True)
        TL.render_sqlite(m, recs, tmp, sid, source_file=str(src), fresh=True)
        os.replace(tmp, out)
        for old in DERIVED.glob(pk + "-*.db"):  # stale renders of the same source
            if old != out:
                try:
                    old.unlink()
                except OSError:
                    pass
        return out


class NotFound(LookupError):
    """A DB id that is not under the opened sources (HTTP 404)."""


def content_disposition(filename: str) -> str:
    """attachment header that survives non-latin-1 names (RFC 6266 / 5987): ASCII fallback + UTF-8 name."""
    stem, dot, ext = filename.rpartition(".")
    if not dot:
        stem, ext = filename, ""
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", stem).strip("_") or "session"
    ascii_name = f"{safe}.{ext}" if ext else safe
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename, safe='')}"


def db_summary(p: Path):
    con = ro(p)
    try:
        if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sessions'").fetchone():
            return None
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT session_id, title, task, outcome, flag, started, duration_s, tokens, cost, "
                           "n_agents, n_nodes, nodes_done, nodes_failed, incidents, incidents_open, hyps, "
                           "hyps_confirmed, n_events FROM sessions ORDER BY started").fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def clean_path(raw: str) -> Path:
    s = (raw or "").strip().strip('"').strip("'").strip()
    if s.lower().startswith("file:///"):
        s = s[8:] if re.match(r"^file:///[A-Za-z]:", s) else s[7:]
    if not s:
        raise ValueError("пустой путь")
    p = Path(os.path.expandvars(os.path.expanduser(s)))
    if not p.is_absolute():
        p = Path.cwd() / p
    return p.resolve()


def nice_label(p: Path) -> str:
    """Short human name of a file source: parent/name (run/ledger.sqlite3 -> task/run/ledger.sqlite3)."""
    parts = p.parts
    n = 3 if p.name == LEDGER and p.parent.name == "run" else 2
    return "/".join(parts[-n:])


# ----------------------------------------------------------------------------
# sources (folders / files) and the index of timeline DBs under them
# ----------------------------------------------------------------------------
class Index:
    """Opened sources and the cached list of timeline DBs under them (rescanned on demand)."""

    def __init__(self, paths: list[Path]):
        self.lock = threading.Lock()
        self.sources: list[dict] = []
        self.cli: list[dict] = []
        self.dbs: dict[str, dict] = {}
        self.status: dict[str, dict] = {}
        for p in paths:
            try:
                self.cli.append(self.make_source(p, "cli"))
            except ValueError as e:
                print(f"warning: {p}: {e}", file=sys.stderr)
        self.sources = list(self.cli)

    @staticmethod
    def make_source(p: Path, origin: str, name: str | None = None, ledgers: bool = False) -> dict:
        p = p.resolve()
        if not p.exists():
            raise ValueError(f"нет такого пути: {p}")
        if p.is_dir() and (p / LEDGER).is_file() and sniff(p / LEDGER) == "harness":
            p = p / LEDGER  # a harness run dir is opened as its ledger
        if p.is_dir():
            return {"id": str(p), "path": str(p), "kind": "dir", "origin": origin, "ledgers": bool(ledgers),
                    "name": name or p.name or str(p)}
        kind = sniff(p)
        if not kind:
            raise ValueError(f"{p.name}: не таймлайн (нужна БД `render sqlite`, журнал .jsonl или {LEDGER})")
        return {"id": str(p), "path": str(p), "kind": "file", "file_kind": kind, "origin": origin,
                "name": name or nice_label(p)}

    def _entry(self, p: Path, root: Path, src: dict, kind: str | None = None) -> dict | None:
        kind = kind or sniff(p)
        if kind not in KIND_LABEL:
            return None
        db = derive(p, kind) if kind != "db" else p
        sessions = db_summary(db)
        if sessions is None:
            return None
        try:
            rel = str(p.relative_to(root)).replace("\\", "/")
        except ValueError:
            rel = p.name
        if src["kind"] == "file":
            label = src["name"]
        else:
            label = f"{src['name']}/{rel}"
        st = p.stat()
        return {"id": str(p), "db": str(db), "rel": rel, "root": str(root), "label": label, "kind": kind,
                "source": src["id"], "mtime": st.st_mtime, "size": st.st_size, "sessions": sessions,
                "analysis": analysis_path(p) is not None}

    def _scan_source(self, src: dict, found: dict) -> dict:
        st = {"n_dbs": 0, "n_sessions": 0, "n_ledgers": 0, "error": None, "truncated": False}
        p = Path(src["path"])
        try:
            if src["kind"] == "file":
                e = self._entry(p, p.parent, src, src.get("file_kind"))
                if e is None:
                    raise ValueError("файл больше не похож на таймлайн")
                found.setdefault(e["id"], e)
                st["n_dbs"], st["n_sessions"] = 1, len(e["sessions"])
                return st
            if not p.is_dir():
                raise ValueError("папка недоступна")
            seen = 0
            for dirpath, dirnames, filenames in os.walk(p):
                seen += 1
                if seen > MAX_SCAN_DIRS:
                    st["truncated"] = True
                    break
                dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not d.startswith("."))
                for fn in filenames:
                    if not fn.lower().endswith(DB_EXT):
                        continue
                    f = Path(dirpath) / fn
                    if fn == LEDGER:
                        st["n_ledgers"] += 1
                        if not src.get("ledgers"):
                            continue
                    try:
                        e = self._entry(f, p, src, "harness" if fn == LEDGER else None)
                    except (Exception, SystemExit):  # timeline.py raises SystemExit on bad input
                        continue
                    if e and e["id"] not in found:
                        found[e["id"]] = e
                        st["n_dbs"] += 1
                        st["n_sessions"] += len(e["sessions"])
        except (Exception, SystemExit) as e:  # noqa: BLE001  shown next to the source in the UI
            st["error"] = f"{e}"
        return st

    def scan(self) -> list[dict]:
        with self.lock:
            sources = list(self.sources)
        found: dict[str, dict] = {}
        status = {s["id"]: self._scan_source(s, found) for s in sources}
        with self.lock:
            self.dbs, self.status = found, status
        return sorted(found.values(), key=lambda d: d["label"])

    def sources_view(self) -> list[dict]:
        with self.lock:
            return [{**s, **self.status.get(s["id"], {})} for s in self.sources]

    def add(self, src: dict, mode: str) -> None:
        with self.lock:
            if mode == "open":
                self.sources = [src]
            else:
                self.sources = [s for s in self.sources if s["id"] != src["id"]] + [src]

    def remove(self, sid: str) -> None:
        with self.lock:
            self.sources = [s for s in self.sources if s["id"] != sid]

    def reset(self) -> None:
        with self.lock:
            self.sources = list(self.cli)

    def entry(self, db_id: str) -> dict:
        with self.lock:
            e = self.dbs.get(db_id)
        if e is None:
            self.scan()
            with self.lock:
                e = self.dbs.get(db_id)
        if e is None:
            raise NotFound("unknown db (not under the opened sources)")
        return e

    def get(self, db_id: str) -> Path:
        return Path(self.entry(db_id)["db"])


# ----------------------------------------------------------------------------
# folder browser, system dialog, uploads
# ----------------------------------------------------------------------------
def drives() -> list[str]:
    if os.name != "nt":
        return ["/"]
    if hasattr(os, "listdrives"):
        try:
            return list(os.listdrives())
        except OSError:
            pass
    import ctypes
    mask = ctypes.windll.kernel32.GetLogicalDrives()
    return [f"{c}:\\" for i, c in enumerate(string.ascii_uppercase) if mask >> i & 1]


def browse(raw: str, index: Index) -> dict:
    places = [{"name": "домашняя", "path": str(Path.home())}, {"name": "рабочая", "path": str(Path.cwd())},
              {"name": "session_timeline", "path": str(HERE)}]
    for s in index.cli:
        places.append({"name": Path(s["path"]).name, "path": s["path"] if s["kind"] == "dir" else str(Path(s["path"]).parent)})
    seen, uniq = set(), []
    for pl in places:
        k = pl["path"].lower()
        if k not in seen:
            seen.add(k)
            uniq.append(pl)
    out = {"places": uniq, "drives": drives(), "sep": os.sep}
    if not (raw or "").strip():
        if os.name == "nt":  # "computer" view: the list of drives
            return {**out, "path": "", "parent": None,
                    "entries": [{"name": d, "path": d, "type": "dir"} for d in out["drives"]]}
        raw = "/"
    p = clean_path(raw)
    if p.is_file():
        p = p.parent
    if not p.is_dir():
        raise ValueError(f"нет такой папки: {p}")
    parent = None if p.parent == p else str(p.parent)
    if parent is None and os.name == "nt":
        parent = ""
    dirs, files, other = [], [], 0
    try:
        items = sorted(os.scandir(p), key=lambda e: e.name.lower())
    except OSError as e:
        raise ValueError(f"нет доступа: {e}")
    for it in items:
        try:
            if it.is_dir():
                if it.name.startswith(".") or it.name in ("$RECYCLE.BIN", "System Volume Information"):
                    continue
                hint = None
                if len(dirs) < 400:
                    try:
                        names = os.listdir(it.path)
                        low = {n.lower() for n in names}
                        if any(n.endswith(DB_EXT) and n != LEDGER for n in low):
                            hint = "db"
                        elif LEDGER in low or (Path(it.path) / "run" / LEDGER).exists():
                            hint = "run"
                    except OSError:
                        pass
                dirs.append({"name": it.name, "path": it.path, "type": "dir", "hint": hint})
            elif it.name.lower().endswith(FILE_EXT):
                kind = sniff(Path(it.path))
                if not kind:
                    other += 1
                    continue
                f = {"name": it.name, "path": it.path, "type": "file", "kind": kind, "kind_label": KIND_LABEL[kind],
                     "size": it.stat().st_size, "mtime": it.stat().st_mtime}
                if kind == "db":
                    try:
                        f["sessions"] = len(db_summary(Path(it.path)) or [])
                    except sqlite3.Error:
                        pass
                files.append(f)
        except OSError:
            continue
    return {**out, "path": str(p), "parent": parent, "entries": dirs + files, "other": other}


PICK_SCRIPT = r"""
import sys, tkinter as tk
from tkinter import filedialog
kind, start = sys.argv[1], sys.argv[2] or None
r = tk.Tk(); r.withdraw(); r.attributes("-topmost", True); r.update()
if kind == "dir":
    p = filedialog.askdirectory(parent=r, initialdir=start, mustexist=True, title="Папка с таймлайнами")
else:
    p = filedialog.askopenfilename(parent=r, initialdir=start, title="Файл таймлайна", filetypes=[
        ("Таймлайны", "*.db *.sqlite *.sqlite3 *.jsonl"), ("Все файлы", "*.*")])
r.destroy()
sys.stdout.buffer.write((p or "").encode("utf-8"))
"""
CAN_PICK = importlib.util.find_spec("tkinter") is not None
_pick_lock = threading.Lock()


def pick(kind: str, start: str | None) -> str | None:
    """Native open dialog on this machine (tkinter in a child process; the server itself stays GUI-free)."""
    if not CAN_PICK:
        raise ValueError("tkinter недоступен: используйте поле пути или обзор папок")
    if not _pick_lock.acquire(blocking=False):
        raise ValueError("системный диалог уже открыт")
    try:
        r = subprocess.run([sys.executable, "-c", PICK_SCRIPT, "dir" if kind == "dir" else "file", start or ""],
                           capture_output=True, timeout=900)
        if r.returncode:
            err = r.stderr.decode("utf-8", "replace").strip().splitlines()
            raise ValueError("системный диалог не открылся" + (f": {err[-1]}" if err else ""))
        p = r.stdout.decode("utf-8", "replace").strip()
        return str(Path(p).resolve()) if p else None
    finally:
        _pick_lock.release()


def safe_rel(rel: str) -> Path:
    parts = [re.sub(r'[<>:"|?*\x00-\x1f]', "_", x) for x in re.split(r"[\\/]+", rel or "") if x not in ("", ".", "..")]
    if not parts:
        raise ValueError("пустое имя файла")
    return Path(*parts)


def prune_cache(max_age_s: float = 7 * 86400) -> None:
    shutil.rmtree(UPLOADS, ignore_errors=True)  # uploads only live as long as the server that took them
    now = time.time()
    for f in DERIVED.glob("*.db") if DERIVED.exists() else []:
        try:
            if now - f.stat().st_mtime > max_age_s:
                f.unlink()
        except OSError:
            pass


# ----------------------------------------------------------------------------
# other API helpers
# ----------------------------------------------------------------------------
def analysis_path(src: Path) -> Path | None:
    p = src.parent / "ANALYSIS.md"
    if not p.exists() and src.name == LEDGER:  # ctf-runs/<task>/run/ledger.sqlite3 -> ctf-runs/<task>/ANALYSIS.md
        p = src.parent.parent / "ANALYSIS.md"
    return p if p.exists() else None


def analysis_for(src: Path) -> str | None:
    p = analysis_path(src)
    return p.read_text(encoding="utf-8", errors="replace")[:30000] if p else None


def incidents_summary(index: "Index") -> list[dict]:
    """Incident counts per (session, category, severity, open?) across all DBs, one DB per unique session."""
    seen, out = set(), []
    with index.lock:
        dbs = list(index.dbs.values())
    for d in sorted(dbs, key=lambda d: (len(d["sessions"]), d["rel"])):  # prefer single-session DBs
        try:
            con = ro(Path(d["db"]))
        except sqlite3.Error:
            continue
        try:
            for s in d["sessions"]:
                key = (s["session_id"], round(s["started"] or 0))
                if key in seen:
                    continue
                seen.add(key)
                for cat, sev, open_, n in con.execute(
                        "SELECT category, severity, status IN ('open','investigating'), COUNT(*) FROM incidents "
                        "WHERE session_id = ? GROUP BY 1, 2, 3", (s["session_id"],)):
                    out.append({"session_id": s["session_id"], "category": cat or "other", "severity": sev or "medium",
                                "open": bool(open_), "n": n})
        except sqlite3.Error:
            pass
        finally:
            con.close()
    return out


def run_sql(db: Path, sql: str) -> dict:
    con = ro(db)
    try:
        # second line of defence besides mode=ro: allow only reads
        def auth(action, *_):
            return sqlite3.SQLITE_DENY if action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH) else sqlite3.SQLITE_OK
        con.set_authorizer(auth)
        t = time.time()
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(MAX_ROWS + 1)
        return {"columns": cols, "rows": rows[:MAX_ROWS], "truncated": len(rows) > MAX_ROWS,
                "ms": round((time.time() - t) * 1000, 1)}
    finally:
        con.close()


class Handler(BaseHTTPRequestHandler):
    index: Index = None  # set in main

    def log_message(self, fmt, *args):  # quiet
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8", headers=None):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False, default=str)
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _local(self, post=False) -> bool:
        """The page can open any path on this machine, so only talk to itself: Host must be loopback
        (DNS rebinding), POSTs must come from our own origin with a non-form content type (CSRF)."""
        host = (self.headers.get("Host") or "").strip().lower()
        hostname = host[1:host.index("]")] if host.startswith("[") and "]" in host else host.split(":")[0]
        if hostname not in ("127.0.0.1", "localhost", "::1"):
            return False
        if post:
            origin = self.headers.get("Origin")
            if origin and urlparse(origin).netloc.lower() != host:
                return False
            ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            if ctype not in ("application/json", "application/octet-stream"):
                return False
        return True

    def _index_payload(self):
        dbs = self.index.scan()
        return {"sources": self.index.sources_view(), "dbs": dbs, "tool": str(HERE / "timeline.py"),
                "can_pick": CAN_PICK, "cli": [s["id"] for s in self.index.cli],
                "roots": [s["path"] for s in self.index.sources_view()]}

    def _journal(self, q):
        db = self.index.get(q.get("db", [""])[0])
        session = q.get("session", [None])[0]
        return db, session, TL.Journal(db, session)

    def do_GET(self):
        if not self._local():
            return self._send(403, {"error": "forbidden host"})
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path == "/":
                return self._send(200, TL.apply_brand(PAGE.read_text(encoding="utf-8"), "gui"), "text/html; charset=utf-8")
            if u.path == "/api/index":
                return self._send(200, self._index_payload())
            if u.path == "/api/browse":
                return self._send(200, browse(q.get("path", [""])[0], self.index))
            if u.path == "/api/incidents":
                if not self.index.dbs:
                    self.index.scan()
                return self._send(200, incidents_summary(self.index))
            if u.path == "/api/render":
                db, session, J = self._journal(q)
                fmt = q.get("format", ["html"])[0]
                m = TL.build_model(J.records)
                if fmt == "html":
                    page = TL.render_html(m)
                    if q.get("embed", ["0"])[0] == "1":
                        page = page.replace("<body>", '<body class="embed">', 1)
                    return self._send(200, page, "text/html; charset=utf-8")
                if fmt == "text":
                    detail = int(q.get("detail", ["160"])[0] or 0)
                    sections = TL.csv(q.get("sections", [None])[0])
                    return self._send(200, TL.render_text(m, sections=sections, detail=detail),
                                      "text/plain; charset=utf-8")
                if fmt == "json":
                    return self._send(200, m)
                return self._send(400, {"error": "format: html|text|json"})
            if u.path == "/api/validate":
                db, session, J = self._journal(q)
                errors, warns = TL.validate_records(J.records)
                return self._send(200, {"errors": errors, "warnings": warns})
            if u.path == "/api/analysis":
                e = self.index.entry(q.get("db", [""])[0])
                return self._send(200, {"analysis": analysis_for(Path(e["id"]))})
            if u.path == "/api/download":
                db, session, J = self._journal(q)
                fmt = q.get("format", ["jsonl"])[0]
                name = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", J.db_session or "session")
                if fmt == "jsonl":
                    body = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in J.records)
                    return self._send(200, body, "application/x-ndjson; charset=utf-8",
                                      {"Content-Disposition": content_disposition(f"{name}.jsonl")})
                if fmt == "html":
                    return self._send(200, TL.render_html(TL.build_model(J.records)), "text/html; charset=utf-8",
                                      {"Content-Disposition": content_disposition(f"{name}.html")})
                return self._send(400, {"error": "format: jsonl|html"})
            return self._send(404, {"error": "not found"})
        except NotFound as e:
            return self._send(404, {"error": str(e)})
        except (ValueError, SystemExit) as e:  # timeline.py reports user errors via SystemExit
            return self._send(400, {"error": str(e)})
        except Exception as e:
            return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    def do_POST(self):
        if not self._local(post=True):
            return self._send(403, {"error": "forbidden origin"})
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            n = int(self.headers.get("Content-Length") or 0)
            if u.path == "/api/upload":
                return self._upload(q, n)
            body = json.loads(self.rfile.read(n) or b"{}")
            if u.path == "/api/sql":
                db = self.index.get(body.get("db", ""))
                return self._send(200, run_sql(db, body.get("sql", "")))
            if u.path == "/api/sources":
                act = body.get("action", "add")
                if act == "remove":
                    self.index.remove(body.get("id", ""))
                    return self._send(200, self._index_payload())
                if act == "reset":
                    self.index.reset()
                    return self._send(200, self._index_payload())
                if act not in ("add", "open"):
                    return self._send(400, {"error": "action: add|open|remove|reset"})
                src = Index.make_source(clean_path(body.get("path", "")), body.get("origin") or "gui",
                                        body.get("name"), bool(body.get("ledgers")))
                note = None
                if src["kind"] == "dir" and not src["ledgers"]:
                    # a folder of harness runs and nothing else: import the runs instead of showing nothing
                    probe = self.index._scan_source(src, {})
                    if not probe["n_dbs"] and probe["n_ledgers"]:
                        src["ledgers"] = True
                        note = f"таймлайн-БД нет — импортированы прогоны харнесса ({probe['n_ledgers']} {LEDGER})"
                self.index.add(src, act)
                payload = self._index_payload()
                payload["source"] = src["id"]
                payload["added"] = [d["id"] for d in payload["dbs"] if d["source"] == src["id"]]
                payload["note"] = note
                return self._send(200, payload)
            if u.path == "/api/pick":
                return self._send(200, {"path": pick(body.get("kind", "dir"), body.get("start"))})
            return self._send(404, {"error": "not found"})
        except NotFound as e:
            return self._send(404, {"error": str(e)})
        except sqlite3.Error as e:
            return self._send(400, {"error": f"SQL: {e}"})
        except (ValueError, SystemExit) as e:
            return self._send(400, {"error": str(e)})
        except subprocess.TimeoutExpired:
            return self._send(408, {"error": "системный диалог закрыт по таймауту"})
        except Exception as e:
            return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    def _upload(self, q, n: int):
        """Raw body = one file. ?batch=<id>&rel=<relative path>; files of one batch share a folder."""
        if n <= 0 or n > MAX_UPLOAD:
            return self._send(413, {"error": f"файл пустой или больше {MAX_UPLOAD >> 20} МБ"})
        batch = re.sub(r"[^A-Za-z0-9_-]", "", q.get("batch", [""])[0])[:40]
        if not batch:
            return self._send(400, {"error": "batch?"})
        rel = safe_rel(q.get("rel", [""])[0])
        if not rel.name.lower().endswith(FILE_EXT):
            return self._send(400, {"error": f"{rel.name}: нужен .db / .sqlite / .sqlite3 / .jsonl"})
        base = UPLOADS / batch
        dest = base / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        left = n
        with open(dest, "wb") as fh:
            while left > 0:
                chunk = self.rfile.read(min(left, 1 << 20))
                if not chunk:
                    break
                fh.write(chunk)
                left -= len(chunk)
        kind = sniff(dest)
        if left or not kind:
            dest.unlink(missing_ok=True)
            return self._send(400, {"error": f"{rel.name}: " + ("загрузка оборвалась" if left else "не таймлайн")})
        return self._send(200, {"path": str(dest), "top": str(base / rel.parts[0]), "kind": kind})


def main():
    TL.utf8_stdio()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("roots", nargs="*", default=["."], metavar="PATH", help="folder or timeline file")
    ap.add_argument("--port", type=int, default=8780)
    ap.add_argument("--open", action="store_true", help="open the browser")
    a = ap.parse_args()
    prune_cache()
    Handler.index = Index([Path(r) for r in a.roots])
    n = len(Handler.index.scan())
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    url = f"http://127.0.0.1:{a.port}/"
    print(f"timeline GUI: {url}  ({n} DBs under {', '.join(s['path'] for s in Handler.index.sources)})", flush=True)
    if a.open:
        webbrowser.open(url)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
