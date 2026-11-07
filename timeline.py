#!/usr/bin/env python3
"""Session timeline: an append-only journal of an agentic session + renderers.

An AI agent (or a human) records what happens during a session — events of
different agents, DAG mutations, token spend, hypotheses, incidents, flags and
the final verdict — into one JSONL file. Two renderers read the same file:

  * ``render text``  — compact, grep-friendly digest for an agent's context;
  * ``render html``  — self-contained visual timeline (one HTML file) for a human.

``import-harness`` bootstraps a journal from an inslopnia_harness run dir
(ledger.sqlite3 + pi-sessions/*.jsonl); the agent can then annotate it.

Stdlib only.  Run ``python timeline.py -h`` / ``python timeline.py <cmd> -h``.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import glob
import html
import json
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "template.html"
DEFAULT_FILE = os.environ.get("TIMELINE_FILE", "timeline.jsonl")

ROLES = ["P", "E", "R", "C", "operator", "runtime", "other"]
NODE_STATUSES = ["pending", "running", "done", "failed", "cancelled", "blocked"]
TERMINAL = {"done", "failed", "cancelled"}
EVENT_KINDS = ["action", "observation", "decision", "hypothesis", "milestone",
               "message", "tool", "result", "error", "note"]
SEVERITIES = ["low", "medium", "high", "critical"]
INCIDENT_CATEGORIES = ["harness", "orchestration", "agent", "model", "infra", "env", "tooling", "platform", "other"]
INCIDENT_STATUSES = ["open", "investigating", "mitigated", "resolved", "wontfix"]
OPEN_INCIDENT = {"open", "investigating"}  # mitigated = worked around, root cause not fixed
OUTCOMES = ["solved", "unsolved", "partial", "in_progress", "abandoned"]

# ----------------------------------------------------------------------------
# time helpers
# ----------------------------------------------------------------------------
_REL = re.compile(r"^[+T]?\+?((?:\d+(?:\.\d+)?[hms])+)$")


def parse_time(value, t0: float | None) -> float:
    """now | epoch | +1h2m30s (relative to session start) | ISO-8601 | HH:MM[:SS]."""
    if value is None or value == "" or value == "now":
        return time.time()
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    mc = re.match(r"^(?:T)?\+(\d+):(\d{1,2})(?::(\d{1,2}))?$", s)  # T+mm:ss | +mm:ss | T+h:mm:ss
    if mc:
        a_, b_, c_ = mc.groups()
        secs = int(a_) * 3600 + int(b_) * 60 + int(c_) if c_ is not None else int(a_) * 60 + int(b_)
        return (t0 if t0 is not None else time.time()) + secs
    m = _REL.match(s) if s[0] in "+T" else None
    if m:
        secs = 0.0
        for num, unit in re.findall(r"(\d+(?:\.\d+)?)([hms])", m.group(1)):
            secs += float(num) * {"h": 3600, "m": 60, "s": 1}[unit]
        return (t0 if t0 is not None else time.time()) + secs
    try:
        f = float(s)
        if f > 1e9:
            return f
        return (t0 if t0 is not None else time.time()) + f
    except ValueError:
        pass
    if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", s):
        base = _dt.datetime.fromtimestamp(t0 if t0 is not None else time.time())
        parts = [int(x) for x in s.split(":")] + [0]
        return base.replace(hour=parts[0], minute=parts[1], second=parts[2], microsecond=0).timestamp()
    iso = s.replace("Z", "+00:00")
    try:
        return _dt.datetime.fromisoformat(iso).timestamp()
    except ValueError:
        raise SystemExit(f"cannot parse time: {value!r} (use now, +12m, epoch, ISO, HH:MM)")


def fmt_rel(sec: float) -> str:
    sec = max(0, int(round(sec)))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def fmt_dur(sec: float) -> str:
    sec = max(0, int(sec))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def fmt_tok(n) -> str:
    n = n or 0
    if n >= 1e6:
        return f"{n/1e6:.2f}M"
    if n >= 1e3:
        return f"{n/1e3:.1f}k"
    return str(int(n))


def fmt_abs(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def csv(v):
    if v is None:
        return None
    if isinstance(v, list):
        return v
    return [x.strip() for x in str(v).split(",") if x.strip()]


# ----------------------------------------------------------------------------
# journal I/O
# ----------------------------------------------------------------------------
def is_sqlite(path: Path) -> bool:
    try:
        with open(path, "rb") as fh:
            return fh.read(16) == b"SQLite format 3\x00"
    except OSError:
        return False


def db_sessions(path: Path) -> list[tuple]:
    con = sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)
    try:
        return con.execute("SELECT session_id, title, outcome, started, duration_s FROM sessions "
                           "ORDER BY started").fetchall()
    finally:
        con.close()


class Journal:
    """JSONL journal (read/write) or a DB made by `render sqlite` (read-only, one session)."""

    def __init__(self, path: str | Path, session: str | None = None):
        self.path = Path(path)
        self.records: list[dict] = []
        self.db_session: str | None = None
        if self.path.exists() and is_sqlite(self.path):
            self._load_db(session)
        elif self.path.exists():
            with open(self.path, encoding="utf-8") as fh:
                for i, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        self.records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"warning: {self.path}:{i}: bad JSON skipped ({e})", file=sys.stderr)

    def _load_db(self, session):
        sess = db_sessions(self.path)
        ids = [r[0] for r in sess]
        if not ids:
            raise SystemExit(f"{self.path}: no sessions in DB")
        if session is None:
            if len(ids) > 1:
                raise SystemExit(f"{self.path} holds {len(ids)} sessions; pick one with --session "
                                 f"({', '.join(ids)}) or run `sessions`")
            session = ids[0]
        if session not in ids:
            raise SystemExit(f"session {session!r} not in {self.path}; available: {', '.join(ids)}")
        con = sqlite3.connect(f"file:{self.path.resolve().as_posix()}?mode=ro", uri=True)
        try:
            rows = con.execute("SELECT json FROM records WHERE session_id = ? ORDER BY seq", (session,)).fetchall()
        finally:
            con.close()
        self.records = [json.loads(r[0]) for r in rows]
        self.db_session = session

    def _writable(self):
        if self.db_session is not None:
            raise SystemExit(f"{self.path} is a rendered SQLite DB (read-only source). Render it back to a "
                             f"journal first: render jsonl -o {self.db_session}.jsonl")

    @property
    def t0(self) -> float | None:
        for r in self.records:
            if r.get("type") == "session":
                return r["t"]
        return min((r["t"] for r in self.records), default=None)

    def next_id(self, prefix: str) -> str:
        n = sum(1 for r in self.records if str(r.get("id", "")).startswith(prefix)
                and str(r.get("id", ""))[len(prefix):].isdigit())
        return f"{prefix}{n + 1}"

    def append(self, rec: dict, quiet=False) -> dict:
        rec = {k: v for k, v in rec.items() if v is not None and v != [] and v != ""}
        self._writable()
        t0 = self.t0
        if rec.get("type") != "session" and t0 is not None and rec.get("t") is not None:
            if rec["t"] < t0 - 1:
                print(f"warning: time is {fmt_rel(t0 - rec['t'])} BEFORE session start — check --t "
                      f"(relative forms: +mm:ss, T+mm:ss, +12m)", file=sys.stderr)
            elif rec["t"] > time.time() + 300:
                print("warning: time is in the future — check --t", file=sys.stderr)
        rec.setdefault("id", self.next_id("r"))
        self.records.append(rec)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        if not quiet:
            print(rec["id"])
        return rec

    def write_all(self, records: list[dict]):
        self._writable()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as fh:
            for r in records:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        self.records = list(records)


def guess_role(agent: str) -> str:
    a = agent.lower()
    if a in ("p", "planner") or a.startswith("p:") or a.startswith("planner"):
        return "P"
    if a.startswith("e:") or a.startswith("e-") or a.startswith("exec") or a == "e":
        return "E"
    if a.startswith("r:") or a.startswith("r-") or a.startswith("research") or a == "r":
        return "R"
    if a.startswith("c:") or a.startswith("critic") or a == "c":
        return "C"
    if a.startswith("operator") or a in ("human", "user"):
        return "operator"
    if a in ("runtime", "supervisor", "harness", "system"):
        return "runtime"
    return "other"


# ----------------------------------------------------------------------------
# fold: records -> model consumed by both renderers
# ----------------------------------------------------------------------------
def build_model(records: list[dict]) -> dict:
    recs = sorted(enumerate(records), key=lambda p: (p[1].get("t", 0), p[0]))
    recs = [r for _, r in recs]
    session = {"title": "untitled session"}
    agents: dict[str, dict] = {}
    agent_order: list[str] = []

    def agent(aid, t=None):
        if not aid:
            return None
        if aid not in agents:
            agents[aid] = {"id": aid, "role": guess_role(aid), "model": None, "parent": None,
                           "label": None, "explicit_spans": [], "open": None,
                           "tokens": 0, "input": 0, "output": 0, "cache_read": 0, "cache_write": 0,
                           "cost": 0.0, "events": 0, "first": t, "last": t, "steps": []}
            agent_order.append(aid)
        a = agents[aid]
        if t is not None:
            a["first"] = t if a["first"] is None else min(a["first"], t)
            a["last"] = t if a["last"] is None else max(a["last"], t)
        return a

    nodes: dict[str, dict] = {}
    dag_ops: list[dict] = []
    items: list[dict] = []
    usage: list[dict] = []
    cum_by_agent: dict[str, float] = defaultdict(float)
    hyps: dict[str, dict] = {}
    incidents: dict[str, dict] = {}
    flags: list[dict] = []
    outcomes: list[dict] = []
    phases: list[dict] = []
    open_phase: dict[str, dict] = {}
    tasks: dict[str, dict] = {}

    for r in recs:
        t = r.get("t", 0)
        typ = r.get("type")
        if typ == "session":
            session.update({k: v for k, v in r.items() if k not in ("type", "id")})
            session["started"] = t
            continue
        if typ == "agent":
            a = agent(r["agent"])
            for k in ("role", "model", "parent", "label"):
                if r.get(k):
                    a[k] = r[k]
            st = r.get("state")
            if st == "start":
                a["open"] = t
                agent(r["agent"], t)
            elif st == "stop":
                if a["open"] is not None:
                    a["explicit_spans"].append([a["open"], t, r.get("status") or "done"])
                a["open"] = None
                agent(r["agent"], t)
            if st:
                items.append({**r, "kind": "agent_" + st, "title": f"{r['agent']} {st}"
                              + (f" ({r['status']})" if r.get("status") else "")})
            continue
        if typ == "event":
            a = agent(r.get("agent") or "?", t)
            a["events"] += 1
            if r.get("to"):
                agent(r["to"], t)
            if r.get("tokens") or r.get("cost"):
                cum_by_agent[a["id"]] += r.get("tokens") or 0
                usage.append({"t": t, "agent": a["id"], "tokens": r.get("tokens") or 0,
                              "cost": r.get("cost") or 0.0, "step": r.get("step")})
            items.append(dict(r))
            continue
        if typ == "usage":
            a = agent(r["agent"], t)
            tot = r.get("total")
            if tot is None:
                tot = sum(r.get(k) or 0 for k in ("input", "output", "cache_read", "cache_write"))
            if r.get("cumulative"):
                delta = tot - cum_by_agent[a["id"]]
                cum_by_agent[a["id"]] = tot
            else:
                delta = tot
                cum_by_agent[a["id"]] += tot
            for k in ("input", "output", "cache_read", "cache_write"):
                a[k] += r.get(k) or 0
            usage.append({"t": t, "agent": a["id"], "tokens": delta, "cost": r.get("cost") or 0.0,
                          "step": r.get("step"), "input": r.get("input"), "output": r.get("output"),
                          "cache_read": r.get("cache_read"), "cache_write": r.get("cache_write")})
            continue
        if typ == "dag":
            op = r["op"]
            nid = r["node"]
            by = r.get("by")
            if by:
                agent(by, t)
            n = nodes.get(nid)
            if op == "add" or n is None:
                n = nodes.setdefault(nid, {"id": nid, "objective": "", "deps": [], "assignee": None,
                                           "status": "pending", "added": t, "added_by": by,
                                           "history": [], "attempts": 0, "removed": False,
                                           "runs": [], "task": r.get("task")})
            if r.get("objective"):
                n["objective"] = r["objective"]
            if r.get("detail"):
                n["detail"] = r["detail"]
            if r.get("deps") is not None and op in ("add", "update"):
                n["deps"] = list(r["deps"])
            if op == "dep_add":
                for d in r.get("deps") or []:
                    if d not in n["deps"]:
                        n["deps"].append(d)
            if op == "dep_rm":
                n["deps"] = [d for d in n["deps"] if d not in (r.get("deps") or [])]
            if r.get("assignee"):
                n["assignee"] = r["assignee"]
                agent(r["assignee"])
                if nid not in agents[r["assignee"]]["steps"]:
                    agents[r["assignee"]]["steps"].append(nid)
            prev = n["status"]
            if op == "status":
                n["status"] = r["status"]
                if r["status"] == "running":
                    n["attempts"] += 1
                    n["runs"].append([t, None, None])
                    if n["assignee"]:
                        agent(n["assignee"], t)
                elif n["runs"] and n["runs"][-1][1] is None:
                    n["runs"][-1][1] = t
                    n["runs"][-1][2] = r["status"]
                    if n["assignee"]:
                        agent(n["assignee"], t)
                n["reason"] = r.get("reason")
            if op == "remove":
                n["removed"] = True
                if n["runs"] and n["runs"][-1][1] is None:
                    n["runs"][-1][1] = t
                    n["runs"][-1][2] = "cancelled"
            op_rec = {"t": t, "id": r.get("id"), "op": op, "node": nid, "by": by,
                      "status": r.get("status"), "prev": prev, "reason": r.get("reason"),
                      "objective": r.get("objective"), "deps": r.get("deps"),
                      "assignee": r.get("assignee"), "note": r.get("note"), "fields": r.get("fields")}
            dag_ops.append({k: v for k, v in op_rec.items() if v is not None})
            n["history"].append(dag_ops[-1])
            continue
        if typ == "hyp":
            hid = r["hid"]
            existed = hid in hyps
            h = hyps.setdefault(hid, {"id": hid, "claim": "", "status": "open", "history": [],
                                      "created": t, "agent": r.get("agent"), "step": r.get("step")})
            if r.get("claim"):
                h["claim"] = r["claim"]
            if r.get("op") == "add" and existed:   # add arrived (near-)simultaneously after a set: keep status
                h["created"] = min(h["created"], t)
                h["agent"] = h.get("agent") or r.get("agent")
                h["step"] = h.get("step") or r.get("step")
            elif r.get("status"):
                h["status"] = r["status"]
            if r.get("agent"):
                agent(r["agent"], t)
            h["history"].append({"t": t, "status": r.get("status") or h["status"],
                                 "agent": r.get("agent"), "evidence": r.get("evidence"),
                                 "op": r.get("op")})
            h["updated"] = t
            items.append({**r, "kind": "hypothesis",
                          "title": (f"hypothesis {hid}: {h['claim']}" if r.get("op") == "add"
                                    else f"hypothesis {hid} → {h['status']}")})
            continue
        if typ == "incident":
            iid = r["iid"]
            existed = iid in incidents
            inc = incidents.setdefault(iid, {"id": iid, "title": "", "severity": "medium",
                                             "category": "other", "status": "open", "created": t,
                                             "history": []})
            late_add = r.get("op") == "add" and existed  # add after an early update/resolve: keep status
            for k in ("title", "severity", "category", "agent", "step", "detail", "status", "impact",
                      "workaround", "auto"):
                if r.get(k) and not (late_add and k == "status"):
                    inc[k] = r[k]
            if late_add:
                inc["created"] = min(inc["created"], t)
            if r.get("op") == "resolve":
                inc["status"] = "resolved"
                inc["resolved"] = t
            elif r.get("op") == "mitigate":
                inc["status"] = "mitigated"
                inc["mitigated"] = t
            if r.get("agent"):
                agent(r["agent"], t)
            inc["history"].append({"t": t, "op": r.get("op"), "note": r.get("note"),
                                   "status": inc["status"]})
            items.append({**r, "kind": "incident",
                          "title": (f"incident {iid}: {inc['title']}" if r.get("op") == "add"
                                    else f"incident {iid} {r.get('op')}: {r.get('note') or inc['status']}")})
            continue
        if typ == "flag":
            flags.append(dict(r))
            if r.get("agent"):
                agent(r["agent"], t)
            items.append({**r, "kind": "flag", "title": f"flag {r.get('status', 'candidate')}: {r['value']}"})
            continue
        if typ == "outcome":
            outcomes.append(dict(r))
            tasks[r.get("task") or "main"] = dict(r)
            items.append({**r, "kind": "outcome", "title": f"OUTCOME {r.get('task') or 'main'}: {r['status']}"})
            continue
        if typ == "phase":
            name = r["name"]
            if r["op"] == "begin":
                open_phase[name] = {"name": name, "start": t, "end": None, "agent": r.get("agent"),
                                    "note": r.get("note")}
                phases.append(open_phase[name])
            elif name in open_phase:
                open_phase.pop(name)["end"] = t
            continue

    all_t = [r.get("t") for r in recs if r.get("t")]
    t0 = session.get("started") or (min(all_t) if all_t else time.time())
    t1 = max(all_t) if all_t else t0
    if session.get("ended"):
        t1 = max(t1, session["ended"])
    for p in phases:
        if p["end"] is None:
            p["end"] = t1
            p["open"] = True

    # usage totals / cumulative series
    usage.sort(key=lambda u: u["t"])
    for u in usage:
        a = agents[u["agent"]]
        a["tokens"] += u["tokens"]
        a["cost"] += u["cost"]
    # agent spans: explicit > node runs > first..last
    for aid, a in agents.items():
        spans = list(a["explicit_spans"])
        if a["open"] is not None:
            spans.append([a["open"], t1, "running"])
        if not spans:
            for nid in a["steps"]:
                for s, e, st in nodes[nid]["runs"]:
                    spans.append([s, e if e is not None else t1, st or "running"])
        if not spans and a["first"] is not None and a["role"] in ("P", "E", "R", "C"):
            spans.append([a["first"], a["last"] if a["last"] is not None else t1, "active"])
        a["spans"] = sorted(spans)
        a.pop("explicit_spans", None)
        a.pop("open", None)
    for n in nodes.values():
        for run in n["runs"]:
            if run[1] is None:
                run[1] = t1
                run[2] = "running"

    final = None
    for o in outcomes:
        if (o.get("task") or "main") == "main" or final is None:
            final = o
    tok_total = sum(a["tokens"] for a in agents.values())
    cost_total = sum(a["cost"] for a in agents.values())
    counts = defaultdict(int)
    for n in nodes.values():
        counts["removed" if n["removed"] else n["status"]] += 1

    # assign ids to items lacking them, order by time
    items.sort(key=lambda i: i.get("t", 0))
    role_rank = {r: i for i, r in enumerate(ROLES)}
    order = sorted(agent_order, key=lambda x: (role_rank.get(agents[x]["role"], 9),
                                                agents[x]["first"] if agents[x]["first"] is not None else t1))
    return {
        "session": session, "t0": t0, "t1": t1,
        "agents": [agents[a] for a in order],
        "nodes": list(nodes.values()), "dag_ops": dag_ops, "items": items, "usage": usage,
        "hyps": list(hyps.values()), "incidents": list(incidents.values()),
        "flags": flags, "outcomes": outcomes, "tasks": tasks, "final": final, "phases": phases,
        "totals": {"tokens": tok_total, "cost": cost_total, "events": len(items),
                   "nodes": dict(counts), "agents": len(agents),
                   "incidents_open": sum(1 for i in incidents.values() if i["status"] in OPEN_INCIDENT),
                   "incidents": len(incidents),
                   "input": sum(a["input"] for a in agents.values()),
                   "output": sum(a["output"] for a in agents.values()),
                   "cache_read": sum(a["cache_read"] for a in agents.values())},
    }


def topo_layers(nodes: list[dict]) -> list[list[dict]]:
    by = {n["id"]: n for n in nodes}
    depth: dict[str, int] = {}

    def d(nid, seen=()):
        if nid in depth:
            return depth[nid]
        if nid in seen or nid not in by:
            return 0
        deps = [x for x in by[nid]["deps"] if x in by]
        depth[nid] = 0 if not deps else 1 + max(d(x, seen + (nid,)) for x in deps)
        return depth[nid]

    for n in nodes:
        d(n["id"])
    layers: dict[int, list] = defaultdict(list)
    for n in nodes:
        layers[depth[n["id"]]].append(n)
    return [layers[k] for k in sorted(layers)]


# ----------------------------------------------------------------------------
# text renderer
# ----------------------------------------------------------------------------
STATUS_MARK = {"pending": "·", "running": "▶", "done": "✓", "failed": "✗", "cancelled": "⊘",
               "blocked": "■", "removed": "⌫"}
VERDICT = {"solved": "SOLVED ✓", "unsolved": "UNSOLVED ✗", "partial": "PARTIAL ◐",
           "in_progress": "IN PROGRESS …", "abandoned": "ABANDONED ⊘"}


def render_text(m: dict, sections=None, agent_filter=None, since=None, last=None, detail=160,
                kinds=None) -> str:
    t0 = m["t0"]
    out: list[str] = []
    S = set(sections or ["summary", "agents", "dag", "dag-history", "hyps", "incidents", "timeline"])
    rel = lambda t: ("T+" + fmt_rel(t - t0)) if t >= t0 - 0.5 else ("T-" + fmt_rel(t0 - t))
    clip = lambda s, n=detail: ("" if not s else (s if len(s) <= n else s[: n - 1] + "…")).replace("\n", " ⏎ ")
    ses = m["session"]
    fin = m["final"]
    tt = m["totals"]

    if "summary" in S:
        verdict = VERDICT.get(fin["status"], fin["status"].upper()) if fin else "NO VERDICT (in progress)"
        out.append(f"# SESSION: {ses.get('title')}   [{verdict}]")
        if ses.get("task"):
            out.append(f"task: {ses['task']}")
        if ses.get("goal"):
            out.append(f"goal: {clip(ses['goal'], 300)}")
        meta = ses.get("meta") or {}
        if meta:
            out.append("meta: " + ", ".join(f"{k}={v}" for k, v in meta.items()))
        out.append(f"window: {fmt_abs(t0)} → {fmt_abs(m['t1'])}  ({fmt_dur(m['t1'] - t0)})")
        out.append(f"tokens: {fmt_tok(tt['tokens'])} (in {fmt_tok(tt['input'])}, out {fmt_tok(tt['output'])}, "
                   f"cache {fmt_tok(tt['cache_read'])})   cost: ${tt['cost']:.2f}   agents: {tt['agents']}   "
                   f"events: {tt['events']}")
        nc = tt["nodes"]
        out.append("dag: " + (", ".join(f"{k} {v}" for k, v in sorted(nc.items())) or "empty")
                   + f"   incidents: {tt['incidents_open']} open / {tt['incidents']} total")
        if len(m["tasks"]) > 1:
            out.append("tasks: " + "; ".join(f"{k}={v['status']}" for k, v in m["tasks"].items()))
        if fin:
            if fin.get("flag"):
                out.append(f"flag: {fin['flag']}")
            if fin.get("summary"):
                out.append(f"verdict note: {clip(fin['summary'], 600)}")
        for f in m["flags"]:
            out.append(f"flag {f.get('status', 'candidate')} @{rel(f['t'])} by {f.get('agent', '?')}: {f['value']}"
                       + (f" — {clip(f.get('note'), 120)}" if f.get("note") else ""))
        out.append("")

    if "agents" in S:
        out.append("## AGENTS")
        out.append(f"{'id':<22} {'role':<8} {'model':<22} {'active':>8} {'tokens':>8} {'cost':>7} {'ev':>4}  steps")
        for a in m["agents"]:
            act = sum(e - s for s, e, _ in a["spans"])
            out.append(f"{a['id'][:22]:<22} {a['role']:<8} {(a['model'] or '-')[:22]:<22} {fmt_dur(act):>8} "
                       f"{fmt_tok(a['tokens']):>8} {('$%.2f' % a['cost']):>7} {a['events']:>4}  "
                       + ",".join(a["steps"]) + (f"  (parent {a['parent']})" if a.get("parent") else ""))
        out.append("")

    if "dag" in S and m["nodes"]:
        out.append("## DAG (current state, by layer)")
        for i, layer in enumerate(topo_layers(m["nodes"])):
            for n in layer:
                st = "removed" if n["removed"] else n["status"]
                runs = "; ".join(f"{rel(s)}–{rel(e)} {st2}" for s, e, st2 in n["runs"])
                out.append(f"L{i} {STATUS_MARK.get(st, '?')} {n['id']} [{st}]"
                           + (f" ←{','.join(n['deps'])}" if n["deps"] else "")
                           + (f" @{n['assignee']}" if n["assignee"] else "")
                           + (f" att={n['attempts']}" if n["attempts"] > 1 else "")
                           + f" :: {clip(n['objective'], 140)}")
                if n.get("reason"):
                    out.append(f"     reason: {clip(n['reason'], 200)}")
                if runs:
                    out.append(f"     runs: {runs}")
        out.append("")

    if "dag-history" in S and m["dag_ops"]:
        out.append("## DAG HISTORY")
        for o in m["dag_ops"]:
            if since is not None and o["t"] < since:
                continue
            desc = {"add": lambda: f"+ {o['node']}" + (f" ←{','.join(o['deps'])}" if o.get('deps') else "")
                    + (f" @{o['assignee']}" if o.get('assignee') else "") + f" :: {clip(o.get('objective'), 100)}",
                    "update": lambda: f"~ {o['node']} " + (f"fields={','.join(o['fields'])} " if o.get('fields') else "")
                    + (f"deps→{','.join(o['deps']) or '∅'} " if o.get('deps') is not None else "")
                    + clip(o.get('note') or o.get('objective'), 100),
                    "status": lambda: f"{STATUS_MARK.get(o['status'], '?')} {o['node']} {o.get('prev')}→{o['status']}"
                    + (f" ({clip(o.get('reason'), 120)})" if o.get('reason') else ""),
                    "remove": lambda: f"⌫ {o['node']}" + (f" ({clip(o.get('reason'), 120)})" if o.get('reason') else ""),
                    "dep_add": lambda: f"+edge {','.join(o.get('deps') or [])}→{o['node']}",
                    "dep_rm": lambda: f"-edge {','.join(o.get('deps') or [])}→{o['node']}",
                    }.get(o["op"], lambda: f"{o['op']} {o['node']}")()
            out.append(f"{rel(o['t'])} [{o.get('by', '?')}] {desc}")
        out.append("")

    if "hyps" in S and m["hyps"]:
        out.append("## HYPOTHESES")
        for h in m["hyps"]:
            out.append(f"{h['id']} [{h['status']}] {clip(h['claim'], 200)}"
                       + (f" (step {h['step']})" if h.get("step") else ""))
            for e in h["history"]:
                if e.get("evidence"):
                    out.append(f"    {rel(e['t'])} {e['status']}: {clip(e['evidence'], 200)}")
        out.append("")

    if "incidents" in S and m["incidents"]:
        out.append("## INCIDENTS / BUGS")
        for i in m["incidents"]:
            out.append(f"{i['id']} [{i['severity']}/{i['category']}] [{i['status']}] @{rel(i['created'])} "
                       f"{i['title']}" + (f" (agent {i['agent']})" if i.get("agent") else "")
                       + (f" (step {i['step']})" if i.get("step") else ""))
            for k in ("detail", "impact", "workaround"):
                if i.get(k):
                    out.append(f"    {k}: {clip(i[k], max(detail, 300))}")
            for h in i["history"]:
                if h.get("note"):
                    out.append(f"    {rel(h['t'])} {h.get('op')}: {clip(h['note'], 200)}")
        out.append("")

    if "timeline" in S:
        out.append("## TIMELINE")
        items = m["items"]
        if agent_filter:
            items = [i for i in items if i.get("agent") in agent_filter or i.get("to") in agent_filter
                     or i.get("by") in agent_filter]
        if kinds:
            items = [i for i in items if i.get("kind") in kinds]
        if since is not None:
            items = [i for i in items if i["t"] >= since]
        if last:
            items = items[-last:]
        for i in items:
            who = i.get("agent") or i.get("by") or "-"
            line = f"{rel(i['t'])} [{who}{'→' + i['to'] if i.get('to') else ''}] {i.get('kind', '?')}: {clip(i.get('title'), 200)}"
            extras = []
            if i.get("step"):
                extras.append(f"step={i['step']}")
            if i.get("tokens"):
                extras.append(f"+{fmt_tok(i['tokens'])}tok")
            if i.get("refs"):
                extras.append("refs=" + ",".join(i["refs"]))
            if i.get("id"):
                extras.append(i["id"])
            if extras:
                line += "  (" + " ".join(extras) + ")"
            out.append(line)
            if detail and i.get("detail"):
                out.append("      " + clip(i["detail"]))
        out.append("")
    return "\n".join(out)


# ----------------------------------------------------------------------------
# HTML renderer
# ----------------------------------------------------------------------------
THEME = HERE / "theme.css"
LOGO = HERE / "assets" / "logo.svg"


def logo_svg(css_class: str = "brand-mark", uid: str = "lg") -> str:
    """The inslopnia eye-and-sun mark as inline SVG (eye stroke follows the theme ink)."""
    return LOGO.read_text(encoding="utf-8").strip().replace("__CLASS__", css_class).replace("__ID__", f"{uid}-glow")


def favicon_link() -> str:
    from urllib.parse import quote
    svg = logo_svg("", "f").replace('class="" ', 'xmlns="http://www.w3.org/2000/svg" ').replace("var(--ink)", "#141414")
    return f'<link rel="icon" href="data:image/svg+xml,{quote(svg)}">'


def apply_brand(page: str, uid: str = "lg") -> str:
    """Inline the shared design system into a page: /*__THEME__*/, <!--LOGO-->, <!--FAVICON-->."""
    return (page.replace("/*__THEME__*/", THEME.read_text(encoding="utf-8"))
                .replace("<!--LOGO-->", logo_svg("brand-mark", uid))
                .replace("<!--FAVICON-->", favicon_link()))


def render_html(m: dict, title=None) -> str:
    tpl = apply_brand(TEMPLATE.read_text(encoding="utf-8"))
    data = json.dumps(m, ensure_ascii=False, default=str).replace("</", "<\\/")
    t = title or m["session"].get("title") or "Session timeline"
    return tpl.replace("/*__DATA__*/null", data).replace("__TITLE__", html.escape(t))


# ----------------------------------------------------------------------------
# SQLite renderer (normalized tables + analysis views; many sessions per DB)
# ----------------------------------------------------------------------------
SQLITE_SCHEMA_VERSION = 2
SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS schema_info (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY, title TEXT, task TEXT, goal TEXT,
  started REAL, ended REAL, duration_s REAL, started_iso TEXT,
  outcome TEXT, flag TEXT, summary TEXT,
  tokens REAL, input REAL, output REAL, cache_read REAL, cache_write REAL, cost REAL,
  n_agents INTEGER, n_events INTEGER, n_nodes INTEGER, nodes_done INTEGER, nodes_failed INTEGER,
  n_dag_ops INTEGER, incidents INTEGER, incidents_open INTEGER, hyps INTEGER, hyps_confirmed INTEGER,
  meta_json TEXT, source_file TEXT, rendered_at REAL);
CREATE TABLE IF NOT EXISTS agents (
  session_id TEXT, agent_id TEXT, role TEXT, model TEXT, parent TEXT, label TEXT,
  tokens REAL, input REAL, output REAL, cache_read REAL, cache_write REAL, cost REAL,
  events INTEGER, active_s REAL, first_t REAL, last_t REAL, steps_json TEXT,
  PRIMARY KEY (session_id, agent_id));
CREATE TABLE IF NOT EXISTS agent_spans (
  session_id TEXT, agent_id TEXT, start REAL, end REAL, start_rel REAL, duration_s REAL, status TEXT);
CREATE TABLE IF NOT EXISTS nodes (
  session_id TEXT, node_id TEXT, objective TEXT, detail TEXT, assignee TEXT, status TEXT,
  removed INTEGER, attempts INTEGER, added REAL, added_rel REAL, added_by TEXT, reason TEXT, task TEXT,
  deps_json TEXT, PRIMARY KEY (session_id, node_id));
CREATE TABLE IF NOT EXISTS node_deps (session_id TEXT, node_id TEXT, dep_id TEXT);
CREATE TABLE IF NOT EXISTS node_runs (
  session_id TEXT, node_id TEXT, attempt INTEGER, start REAL, end REAL, start_rel REAL,
  duration_s REAL, status TEXT);
CREATE TABLE IF NOT EXISTS dag_ops (
  session_id TEXT, seq INTEGER, record_id TEXT, t REAL, t_rel REAL, op TEXT, node_id TEXT,
  by_agent TEXT, status TEXT, prev_status TEXT, reason TEXT, objective TEXT, deps_json TEXT,
  assignee TEXT, note TEXT, fields_json TEXT);
CREATE TABLE IF NOT EXISTS events (
  session_id TEXT, seq INTEGER, record_id TEXT, t REAL, t_rel REAL, agent TEXT, to_agent TEXT,
  kind TEXT, title TEXT, detail TEXT, step TEXT, tokens REAL, cost REAL, status TEXT,
  refs_json TEXT, tags_json TEXT, source_type TEXT);
CREATE TABLE IF NOT EXISTS usage (
  session_id TEXT, seq INTEGER, t REAL, t_rel REAL, agent TEXT, role TEXT, step TEXT,
  tokens REAL, input REAL, output REAL, cache_read REAL, cache_write REAL, cost REAL);
CREATE TABLE IF NOT EXISTS hypotheses (
  session_id TEXT, hyp_id TEXT, claim TEXT, status TEXT, agent TEXT, step TEXT,
  created REAL, created_rel REAL, updated REAL, PRIMARY KEY (session_id, hyp_id));
CREATE TABLE IF NOT EXISTS hyp_history (
  session_id TEXT, hyp_id TEXT, t REAL, t_rel REAL, op TEXT, status TEXT, agent TEXT, evidence TEXT);
CREATE TABLE IF NOT EXISTS incidents (
  session_id TEXT, incident_id TEXT, title TEXT, severity TEXT, category TEXT, status TEXT,
  agent TEXT, step TEXT, detail TEXT, impact TEXT, workaround TEXT, created REAL, created_rel REAL,
  resolved REAL, time_to_resolve_s REAL, auto INTEGER, mitigated REAL, PRIMARY KEY (session_id, incident_id));
CREATE TABLE IF NOT EXISTS incident_history (
  session_id TEXT, incident_id TEXT, t REAL, t_rel REAL, op TEXT, status TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS flags (
  session_id TEXT, t REAL, t_rel REAL, value TEXT, status TEXT, agent TEXT, note TEXT, task TEXT);
CREATE TABLE IF NOT EXISTS outcomes (
  session_id TEXT, t REAL, t_rel REAL, task TEXT, status TEXT, flag TEXT, summary TEXT, by_agent TEXT);
CREATE TABLE IF NOT EXISTS phases (
  session_id TEXT, name TEXT, start REAL, end REAL, start_rel REAL, duration_s REAL, agent TEXT,
  note TEXT, open INTEGER);
CREATE TABLE IF NOT EXISTS records (
  session_id TEXT, seq INTEGER, record_id TEXT, type TEXT, t REAL, json TEXT);
CREATE INDEX IF NOT EXISTS ix_events_st ON events(session_id, t);
CREATE INDEX IF NOT EXISTS ix_events_step ON events(session_id, step);
CREATE INDEX IF NOT EXISTS ix_usage_st ON usage(session_id, t);
CREATE INDEX IF NOT EXISTS ix_dagops_st ON dag_ops(session_id, t);
CREATE INDEX IF NOT EXISTS ix_records_st ON records(session_id, seq);

DROP VIEW IF EXISTS v_timeline;
CREATE VIEW v_timeline AS
  SELECT session_id, t_rel, printf('%02d:%02d:%02d', CAST(t_rel AS INT)/3600, CAST(t_rel AS INT)%3600/60, CAST(t_rel AS INT)%60) AS t_plus,
         'event' AS src, agent, to_agent, kind, step, title, tokens FROM events
  UNION ALL
  SELECT session_id, t_rel, printf('%02d:%02d:%02d', CAST(t_rel AS INT)/3600, CAST(t_rel AS INT)%3600/60, CAST(t_rel AS INT)%60),
         'dag', by_agent, NULL, 'dag_' || op, node_id,
         op || ' ' || node_id || COALESCE(' ' || prev_status || '->' || status, '') || COALESCE(': ' || objective, '') || COALESCE(' (' || reason || ')', ''), NULL FROM dag_ops
  ORDER BY session_id, t_rel;
DROP VIEW IF EXISTS v_agent_share;
CREATE VIEW v_agent_share AS
  SELECT a.session_id, a.agent_id, a.role, a.model, a.tokens, a.cost, a.active_s, a.events,
         ROUND(100.0 * a.tokens / NULLIF(s.tokens, 0), 1) AS pct_tokens,
         ROUND(100.0 * a.cost / NULLIF(s.cost, 0), 1) AS pct_cost,
         ROUND(a.tokens / NULLIF(a.active_s / 60.0, 0)) AS tokens_per_active_min
  FROM agents a JOIN sessions s USING (session_id);
DROP VIEW IF EXISTS v_role_totals;
CREATE VIEW v_role_totals AS
  SELECT session_id, role, COUNT(*) AS agents, SUM(tokens) AS tokens, SUM(cost) AS cost,
         SUM(active_s) AS active_s, SUM(events) AS events FROM agents GROUP BY session_id, role;
DROP VIEW IF EXISTS v_node_summary;
CREATE VIEW v_node_summary AS
  SELECT n.session_id, n.node_id, CASE WHEN n.removed THEN 'removed' ELSE n.status END AS status, n.removed, n.assignee, n.attempts, n.added_by, n.added_rel,
         (SELECT SUM(duration_s) FROM node_runs r WHERE r.session_id = n.session_id AND r.node_id = n.node_id) AS run_s,
         (SELECT COUNT(*) FROM events e WHERE e.session_id = n.session_id AND e.step = n.node_id) AS events,
         (SELECT SUM(u.tokens) FROM usage u WHERE u.session_id = n.session_id
             AND (u.step = n.node_id OR (u.step IS NULL AND u.agent = n.assignee))) AS tokens,
         (SELECT SUM(u.cost) FROM usage u WHERE u.session_id = n.session_id
             AND (u.step = n.node_id OR (u.step IS NULL AND u.agent = n.assignee))) AS cost,
         (SELECT COUNT(*) FROM incidents i WHERE i.session_id = n.session_id AND i.step = n.node_id) AS incidents,
         n.objective
  FROM nodes n;
DROP VIEW IF EXISTS v_tokens_per_minute;
CREATE VIEW v_tokens_per_minute AS
  SELECT session_id, CAST(t_rel / 60 AS INT) AS minute, role, SUM(tokens) AS tokens, SUM(cost) AS cost
  FROM usage GROUP BY session_id, minute, role ORDER BY session_id, minute, role;
DROP VIEW IF EXISTS v_open_incidents;
CREATE VIEW v_open_incidents AS
  SELECT * FROM incidents WHERE status IN ('open', 'investigating')
  ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, created;
DROP VIEW IF EXISTS v_sessions_compare;
CREATE VIEW v_sessions_compare AS
  SELECT session_id, task, outcome, ROUND(duration_s / 60.0, 1) AS minutes, tokens, ROUND(cost, 2) AS cost,
         n_agents, n_nodes, nodes_done, nodes_failed, n_dag_ops, incidents, incidents_open, hyps_confirmed,
         (SELECT MIN(t_rel) FROM flags f WHERE f.session_id = s.session_id AND f.status = 'accepted') AS first_flag_s
  FROM sessions s ORDER BY started;
"""
SQLITE_TABLES = ["sessions", "agents", "agent_spans", "nodes", "node_deps", "node_runs", "dag_ops", "events",
                 "usage", "hypotheses", "hyp_history", "incidents", "incident_history", "flags", "outcomes",
                 "phases", "records"]


def render_sqlite(m: dict, records: list[dict], db_path: str | Path, session_id: str,
                  source_file: str | None = None, fresh: bool = False) -> dict:
    """Write the folded model into normalized tables. Rows of `session_id` are replaced;
    other sessions in the same DB are kept, so one DB can hold many runs for comparison."""
    db_path = Path(db_path)
    if fresh and db_path.exists():
        db_path.unlink()
    t0 = m["t0"]
    R = lambda t: None if t is None else round(t - t0, 3)
    J = lambda v: None if v in (None, [], {}) else json.dumps(v, ensure_ascii=False, default=str)
    sid = session_id
    con = sqlite3.connect(db_path)
    try:
        con.executescript(SQLITE_DDL)
        # migrations for DBs created by older versions
        cols = {r[1] for r in con.execute("PRAGMA table_info(incidents)")}
        if "mitigated" not in cols:
            con.execute("ALTER TABLE incidents ADD COLUMN mitigated REAL")
        with con:
            for tbl in SQLITE_TABLES:
                con.execute(f"DELETE FROM {tbl} WHERE session_id = ?", (sid,))
            con.execute("INSERT OR REPLACE INTO schema_info VALUES ('version', ?)", (str(SQLITE_SCHEMA_VERSION),))
            ses, tt, fin = m["session"], m["totals"], m["final"] or {}
            nc = tt["nodes"]
            con.execute("INSERT INTO sessions VALUES (" + ",".join("?" * 30) + ")", (
                sid, ses.get("title"), ses.get("task"), ses.get("goal"), t0, m["t1"], m["t1"] - t0,
                _dt.datetime.fromtimestamp(t0).isoformat(timespec="seconds"),
                fin.get("status"), fin.get("flag"), fin.get("summary"),
                tt["tokens"], tt["input"], tt["output"], tt["cache_read"],
                sum(a["cache_write"] for a in m["agents"]), tt["cost"],
                tt["agents"], tt["events"], sum(nc.values()), nc.get("done", 0), nc.get("failed", 0),
                len(m["dag_ops"]), tt["incidents"], tt["incidents_open"], len(m["hyps"]),
                sum(1 for h in m["hyps"] if h["status"] == "confirmed"),
                J(ses.get("meta")), source_file, time.time()))
            con.executemany("INSERT INTO agents VALUES (" + ",".join("?" * 17) + ")", [
                (sid, a["id"], a["role"], a["model"], a["parent"], a["label"], a["tokens"], a["input"],
                 a["output"], a["cache_read"], a["cache_write"], a["cost"], a["events"],
                 sum(e - s for s, e, _ in a["spans"]), a["first"], a["last"], J(a["steps"]))
                for a in m["agents"]])
            con.executemany("INSERT INTO agent_spans VALUES (?,?,?,?,?,?,?)", [
                (sid, a["id"], s, e, R(s), e - s, st) for a in m["agents"] for s, e, st in a["spans"]])
            con.executemany("INSERT INTO nodes VALUES (" + ",".join("?" * 14) + ")", [
                (sid, n["id"], n["objective"], n.get("detail"), n["assignee"], n["status"], int(n["removed"]),
                 n["attempts"], n["added"], R(n["added"]), n.get("added_by"), n.get("reason"), n.get("task"),
                 J(n["deps"])) for n in m["nodes"]])
            con.executemany("INSERT INTO node_deps VALUES (?,?,?)", [
                (sid, n["id"], d) for n in m["nodes"] for d in n["deps"]])
            con.executemany("INSERT INTO node_runs VALUES (?,?,?,?,?,?,?,?)", [
                (sid, n["id"], i + 1, s, e, R(s), (e - s) if e is not None else None, st)
                for n in m["nodes"] for i, (s, e, st) in enumerate(n["runs"])])
            con.executemany("INSERT INTO dag_ops VALUES (" + ",".join("?" * 16) + ")", [
                (sid, i, o.get("id"), o["t"], R(o["t"]), o["op"], o["node"], o.get("by"), o.get("status"),
                 o.get("prev"), o.get("reason"), o.get("objective"), J(o.get("deps")) if "deps" in o else None,
                 o.get("assignee"), o.get("note"), J(o.get("fields"))) for i, o in enumerate(m["dag_ops"])])
            con.executemany("INSERT INTO events VALUES (" + ",".join("?" * 17) + ")", [
                (sid, i, it.get("id"), it["t"], R(it["t"]), it.get("agent") or it.get("by"), it.get("to"),
                 it.get("kind"), it.get("title"), it.get("detail") or it.get("summary") or it.get("evidence")
                 or it.get("note"), it.get("step"), it.get("tokens"), it.get("cost"), it.get("status"),
                 J(it.get("refs")), J(it.get("tags")), it.get("type")) for i, it in enumerate(m["items"])])
            role = {a["id"]: a["role"] for a in m["agents"]}
            con.executemany("INSERT INTO usage VALUES (" + ",".join("?" * 13) + ")", [
                (sid, i, u["t"], R(u["t"]), u["agent"], role.get(u["agent"]), u.get("step"), u["tokens"],
                 u.get("input"), u.get("output"), u.get("cache_read"), u.get("cache_write"), u["cost"])
                for i, u in enumerate(m["usage"])])
            con.executemany("INSERT INTO hypotheses VALUES (?,?,?,?,?,?,?,?,?)", [
                (sid, h["id"], h["claim"], h["status"], h.get("agent"), h.get("step"), h["created"],
                 R(h["created"]), h.get("updated")) for h in m["hyps"]])
            con.executemany("INSERT INTO hyp_history VALUES (?,?,?,?,?,?,?,?)", [
                (sid, h["id"], e["t"], R(e["t"]), e.get("op"), e.get("status"), e.get("agent"), e.get("evidence"))
                for h in m["hyps"] for e in h["history"]])
            con.executemany("INSERT INTO incidents VALUES (" + ",".join("?" * 17) + ")", [
                (sid, i["id"], i["title"], i["severity"], i["category"], i["status"], i.get("agent"),
                 i.get("step"), i.get("detail"), i.get("impact"), i.get("workaround"), i["created"],
                 R(i["created"]), i.get("resolved"),
                 (i["resolved"] - i["created"]) if i.get("resolved") else None, int(bool(i.get("auto"))),
                 i.get("mitigated"))
                for i in m["incidents"]])
            con.executemany("INSERT INTO incident_history VALUES (?,?,?,?,?,?,?)", [
                (sid, i["id"], h["t"], R(h["t"]), h.get("op"), h.get("status"), h.get("note"))
                for i in m["incidents"] for h in i["history"]])
            con.executemany("INSERT INTO flags VALUES (?,?,?,?,?,?,?,?)", [
                (sid, f["t"], R(f["t"]), f["value"], f.get("status"), f.get("agent"), f.get("note"), f.get("task"))
                for f in m["flags"]])
            con.executemany("INSERT INTO outcomes VALUES (?,?,?,?,?,?,?,?)", [
                (sid, o["t"], R(o["t"]), o.get("task") or "main", o["status"], o.get("flag"), o.get("summary"),
                 o.get("by")) for o in m["outcomes"]])
            con.executemany("INSERT INTO phases VALUES (?,?,?,?,?,?,?,?,?)", [
                (sid, p["name"], p["start"], p["end"], R(p["start"]), p["end"] - p["start"], p.get("agent"),
                 p.get("note"), int(bool(p.get("open")))) for p in m["phases"]])
            con.executemany("INSERT INTO records VALUES (?,?,?,?,?,?)", [
                (sid, i, r.get("id"), r.get("type"), r.get("t"), json.dumps(r, ensure_ascii=False))
                for i, r in enumerate(records)])
        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t} WHERE session_id = ?", (sid,)).fetchone()[0]
                  for t in SQLITE_TABLES}
        counts["_sessions_in_db"] = con.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        return counts
    finally:
        con.close()


# ----------------------------------------------------------------------------
# importer: inslopnia_harness run dir
# ----------------------------------------------------------------------------
def _short(s, n=2000):
    s = s or ""
    return s if len(s) <= n else s[: n - 1] + "…"


def import_harness(run_dir: Path, bucket_s: float = 60.0) -> list[dict]:
    run_dir = Path(run_dir)
    db = run_dir / "ledger.sqlite3"
    if not db.exists():
        raise SystemExit(f"no ledger.sqlite3 in {run_dir}")
    con = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    kv = {r["k"]: r["v"] for r in con.execute("SELECT k, v FROM kv")}
    jl = lambda s: json.loads(s) if s else {}
    steps = {r["id"]: dict(r) for r in con.execute("SELECT * FROM steps ORDER BY ord")}
    for s in steps.values():
        s["spec"] = jl(s["spec"])
        s["deps"] = jl(s["deps"])
        s["result"] = jl(s["result"]) if s["result"] else None
    events = [dict(r) for r in con.execute("SELECT * FROM events ORDER BY seq")]
    forks = []
    try:
        forks = [dict(r) for r in con.execute("SELECT * FROM forks")]
    except sqlite3.Error:
        pass

    cfg = {}
    snap = run_dir / "config.snapshot.json"
    if snap.exists():
        cfg = json.loads(snap.read_text(encoding="utf-8"))
    pi = cfg.get("pi", {})
    model = f"{pi.get('provider')}/{pi.get('model')}" if pi.get("model") else None
    challenge = jl(kv.get("challenge"))
    started = float(jl(kv.get("run_started_at")) or (events[0]["ts"] if events else time.time()))

    out: list[dict] = []
    seq = [0]

    def add(rec):
        seq[0] += 1
        rec.setdefault("id", f"h{seq[0]}")
        out.append({k: v for k, v in rec.items() if v is not None and v != ""})

    def agent_of(step_id):
        s = steps.get(step_id, {})
        role = (s.get("spec") or {}).get("assigned_agent") or "E"
        return f"{role}:{step_id}", role

    add({"type": "session", "t": started, "title": challenge.get("title") or run_dir.parent.name,
         "task": run_dir.parent.name, "goal": _short(challenge.get("goal"), 1500),
         "meta": {"source": str(run_dir.resolve()), "model": model, "thinking": pi.get("thinking"),
                  "pi": jl(kv.get("pi_version")),
                  "max_parallel": (cfg.get("limits") or {}).get("max_parallel_agents")}})
    add({"type": "agent", "t": started, "agent": "P", "role": "P", "model": model, "label": "planner",
         "state": "start"})
    add({"type": "agent", "t": started, "agent": "runtime", "role": "runtime", "label": "supervisor"})

    declared = set()
    last_deps_update = {}
    for e in events:
        if e["kind"] == "step_updated":
            last_deps_update[e["step_id"]] = e["seq"]
    planner_end = None
    for e in events:
        t, sid, kind, d = e["ts"], e["step_id"], e["kind"], jl(e["data"])
        by = d.get("by")
        by_agent = {"planner": "P", "operator": "operator", "harness": "runtime",
                    "supervisor": "runtime"}.get(by, by)
        if sid and sid in steps and sid not in declared:
            aid, role = agent_of(sid)
            spec_model = steps[sid]["spec"].get("model")
            add({"type": "agent", "t": t, "agent": aid, "role": role, "parent": "P",
                 "model": (f"{steps[sid]['spec'].get('provider')}/{spec_model}" if spec_model else model),
                 "label": steps[sid]["spec"].get("objective", "")[:80]})
            declared.add(sid)
        aid = agent_of(sid)[0] if sid else None
        if kind == "step_added":
            spec = steps.get(sid, {}).get("spec", {})
            add({"type": "dag", "t": t, "op": "add", "node": sid, "by": by_agent or "P",
                 "objective": d.get("objective"), "deps": d.get("deps") or [], "assignee": aid,
                 "detail": _short(spec.get("instructions"), 4000)})
        elif kind == "step_updated":
            rec = {"type": "dag", "t": t, "op": "update", "node": sid, "by": by_agent or "P",
                   "fields": d.get("fields")}
            if "deps" in (d.get("fields") or []) and last_deps_update.get(sid) == e["seq"]:
                rec["deps"] = steps.get(sid, {}).get("deps")
            add(rec)
        elif kind == "step_started":
            add({"type": "dag", "t": t, "op": "status", "node": sid, "status": "running",
                 "by": by_agent or "P", "reason": f"attempt {d.get('attempt')}"})
        elif kind == "step_done":
            add({"type": "dag", "t": t, "op": "status", "node": sid, "status": "done", "by": "runtime",
                 "reason": "verified" if d.get("verified") else "agent claim"})
            add({"type": "event", "t": t, "agent": aid, "to": "P", "kind": "result", "step": sid,
                 "title": "step done: " + _short((d.get("summary") or "").replace("TASK_COMPLETE", "").strip(), 160),
                 "detail": _short(d.get("summary"), 4000)})
        elif kind == "step_failed":
            add({"type": "dag", "t": t, "op": "status", "node": sid, "status": "failed", "by": "runtime",
                 "reason": f"{d.get('reason')}: {_short(d.get('error'), 300)}"})
            add({"type": "incident", "t": t, "op": "add", "iid": f"I-{sid}-{d.get('attempt')}",
                 "title": f"step {sid} failed ({d.get('reason')})", "agent": aid, "step": sid,
                 "severity": "high" if d.get("reason") in ("timeout", "runner_error", "protocol") else "medium",
                 "category": "agent" if d.get("reason") == "blocked" else "harness",
                 "detail": _short(d.get("error"), 1500), "status": "open", "auto": True})
        elif kind == "step_cancelled":
            add({"type": "dag", "t": t, "op": "status", "node": sid, "status": "cancelled",
                 "by": by_agent or "P", "reason": _short(d.get("reason"), 400)})
        elif kind == "step_verified":
            add({"type": "event", "t": t, "agent": "runtime", "kind": "result", "step": sid,
                 "title": f"acceptance {'passed' if d.get('verified') else 'failed'} for {sid}",
                 "detail": _short(d.get("report"), 2000)})
        elif kind == "step_report":
            add({"type": "event", "t": t, "agent": aid, "to": "P", "kind": "milestone", "step": sid,
                 "title": _short(d.get("summary"), 160), "detail": _short(d.get("summary"), 4000)})
        elif kind == "executor_answer":
            add({"type": "event", "t": t, "agent": aid, "to": "P", "kind": "message", "step": sid,
                 "title": "answer: " + _short(d.get("body"), 150), "detail": d.get("body")})
        elif kind == "exec_directive":
            add({"type": "event", "t": t, "agent": by_agent or "P", "to": aid, "kind": "message", "step": sid,
                 "title": f"{d.get('msg_kind')} → {sid}: " + _short(d.get("body"), 140), "detail": d.get("body")})
        elif kind.startswith("operator_") and kind != "operator_stop":
            add({"type": "event", "t": t, "agent": "operator", "to": "P", "kind": "message",
                 "title": f"{kind.split('_', 1)[1]}: " + _short(d.get("body"), 150), "detail": d.get("body")})
        elif kind == "operator_stop":
            add({"type": "event", "t": t, "agent": "operator", "kind": "decision", "title": "operator STOP"})
        elif kind == "fork_opened":
            hy = d.get("hypotheses") or []
            add({"type": "event", "t": t, "agent": aid, "to": "P", "kind": "decision", "step": sid,
                 "title": f"fork {d.get('fork_id')} raised ({len(hy)} hypotheses, lead {d.get('lead')})",
                 "detail": json.dumps(d, ensure_ascii=False)[:3000]})
            for h in hy:
                if isinstance(h, dict) and h.get("id"):
                    add({"type": "hyp", "t": t, "op": "add", "hid": f"{d.get('fork_id')}:{h['id']}",
                         "claim": h.get("claim") or "", "agent": aid, "step": sid})
        elif kind == "fork_resolved":
            add({"type": "event", "t": t, "agent": by_agent or "P", "to": aid, "kind": "decision", "step": sid,
                 "title": f"fork {d.get('fork_id')} {d.get('status')}" + (f" → {d.get('winner')}" if d.get("winner") else ""),
                 "detail": json.dumps(d, ensure_ascii=False)[:3000]})
        elif kind in ("stale_agent_killed", "stale_planner_killed", "critic_error", "pi_version_drift"):
            add({"type": "incident", "t": t, "op": "add", "iid": f"I-{kind}-{e['seq']}", "title": kind.replace("_", " "),
                 "severity": "medium", "category": "harness", "agent": aid, "step": sid,
                 "detail": json.dumps(d, ensure_ascii=False)[:1500], "auto": True})
        elif kind == "cancel_requested":
            add({"type": "event", "t": t, "agent": by_agent or "runtime", "to": aid, "kind": "decision", "step": sid,
                 "title": f"cancel requested for {sid}: " + _short(d.get("reason"), 120)})
        elif kind == "planner_turn_end":
            planner_end = t
            add({"type": "event", "t": t, "agent": "P", "kind": "note",
                 "title": f"planner turn {d.get('turn')} ended ({d.get('reason')}, exit {d.get('exit_code')}, "
                          f"{d.get('tool_calls')} tool calls)", "detail": _short(d.get("final"), 2000)})
            if d.get("reason") not in ("exited", None):
                add({"type": "incident", "t": t, "op": "add", "iid": f"I-planner-{d.get('turn')}",
                     "title": f"planner turn ended abnormally: {d.get('reason')} (exit {d.get('exit_code')})",
                     "severity": "medium", "category": "harness", "agent": "P", "auto": True})
        elif kind == "run_finished":
            outcome = d.get("outcome")
            summ = d.get("summary") or ""
            fl = re.search(r"FLAG:\s*(\S+)", summ)
            st = {"solved": "solved", "operator_stop": "abandoned"}.get(outcome, "unsolved" if outcome else "in_progress")
            if fl:
                add({"type": "flag", "t": t, "value": fl.group(1), "status": "accepted" if st == "solved" else "candidate",
                     "agent": by_agent or "P"})
            add({"type": "outcome", "t": t, "status": st, "summary": _short(summ, 3000),
                 "flag": fl.group(1) if fl else None, "by": by_agent, "raw": outcome})
        elif kind == "step_progress":
            pass  # token spend is imported from pi-sessions below
        elif kind == "stage" and d.get("stage") not in ("running",):
            add({"type": "event", "t": t, "agent": "runtime", "kind": "note", "step": sid,
                 "title": f"{sid} stage {d.get('stage')}"})

    # stop planner span
    if planner_end:
        add({"type": "agent", "t": planner_end, "agent": "P", "state": "stop", "status": "done"})

    # token usage from pi session files, bucketed per agent
    for f in sorted(glob.glob(str(run_dir / "pi-sessions" / "*.jsonl"))):
        name = Path(f).name
        mm = re.search(r"_(planner-run|([er])-(.+)-r\d+)\.jsonl$", name)
        if not mm:
            continue
        if mm.group(1) == "planner-run":
            aid = "P"
        else:
            sid = mm.group(3)
            aid = agent_of(sid)[0] if sid in steps else f"{mm.group(2).upper()}:{sid}"
        buckets: dict[int, dict] = {}
        sess_model = None
        tools: dict[int, list] = defaultdict(list)
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if o.get("type") == "model_change":
                    sess_model = f"{o.get('provider')}/{o.get('modelId')}"
                msg = o.get("message") or {}
                if o.get("type") != "message" or msg.get("role") != "assistant":
                    continue
                u = msg.get("usage") or {}
                ts = msg.get("timestamp")
                ts = ts / 1000 if ts else parse_time(o.get("timestamp"), None)
                b = int((ts - started) // bucket_s)
                bk = buckets.setdefault(b, {"t": ts, "input": 0, "output": 0, "cache_read": 0,
                                            "cache_write": 0, "cost": 0.0, "turns": 0})
                bk["t"] = ts
                bk["input"] += u.get("input") or 0
                bk["output"] += u.get("output") or 0
                bk["cache_read"] += u.get("cacheRead") or 0
                bk["cache_write"] += u.get("cacheWrite") or 0
                bk["cost"] += ((u.get("cost") or {}).get("total") or 0.0)
                bk["turns"] += 1
                for c in msg.get("content") or []:
                    if c.get("type") == "toolCall":
                        tools[b].append(c.get("name"))
        if sess_model and aid != "P":
            add({"type": "agent", "t": started, "agent": aid, "model": sess_model})
        for b in sorted(buckets):
            bk = buckets[b]
            add({"type": "usage", "t": bk["t"], "agent": aid, "input": bk["input"], "output": bk["output"],
                 "cache_read": bk["cache_read"], "cache_write": bk["cache_write"],
                 "cost": round(bk["cost"], 6), "turns": bk["turns"],
                 "tools": dict(sorted({x: tools[b].count(x) for x in set(tools[b])}.items()))})
    # unresolved forks still open
    for fk in forks:
        if fk.get("status") == "open":
            pass
    out.sort(key=lambda r: r["t"])
    return out


# ----------------------------------------------------------------------------
# transcript digest (compact reading of pi sessions with T+ times)
# ----------------------------------------------------------------------------
def _ts_of(o, m):
    ts = m.get("timestamp") if isinstance(m.get("timestamp"), (int, float)) else None
    if ts:
        return ts / 1000 if ts > 1e11 else ts
    if o.get("timestamp"):
        return parse_time(o["timestamp"], None)
    return None


def transcript_lines(path: Path, t0: float | None, grep=None, since=None, until=None, width=220,
                     results=160, thinking=False) -> list[str]:
    """One line per assistant text / tool call / tool result / user prompt of a pi session (or pi stream) file."""
    rx = re.compile(grep, re.I) if grep else None
    msgs = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            if o.get("type") not in ("message", "message_end"):
                continue
            m = o.get("message") or {}
            if m.get("role") in ("system",):
                continue
            msgs.append((_ts_of(o, m), m))
    if not msgs:
        return [f"(no messages in {path})"]
    base = t0 if t0 is not None else next(t for t, _ in msgs if t)
    one = lambda x: re.sub(r"\s+", " ", str(x or "")).strip()
    clip = lambda x, n: x if len(x) <= n else x[: n - 1] + "…"
    out = []
    last_t = base
    for t, m in msgs:
        t = t or last_t
        last_t = t
        if since is not None and t < since or until is not None and t > until:
            continue
        tag = "T+" + fmt_rel(t - base) if t >= base else "T-" + fmt_rel(base - t)
        role = m.get("role")
        rows = []
        if role == "user":
            for c in m.get("content") or []:
                if isinstance(c, dict) and c.get("type") == "text":
                    rows.append(("U ", one(c.get("text"))))
            if isinstance(m.get("content"), str):
                rows.append(("U ", one(m["content"])))
        elif role == "assistant":
            u = m.get("usage") or {}
            tok = f" ({fmt_tok(u.get('totalTokens'))} tok)" if u.get("totalTokens") else ""
            for c in m.get("content") or []:
                if c.get("type") == "text" and c.get("text", "").strip():
                    rows.append(("A ", one(c["text"]) + tok)); tok = ""
                elif c.get("type") == "thinking" and thinking and c.get("thinking", "").strip():
                    rows.append(("th", one(c["thinking"])))
                elif c.get("type") == "toolCall":
                    args = c.get("arguments") or {}
                    arg = args.get("command") or args.get("path") or args.get("pattern") or json.dumps(args, ensure_ascii=False)
                    rows.append(("→ ", f"{c.get('name')}: {one(arg)}" + tok)); tok = ""
        elif role == "toolResult" and results:
            txt = " ".join(one(c.get("text")) for c in m.get("content") or [] if isinstance(c, dict))
            rows.append(("← ", f"{m.get('toolName')}{' ERROR' if m.get('isError') else ''}: " + clip(txt, results)))
        for kind, text in rows:
            if rx and not rx.search(text):
                continue
            out.append(f"{tag:>9} {kind} {clip(text, width)}")
    return out


# ----------------------------------------------------------------------------
# validation
# ----------------------------------------------------------------------------
def validate_records(records: list[dict]) -> tuple[list[str], list[str]]:
    """Errors = the journal says something impossible; warnings = likely annotation slips."""
    errors: list[str] = []
    warns: list[str] = []
    if not records:
        return ["journal is empty"], []
    sess = [r for r in records if r.get("type") == "session"]
    if not sess:
        warns.append("no session record (run `init`)")
    t0 = sess[0]["t"] if sess else min(r["t"] for r in records)
    t_end = sess[0].get("ended") if sess else None
    now = time.time()
    rel = lambda t: ("T-" + fmt_rel(t0 - t)) if t < t0 else ("T+" + fmt_rel(t - t0))
    rid = lambda r: r.get("id") or "?"
    nodes, hyps, incs, agents_declared = set(), set(), set(), set()
    phases_open: dict[str, float] = {}
    order = sorted(enumerate(records), key=lambda p: (p[1].get("t", 0), p[0]))
    future: list[str] = []
    add_t = {}  # first add time per (kind, id), to tolerate refs a moment before their add
    for r in records:
        key = {"dag": ("dag", r.get("node")) if r.get("op") == "add" else None,
               "hyp": ("hyp", r.get("hid")) if r.get("op") == "add" else None,
               "incident": ("inc", r.get("iid")) if r.get("op") == "add" else None}.get(r.get("type"))
        if key and r.get("t") is not None:
            add_t[key] = min(add_t.get(key, r["t"]), r["t"])

    def early_ref(key, t, msg):
        at = add_t.get(key)
        if at is not None and at - t <= 2:
            warns.append(msg + f" ({at - t:.1f}s before its add — nearly simultaneous, kept)")
        else:
            errors.append(msg)
    for _, r in order:
        t, typ = r.get("t"), r.get("type")
        if t is None:
            errors.append(f"{rid(r)}: record without time")
            continue
        if typ != "session" and t < t0 - 1:
            errors.append(f"{rid(r)} ({typ}): {rel(t)} is before session start")
        if t > now + 300:
            future.append(rid(r))
        if t_end and t > t_end + 1 and typ != "session":
            warns.append(f"{rid(r)} ({typ}): {rel(t)} is after session end")
        if typ == "agent":
            agents_declared.add(r["agent"])
        elif typ == "dag":
            if r["op"] == "add":
                if r["node"] in nodes:
                    warns.append(f"{rid(r)}: node {r['node']} added twice")
                nodes.add(r["node"])
            elif r["node"] not in nodes:
                early_ref(("dag", r["node"]), t, f"{rid(r)}: dag {r['op']} on node {r['node']!r} at {rel(t)} before it was added")
        elif typ == "hyp":
            if r.get("op") == "add":
                hyps.add(r["hid"])
            elif r["hid"] not in hyps:
                early_ref(("hyp", r["hid"]), t, f"{rid(r)}: hyp set {r['hid']!r} at {rel(t)} before `hyp add`")
        elif typ == "incident":
            if r.get("op") == "add":
                incs.add(r["iid"])
                if r.get("category") and r["category"] not in INCIDENT_CATEGORIES:
                    warns.append(f"{rid(r)}: incident {r['iid']} unknown category {r['category']!r}")
            elif r["iid"] not in incs:
                early_ref(("inc", r["iid"]), t, f"{rid(r)}: incident {r.get('op')} {r['iid']!r} at {rel(t)} before `incident add`")
        elif typ == "phase":
            if r["op"] == "begin":
                if r["name"] in phases_open:
                    warns.append(f"{rid(r)}: phase {r['name']!r} begun twice without end")
                phases_open[r["name"]] = t
            elif phases_open.pop(r["name"], None) is None:
                errors.append(f"{rid(r)}: phase end {r['name']!r} without begin")
    if future:
        warns.append(f"{len(future)} records are dated in the future (fine for synthetic demos): "
                     + ", ".join(future[:6]) + (" …" if len(future) > 6 else ""))
    m = build_model(records)
    ids = {n["id"] for n in m["nodes"]}
    for n in m["nodes"]:
        for d in n["deps"]:
            if d not in ids:
                errors.append(f"node {n['id']}: unknown dependency {d!r}")
        if n["status"] == "running" and not n["assignee"]:
            warns.append(f"node {n['id']}: running without assignee (no lane/arrow in HTML)")
    for name in phases_open:
        warns.append(f"phase {name!r} never ended (rendered as open until session end)")
    roles = {a["id"]: a["role"] for a in m["agents"]}
    no_to = [i for i in m["items"] if i.get("type") == "event" and i.get("kind") in ("milestone", "result")
             and roles.get(i.get("agent")) in ("E", "R") and not i.get("to")]
    if no_to:
        warns.append(f"{len(no_to)} milestone/result events of E/R have no --to (no report arrow to P): "
                     + ", ".join(rid(i) for i in no_to[:8]) + (" …" if len(no_to) > 8 else ""))
    for i in m["items"]:
        if i.get("step") and i["step"] not in ids:
            warns.append(f"{rid(i)}: step {i['step']!r} is not a DAG node")
    for h in m["hyps"]:
        if h["status"] in ("confirmed", "refuted") and not any(x.get("evidence") for x in h["history"]):
            warns.append(f"hypothesis {h['id']} is {h['status']} without evidence (-e)")
    for inc in m["incidents"]:
        if inc["status"] == "resolved" and inc.get("category") == "harness" and not inc.get("auto")                 and not any(x.get("note") for x in inc["history"]):
            warns.append(f"incident {inc['id']}: harness bug marked resolved without note — "
                         f"if it was only worked around, use `incident mitigate`")
    if not m["final"]:
        warns.append("no outcome recorded yet")
    return errors, warns


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="timeline.py", description="Session timeline journal (JSONL) + text/HTML renderers.",
        epilog="Times (--t): now (default) | +12m / +1h5m / +90s (from session start) | epoch | "
               "ISO-8601 | HH:MM[:SS]. Lists: comma-separated.")
    ap.add_argument("-f", "--file", default=DEFAULT_FILE,
                    help="journal .jsonl, or a .db made by `render sqlite` (read-only; env TIMELINE_FILE; default %(default)s)")
    ap.add_argument("--session", help="session_id when -f is a multi-session SQLite DB")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def T(p):
        p.add_argument("--t", "--at", dest="t", help="event time (default now)")
        return p

    p = T(sub.add_parser("init", help="start a session"))
    p.add_argument("title")
    p.add_argument("--task")
    p.add_argument("--goal")
    p.add_argument("--meta", action="append", default=[], help="key=value (repeatable)")
    p.add_argument("--force", action="store_true", help="overwrite existing journal")

    p = T(sub.add_parser("agent", help="declare/update agent; --start/--stop mark activity span"))
    p.add_argument("agent")
    p.add_argument("--role", choices=ROLES)
    p.add_argument("--model")
    p.add_argument("--parent", help="spawning agent id")
    p.add_argument("--label")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--start", action="store_true")
    g.add_argument("--stop", action="store_true")
    p.add_argument("--status", help="with --stop: done|failed|killed|...")

    p = T(sub.add_parser("event", aliases=["ev"], help="log an event of an agent"))
    p.add_argument("agent")
    p.add_argument("title")
    p.add_argument("-k", "--kind", default="action", help="|".join(EVENT_KINDS) + " (free-form allowed)")
    p.add_argument("-d", "--detail")
    p.add_argument("-s", "--step", help="DAG node this event belongs to")
    p.add_argument("--to", help="recipient agent (message/delegation arrow)")
    p.add_argument("--tokens", type=float, help="tokens spent by this action (added to agent usage)")
    p.add_argument("--cost", type=float)
    p.add_argument("--refs", help="related record ids")
    p.add_argument("--tags")

    p = T(sub.add_parser("note", help="shortcut: event of kind note"))
    p.add_argument("title")
    p.add_argument("-a", "--agent", default="operator")
    p.add_argument("-d", "--detail")
    p.add_argument("-s", "--step")

    p = sub.add_parser("dag", help="DAG mutations")
    dsub = p.add_subparsers(dest="op", required=True)
    for name, hlp in [("add", "add node"), ("update", "change objective/deps/assignee"),
                      ("status", "set node status"), ("remove", "remove node"),
                      ("dep-add", "add dependency edges DEPS→NODE"), ("dep-rm", "remove dependency edges")]:
        q = T(dsub.add_parser(name, help=hlp))
        q.add_argument("node")
        if name == "status":
            q.add_argument("status", choices=NODE_STATUSES)
        if name in ("dep-add", "dep-rm"):
            q.add_argument("deps")
        q.add_argument("--by", help="agent that made the change (default P)", default="P")
        if name in ("add", "update"):
            q.add_argument("-o", "--objective")
            q.add_argument("--deps")
            q.add_argument("--assignee", help="agent executing this node")
            q.add_argument("-d", "--detail", help="instructions / spec")
            q.add_argument("--task", help="task id for multi-task sessions")
        q.add_argument("-r", "--reason")
        q.add_argument("--note")

    p = T(sub.add_parser("tokens", aliases=["usage"], help="record token spend of an agent"))
    p.add_argument("agent")
    p.add_argument("total", nargs="?", type=float, help="total tokens (or use --in/--out)")
    p.add_argument("--in", dest="input", type=float)
    p.add_argument("--out", dest="output", type=float)
    p.add_argument("--cache-read", type=float)
    p.add_argument("--cache-write", type=float)
    p.add_argument("--cost", type=float)
    p.add_argument("--cumulative", action="store_true", help="value is agent's running total, not a delta")
    p.add_argument("-s", "--step")

    p = sub.add_parser("hyp", help="hypotheses")
    hsub = p.add_subparsers(dest="op", required=True)
    q = T(hsub.add_parser("add"))
    q.add_argument("claim")
    q.add_argument("--hid", help="id (default H<n>)")
    q.add_argument("-a", "--agent")
    q.add_argument("-s", "--step")
    q = T(hsub.add_parser("set"))
    q.add_argument("hid")
    q.add_argument("status", choices=["open", "testing", "confirmed", "refuted", "abandoned"])
    q.add_argument("-e", "--evidence")
    q.add_argument("-a", "--agent")

    p = sub.add_parser("incident", aliases=["bug"], help="incidents / bugs found during the session")
    isub = p.add_subparsers(dest="op", required=True)
    q = T(isub.add_parser("add"))
    q.add_argument("title")
    q.add_argument("--sev", "--severity", dest="severity", choices=SEVERITIES, default="medium")
    q.add_argument("-c", "--category", default="other", choices=INCIDENT_CATEGORIES,
                   help="harness = bug in runtime code; orchestration = planner/DAG decisions; agent = executor "
                        "behaviour; model, infra, env, tooling, platform, other")
    q.add_argument("-a", "--agent")
    q.add_argument("-s", "--step")
    q.add_argument("-d", "--detail")
    q.add_argument("--impact")
    q.add_argument("--workaround")
    q.add_argument("--iid")
    q = T(isub.add_parser("update"))
    q.add_argument("iid")
    q.add_argument("--note")
    q.add_argument("--sev", "--severity", dest="severity", choices=SEVERITIES)
    q.add_argument("--status", choices=INCIDENT_STATUSES)
    q.add_argument("--workaround")
    q = T(isub.add_parser("resolve", help="root cause fixed / no longer a problem"))
    q.add_argument("iid")
    q.add_argument("--note")
    q = T(isub.add_parser("mitigate", help="worked around in this session, root cause NOT fixed"))
    q.add_argument("iid")
    q.add_argument("--note")
    q.add_argument("--workaround")

    p = T(sub.add_parser("flag", help="flag candidate / submission result"))
    p.add_argument("value")
    p.add_argument("--status", default="candidate", choices=["candidate", "submitted", "accepted", "rejected"])
    p.add_argument("-a", "--agent")
    p.add_argument("--note")
    p.add_argument("--task")

    p = T(sub.add_parser("outcome", aliases=["verdict"], help="task solved / unsolved"))
    p.add_argument("status", choices=OUTCOMES)
    p.add_argument("--summary")
    p.add_argument("--flag")
    p.add_argument("--task", help="for multi-task sessions (default main)")
    p.add_argument("--by")

    p = T(sub.add_parser("phase", help="named phase band (recon, exploit, ...)"))
    p.add_argument("action", choices=["begin", "end"])
    p.add_argument("name")
    p.add_argument("-a", "--agent")
    p.add_argument("--note")

    p = T(sub.add_parser("end", help="mark session end time"))

    p = sub.add_parser("render", help="render text | html | json | sqlite")
    p.add_argument("format", choices=["text", "html", "json", "sqlite", "jsonl"])
    p.add_argument("--session-id", help="sqlite: session key (default: journal file stem)")
    p.add_argument("--fresh", action="store_true", help="sqlite: recreate the DB file instead of upserting")
    p.add_argument("-o", "--out")
    p.add_argument("--sections", help="text: summary,agents,dag,dag-history,hyps,incidents,timeline")
    p.add_argument("--agent", help="text: filter timeline by agents")
    p.add_argument("--kinds", help="text: filter timeline by kinds")
    p.add_argument("--since", help="text: only after this time")
    p.add_argument("--last", type=int, help="text: last N timeline items")
    p.add_argument("--detail", type=int, default=160, help="text: detail chars (0 = hide)")

    p = sub.add_parser("import-harness", help="build journal from an inslopnia_harness run dir")
    p.add_argument("run_dir")
    p.add_argument("--bucket", type=float, default=60.0, help="token bucket seconds")
    p.add_argument("--merge", action="store_true", help="keep existing manual records (drop old imported)")

    p = sub.add_parser("validate", help="check journal consistency (exit 1 on errors)")
    p.add_argument("--strict", action="store_true", help="treat warnings as errors")
    sub.add_parser("sessions", help="list sessions stored in a SQLite DB (-f file.db)")

    p = sub.add_parser("transcript", help="compact digest of a pi session file with T+ times of THIS journal")
    p.add_argument("session_file", nargs="+", help="pi-sessions/*.jsonl or logs/*.pi.jsonl (several = merged)")
    p.add_argument("-g", "--grep", help="regex filter on lines")
    p.add_argument("--since")
    p.add_argument("--until")
    p.add_argument("--width", type=int, default=220, help="max chars per line")
    p.add_argument("--results", type=int, default=160, help="chars of tool results (0 = hide)")
    p.add_argument("--thinking", action="store_true", help="include model thinking")
    p.add_argument("--last", type=int, help="only last N lines")

    a = ap.parse_args(argv)
    if a.cmd == "sessions":
        if not is_sqlite(Path(a.file)):
            raise SystemExit(f"{a.file} is not a SQLite DB")
        sys.stdout.reconfigure(encoding="utf-8")
        for sid, title, outcome, started, dur in db_sessions(Path(a.file)):
            print(f"{sid:<24} {outcome or '-':<12} {fmt_abs(started)}  {fmt_dur(dur or 0):>8}  {title}")
        return
    J = Journal(a.file, a.session)
    # default output stem: <journal stem>, or <db stem>.<session> when reading a DB
    out_stem = (Path(a.file).with_name(f"{Path(a.file).stem}.{J.db_session}") if J.db_session
                else Path(a.file).with_suffix(""))
    t0 = J.t0
    ts = lambda: parse_time(getattr(a, "t", None), t0)

    if a.cmd == "transcript":
        since = parse_time(a.since, t0) if a.since else None
        until = parse_time(a.until, t0) if a.until else None
        lines = []
        for f in a.session_file:
            if len(a.session_file) > 1:
                lines.append(f"===== {Path(f).name}")
            lines += transcript_lines(Path(f), t0, a.grep, since, until, a.width, a.results, a.thinking)
        if a.last:
            lines = lines[-a.last:]
        sys.stdout.reconfigure(encoding="utf-8")
        if t0 is None:
            print("# note: no journal session yet — times are relative to the transcript start")
        print("\n".join(lines))
        return

    if a.cmd == "init":
        if J.records and not a.force:
            raise SystemExit(f"{a.file} already has {len(J.records)} records (use --force)")
        if a.force:
            J.write_all([])
        meta = dict(kv.split("=", 1) for kv in a.meta)
        J.append({"type": "session", "t": parse_time(a.t, None), "id": "s0", "title": a.title, "task": a.task,
                  "goal": a.goal, "meta": meta or None})
    elif a.cmd == "agent":
        J.append({"type": "agent", "t": ts(), "id": J.next_id("a"), "agent": a.agent, "role": a.role,
                  "model": a.model, "parent": a.parent, "label": a.label,
                  "state": "start" if a.start else "stop" if a.stop else None, "status": a.status})
    elif a.cmd in ("event", "ev"):
        J.append({"type": "event", "t": ts(), "id": J.next_id("e"), "agent": a.agent, "kind": a.kind,
                  "title": a.title, "detail": a.detail, "step": a.step, "to": a.to, "tokens": a.tokens,
                  "cost": a.cost, "refs": csv(a.refs), "tags": csv(a.tags)})
    elif a.cmd == "note":
        J.append({"type": "event", "t": ts(), "id": J.next_id("e"), "agent": a.agent, "kind": "note",
                  "title": a.title, "detail": a.detail, "step": a.step})
    elif a.cmd == "dag":
        op = a.op.replace("-", "_")
        rec = {"type": "dag", "t": ts(), "id": J.next_id("d"), "op": op, "node": a.node, "by": a.by,
               "reason": a.reason, "note": a.note}
        if op == "status":
            rec["status"] = a.status
        if op in ("dep_add", "dep_rm"):
            rec["deps"] = csv(a.deps)
        if op in ("add", "update"):
            rec.update({"objective": a.objective, "assignee": a.assignee, "detail": a.detail, "task": a.task})
            if a.deps is not None:
                rec["deps"] = csv(a.deps)
            elif op == "add":
                rec["deps"] = []
        if op != "add" and not any(r.get("type") == "dag" and r.get("node") == a.node for r in J.records):
            print(f"warning: node {a.node!r} was never added; it will be created implicitly", file=sys.stderr)
        rec = {k: v for k, v in rec.items() if v is not None}
        if op == "add":
            rec.setdefault("deps", [])
        J.append(rec)
    elif a.cmd in ("tokens", "usage"):
        J.append({"type": "usage", "t": ts(), "id": J.next_id("u"), "agent": a.agent, "total": a.total,
                  "input": a.input, "output": a.output, "cache_read": a.cache_read, "cache_write": a.cache_write,
                  "cost": a.cost, "cumulative": a.cumulative or None, "step": a.step})
    elif a.cmd == "hyp":
        if a.op == "add":
            hid = a.hid or f"H{sum(1 for r in J.records if r.get('type') == 'hyp' and r.get('op') == 'add') + 1}"
            J.append({"type": "hyp", "t": ts(), "id": J.next_id("y"), "op": "add", "hid": hid, "claim": a.claim,
                      "agent": a.agent, "step": a.step, "status": "open"}, quiet=True)
            print(hid)
        else:
            J.append({"type": "hyp", "t": ts(), "id": J.next_id("y"), "op": "set", "hid": a.hid,
                      "status": a.status, "evidence": a.evidence, "agent": a.agent})
    elif a.cmd in ("incident", "bug"):
        if a.op == "add":
            iid = a.iid or f"I{sum(1 for r in J.records if r.get('type') == 'incident' and r.get('op') == 'add') + 1}"
            J.append({"type": "incident", "t": ts(), "id": J.next_id("i"), "op": "add", "iid": iid,
                      "title": a.title, "severity": a.severity, "category": a.category, "agent": a.agent,
                      "step": a.step, "detail": a.detail, "impact": a.impact, "workaround": a.workaround,
                      "status": "open"}, quiet=True)
            print(iid)
        elif a.op == "update":
            J.append({"type": "incident", "t": ts(), "id": J.next_id("i"), "op": "update", "iid": a.iid,
                      "note": a.note, "severity": a.severity, "status": a.status, "workaround": a.workaround})
        elif a.op == "mitigate":
            J.append({"type": "incident", "t": ts(), "id": J.next_id("i"), "op": "mitigate", "iid": a.iid,
                      "note": a.note, "workaround": a.workaround})
        else:
            J.append({"type": "incident", "t": ts(), "id": J.next_id("i"), "op": "resolve", "iid": a.iid,
                      "note": a.note})
    elif a.cmd == "flag":
        J.append({"type": "flag", "t": ts(), "id": J.next_id("f"), "value": a.value, "status": a.status,
                  "agent": a.agent, "note": a.note, "task": a.task})
    elif a.cmd in ("outcome", "verdict"):
        J.append({"type": "outcome", "t": ts(), "id": J.next_id("o"), "status": a.status, "summary": a.summary,
                  "flag": a.flag, "task": a.task, "by": a.by})
    elif a.cmd == "phase":
        J.append({"type": "phase", "t": ts(), "id": J.next_id("p"), "op": a.action, "name": a.name,
                  "agent": a.agent, "note": a.note})
    elif a.cmd == "end":
        recs = [dict(r) for r in J.records]
        for r in recs:
            if r.get("type") == "session":
                r["ended"] = ts()
        J.write_all(recs)
        print("ok")
    elif a.cmd == "render":
        m = build_model(J.records)
        if a.format == "text":
            since = parse_time(a.since, m["t0"]) if a.since else None
            txt = render_text(m, sections=csv(a.sections), agent_filter=csv(a.agent), since=since,
                              last=a.last, detail=a.detail, kinds=csv(a.kinds))
            if a.out:
                Path(a.out).write_text(txt, encoding="utf-8")
                print(a.out)
            else:
                sys.stdout.reconfigure(encoding="utf-8")
                print(txt)
        elif a.format == "sqlite":
            outp = a.out or str(out_stem) + ".db"
            if J.db_session and Path(outp).resolve() == Path(a.file).resolve() and not a.session_id:
                raise SystemExit("refusing to re-render a DB session into itself; pass -o other.db")
            sid = a.session_id or J.db_session or Path(a.file).stem
            counts = render_sqlite(m, J.records, outp, sid, source_file=str(Path(a.file).resolve()), fresh=a.fresh)
            print(f"{outp}  session_id={sid}  " + " ".join(f"{k}={v}" for k, v in counts.items() if v))
        elif a.format == "jsonl":
            outp = a.out or str(out_stem) + ".jsonl"
            if Path(outp).exists() and Path(outp).resolve() == Path(a.file).resolve():
                raise SystemExit("output equals input")
            with open(outp, "w", encoding="utf-8") as fh:
                for r in J.records:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            print(f"{outp}  ({len(J.records)} records)")
        elif a.format == "json":
            s = json.dumps(m, ensure_ascii=False, indent=1, default=str)
            if a.out:
                Path(a.out).write_text(s, encoding="utf-8")
            else:
                sys.stdout.reconfigure(encoding="utf-8")
                print(s)
        else:
            outp = a.out or str(out_stem) + ".html"
            Path(outp).write_text(render_html(m), encoding="utf-8")
            print(outp)
    elif a.cmd == "import-harness":
        recs = import_harness(Path(a.run_dir), a.bucket)
        if a.merge and J.records:
            manual = [r for r in J.records if not str(r.get("id", "")).startswith("h") and r.get("type") != "session"]
            recs = recs + manual
        J.write_all(recs)
        print(f"{len(recs)} records -> {a.file}")
    elif a.cmd == "validate":
        errors, warns = validate_records(J.records)
        sys.stdout.reconfigure(encoding="utf-8")
        for e in errors:
            print("ERROR", e)
        for w in warns:
            print("WARN ", w)
        if not errors and not warns:
            print("ok")
        if errors or (a.strict and warns):
            sys.exit(1)

if __name__ == "__main__":
    main()
