#!/usr/bin/env python3
import json
import os
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
NODES = [
    {"id": "single", "host": "127.0.0.1", "port": 9006, "config": "configs/kvserver.yaml"},
    {"id": "node-1", "host": "127.0.0.1", "port": 19021, "config": "configs/cluster-node1.yaml"},
    {"id": "node-2", "host": "127.0.0.1", "port": 19022, "config": "configs/cluster-node2.yaml"},
    {"id": "node-3", "host": "127.0.0.1", "port": 19023, "config": "configs/cluster-node3.yaml"},
    {"id": "node-4", "host": "127.0.0.1", "port": 19024, "config": "configs/cluster-node4.yaml"},
    {"id": "node-5", "host": "127.0.0.1", "port": 19025, "config": "configs/cluster-node5.yaml"},
]
PROCESS_LOCK = threading.Lock()
MANAGED_PROCESSES = {}


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TinyKV Dashboard</title>
  <style>
    :root {
      --bg: #f6f7f9;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #667085;
      --line: #d9dee7;
      --blue: #2563eb;
      --green: #0f9f6e;
      --red: #dc2626;
      --amber: #b7791f;
      --code: #111827;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
    }
    header {
      height: 64px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 28px;
      background: #0f172a;
      color: #fff;
      border-bottom: 1px solid #0b1120;
    }
    .brand { display: flex; align-items: center; gap: 12px; font-weight: 700; font-size: 18px; }
    .logo {
      width: 34px;
      height: 34px;
      border-radius: 8px;
      display: grid;
      place-items: center;
      background: #2563eb;
      color: #fff;
      font-weight: 800;
    }
    .status-pill {
      border: 1px solid rgba(255,255,255,.22);
      color: #dbeafe;
      padding: 7px 10px;
      border-radius: 999px;
      font-size: 12px;
    }
    main {
      max-width: 1240px;
      margin: 0 auto;
      padding: 24px;
    }
    .grid {
      display: grid;
      grid-template-columns: 1.05fr .95fr;
      gap: 18px;
      align-items: start;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    .panel-head {
      min-height: 52px;
      padding: 14px 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid var(--line);
    }
    .panel-title {
      display: flex;
      align-items: center;
      gap: 9px;
      font-weight: 700;
    }
    .icon {
      width: 18px;
      height: 18px;
      display: inline-block;
      color: currentColor;
    }
    .panel-body { padding: 16px; }
    .node-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .node {
      min-height: 190px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      background: #fbfcfe;
    }
    .node-top { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
    .node-name { font-weight: 700; }
    .dot { width: 10px; height: 10px; border-radius: 50%; background: #98a2b3; flex: 0 0 auto; }
    .dot.up { background: var(--green); }
    .dot.down { background: var(--red); }
    .node-port { color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    .node-latency { color: var(--muted); font-size: 12px; }
    .role {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 7px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      background: #eef2ff;
      color: #3730a3;
    }
    .role.leader { background: #dcfce7; color: #166534; }
    .role.candidate { background: #fef3c7; color: #92400e; }
    .role.follower { background: #e0f2fe; color: #075985; }
    .node-metrics {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px;
      margin-top: 10px;
    }
    .node-metric {
      min-width: 0;
      border: 1px solid #e5e7eb;
      border-radius: 6px;
      padding: 6px 7px;
      background: #fff;
    }
    .node-metric span {
      display: block;
      color: var(--muted);
      font-size: 11px;
      line-height: 1.2;
    }
    .node-metric strong {
      display: block;
      margin-top: 2px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-size: 12px;
    }
    .node-actions { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-top: 8px; }
    .node-actions button { min-height: 30px; font-size: 12px; }
    .node-actions .start { color: #fff; background: var(--green); border-color: var(--green); }
    .node-actions .stop { color: #fff; background: var(--red); border-color: var(--red); }
    .controls {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 12px; }
    input, select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      border-radius: 6px;
      padding: 9px 10px;
      font: inherit;
      outline: none;
    }
    textarea {
      min-height: 92px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }
    input:focus, select:focus, textarea:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(37,99,235,.12); }
    .wide { grid-column: span 2; }
    .actions {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 8px;
      margin: 10px 0 14px;
    }
    button {
      min-height: 38px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      border-radius: 6px;
      font-weight: 650;
      cursor: pointer;
    }
    button.primary { background: var(--blue); color: #fff; border-color: var(--blue); }
    button:hover { filter: brightness(.98); }
    .terminal {
      background: var(--code);
      color: #d1fae5;
      border-radius: 8px;
      padding: 12px;
      min-height: 210px;
      overflow: auto;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      white-space: pre-wrap;
      line-height: 1.45;
    }
    .metric-row {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 12px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: #fbfcfe;
    }
    .metric-value { font-size: 20px; font-weight: 750; margin-top: 4px; }
    .metric-label { color: var(--muted); font-size: 12px; }
    .feature-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 18px;
    }
    .feature {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      padding: 14px;
      min-height: 108px;
    }
    .feature strong { display: block; margin: 8px 0 5px; }
    .feature span { color: var(--muted); line-height: 1.45; }
    @media (max-width: 900px) {
      header { padding: 0 16px; }
      main { padding: 16px; }
      .grid, .feature-grid { grid-template-columns: 1fr; }
      .node-grid, .controls, .actions, .metric-row { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .wide { grid-column: span 2; }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand"><div class="logo">KV</div><div>TinyKV Dashboard</div></div>
    <div class="status-pill" id="summary">checking</div>
  </header>

  <main>
    <section class="grid">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title">
            <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="6" rx="2"/><rect x="3" y="14" width="18" height="6" rx="2"/><path d="M7 8h.01M7 18h.01"/></svg>
            节点状态
          </div>
          <button id="refreshNodes">刷新</button>
          <button id="startRaft">启动 node-1~3</button>
        </div>
        <div class="panel-body">
          <div class="node-grid" id="nodes"></div>
        </div>
      </div>

      <div class="panel">
        <div class="panel-head">
          <div class="panel-title">
            <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 17l6-6 4 4 6-8"/><path d="M14 7h6v6"/></svg>
            压测
          </div>
          <button class="primary" id="runBench">运行</button>
        </div>
        <div class="panel-body">
          <div class="controls">
            <label class="wide">端口<select id="benchPort"></select></label>
            <label class="wide">请求数<input id="benchRequests" type="number" value="1000" min="1"></label>
            <label class="wide">并发数<input id="benchClients" type="number" value="20" min="1"></label>
          </div>
          <div class="metric-row">
            <div class="metric"><div class="metric-label">QPS</div><div class="metric-value" id="qps">0</div></div>
            <div class="metric"><div class="metric-label">AVG us</div><div class="metric-value" id="avg">0</div></div>
            <div class="metric"><div class="metric-label">P95 us</div><div class="metric-value" id="p95">0</div></div>
            <div class="metric"><div class="metric-label">失败</div><div class="metric-value" id="failed">0</div></div>
          </div>
        </div>
      </div>
    </section>

    <section class="panel" style="margin-top:18px">
      <div class="panel-head">
        <div class="panel-title">
          <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 17l6-6-6-6"/><path d="M12 19h8"/></svg>
          KV 操作
        </div>
        <button class="primary" id="sendRaw">发送</button>
      </div>
      <div class="panel-body">
        <div class="controls">
          <label>端口<select id="port"></select></label>
          <label class="wide">Key<input id="key" value="user:1"></label>
          <label class="wide">Value<input id="value" value="alice"></label>
          <label>TTL 秒<input id="ttl" type="number" value="60" min="1"></label>
        </div>
        <div class="actions">
          <button data-op="PING">PING</button>
          <button data-op="SET">SET</button>
          <button data-op="GET">GET</button>
          <button data-op="EXISTS">EXISTS</button>
          <button data-op="EXPIRE">EXPIRE</button>
          <button data-op="TTL">TTL</button>
          <button data-op="DEL">DEL</button>
        </div>
        <textarea id="raw">SET user:1 alice</textarea>
        <div class="terminal" id="terminal"></div>
      </div>
    </section>

    <section class="feature-grid">
      <div class="feature">
        <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="#2563eb" stroke-width="2"><path d="M13 2L3 14h8l-1 8 10-12h-8l1-8z"/></svg>
        <strong>epoll + 线程池</strong>
        <span>非阻塞网络事件循环，worker 处理完整命令帧。</span>
      </div>
      <div class="feature">
        <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="#0f9f6e" stroke-width="2"><path d="M12 3v18"/><path d="M5 8h14"/><path d="M5 16h14"/></svg>
        <strong>WAL + Snapshot</strong>
        <span>写前日志、快照压缩和重启恢复。</span>
      </div>
      <div class="feature">
        <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="#b7791f" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 3"/></svg>
        <strong>TTL + LRU</strong>
        <span>过期键和容量上限淘汰策略。</span>
      </div>
      <div class="feature">
        <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="#7c3aed" stroke-width="2"><path d="M4 7h16M4 12h16M4 17h16"/></svg>
        <strong>RESP</strong>
        <span>支持 redis-cli 的常用命令格式。</span>
      </div>
      <div class="feature">
        <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="#dc2626" stroke-width="2"><circle cx="6" cy="12" r="3"/><circle cx="18" cy="6" r="3"/><circle cx="18" cy="18" r="3"/><path d="M8.7 10.7l6.6-3.4M8.7 13.3l6.6 3.4"/></svg>
        <strong>五节点路由</strong>
        <span>一致性哈希选择 key 的 owner 节点。</span>
      </div>
      <div class="feature">
        <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="#0891b2" stroke-width="2"><path d="M3 3v18h18"/><path d="M7 15l3-3 3 2 4-6"/></svg>
        <strong>Benchmark</strong>
        <span>展示 QPS、平均延迟和尾延迟。</span>
      </div>
    </section>
  </main>

  <script>
    const nodeDefs = [
      {id:'single', port:9006}, {id:'node-1', port:19021}, {id:'node-2', port:19022},
      {id:'node-3', port:19023}, {id:'node-4', port:19024}, {id:'node-5', port:19025}
    ];
    const terminal = document.getElementById('terminal');
    const portSelect = document.getElementById('port');
    const benchPort = document.getElementById('benchPort');

    for (const node of nodeDefs) {
      for (const select of [portSelect, benchPort]) {
        const option = document.createElement('option');
        option.value = node.port;
        option.textContent = `${node.id} : ${node.port}`;
        select.appendChild(option);
      }
    }

    function log(line) {
      const now = new Date().toLocaleTimeString();
      terminal.textContent = `[${now}] ${line}\n` + terminal.textContent;
    }

    async function api(path, body) {
      const res = await fetch(path, {
        method: body ? 'POST' : 'GET',
        headers: body ? {'Content-Type': 'application/json'} : {},
        body: body ? JSON.stringify(body) : undefined
      });
      return await res.json();
    }

    async function refreshNodes() {
      const data = await api('/api/nodes');
      const nodes = document.getElementById('nodes');
      nodes.innerHTML = '';
      let up = 0;
      for (const node of data.nodes) {
        if (node.up) up++;
        const m = node.metrics || {};
        const role = m.tinykv_raft_role || (node.up ? 'single' : 'offline');
        const roleClass = ['leader', 'candidate', 'follower'].includes(role) ? role : '';
        const leader = m.tinykv_raft_leader || '-';
        const term = m.tinykv_raft_term ?? '-';
        const commit = m.tinykv_raft_commit_index ?? '-';
        const applied = m.tinykv_raft_last_applied ?? '-';
        const logSize = m.tinykv_raft_log_size ?? '-';
        const snapIndex = m.tinykv_raft_snapshot_last_included_index ?? '-';
        const qps = m.tinykv_qps ?? '-';
        const p95 = m.tinykv_latency_p95_us ?? '-';
        const p99 = m.tinykv_latency_p99_us ?? '-';
        const alive = m.tinykv_alive_nodes ?? '-';
        const elections = m.tinykv_raft_election_count ?? '-';
        const replFail = m.tinykv_raft_replication_failure_count ?? '-';
        const el = document.createElement('div');
        el.className = 'node';
        el.innerHTML = `
          <div class="node-top"><div class="node-name">${node.id}</div><div class="dot ${node.up ? 'up' : 'down'}"></div></div>
          <div class="node-port">${node.host}:${node.port}</div>
          <div class="node-latency">${node.up ? node.latency_ms + ' ms' : 'offline'}${node.managed ? ' / managed' : ''}</div>
          <div class="role ${roleClass}">${role}</div>
          <div class="node-metrics">
            <div class="node-metric"><span>term / leader</span><strong>${term} / ${leader}</strong></div>
            <div class="node-metric"><span>commit / apply</span><strong>${commit} / ${applied}</strong></div>
            <div class="node-metric"><span>log / snap</span><strong>${logSize} / ${snapIndex}</strong></div>
            <div class="node-metric"><span>QPS</span><strong>${qps}</strong></div>
            <div class="node-metric"><span>p95 / p99 us</span><strong>${p95} / ${p99}</strong></div>
            <div class="node-metric"><span>alive / fail</span><strong>${alive} / ${replFail}</strong></div>
          </div>
          <div class="node-actions">
            <button class="start" data-node-start="${node.id}">启动</button>
            <button class="stop" data-node-stop="${node.id}">停止</button>
          </div>`;
        nodes.appendChild(el);
      }
      document.querySelectorAll('[data-node-start]').forEach(btn => {
        btn.onclick = () => controlNode(btn.dataset.nodeStart, 'start');
      });
      document.querySelectorAll('[data-node-stop]').forEach(btn => {
        btn.onclick = () => controlNode(btn.dataset.nodeStop, 'stop');
      });
      document.getElementById('summary').textContent = `${up}/${data.nodes.length} online`;
    }

    async function controlNode(id, action) {
      const data = await api(`/api/node/${action}`, {id});
      log(`${action} ${id} -> ${data.ok ? 'OK' : data.error}`);
      await new Promise(resolve => setTimeout(resolve, 450));
      await refreshNodes();
    }

    async function send(command) {
      const port = Number(portSelect.value);
      const data = await api('/api/command', {port, command});
      if (data.ok) log(`:${port} ${command} -> ${data.response.trim()}`);
      else log(`:${port} ${command} -> ${data.error}`);
      await refreshNodes();
    }

    document.getElementById('refreshNodes').onclick = refreshNodes;
    document.getElementById('startRaft').onclick = async () => {
      for (const id of ['node-1', 'node-2', 'node-3']) {
        await controlNode(id, 'start');
      }
    };
    document.getElementById('sendRaw').onclick = () => send(document.getElementById('raw').value);
    document.querySelectorAll('[data-op]').forEach(btn => {
      btn.onclick = () => {
        const op = btn.dataset.op;
        const key = document.getElementById('key').value.trim();
        const value = document.getElementById('value').value;
        const ttl = document.getElementById('ttl').value;
        let cmd = op;
        if (op === 'SET') cmd = `SET ${key} ${value}`;
        if (op === 'GET') cmd = `GET ${key}`;
        if (op === 'EXISTS') cmd = `EXISTS ${key}`;
        if (op === 'TTL') cmd = `TTL ${key}`;
        if (op === 'DEL') cmd = `DEL ${key}`;
        if (op === 'EXPIRE') cmd = `EXPIRE ${key} ${ttl}`;
        if (op === 'SET' && ttl) cmd = `SET ${key} ${value} EX ${ttl}`;
        document.getElementById('raw').value = cmd;
        send(cmd);
      };
    });

    document.getElementById('runBench').onclick = async () => {
      const data = await api('/api/bench', {
        port: Number(benchPort.value),
        requests: Number(document.getElementById('benchRequests').value),
        clients: Number(document.getElementById('benchClients').value)
      });
      if (!data.ok) { log(`bench -> ${data.error}`); return; }
      document.getElementById('qps').textContent = data.qps;
      document.getElementById('avg').textContent = data.avg_latency_us;
      document.getElementById('p95').textContent = data.p95_latency_us;
      document.getElementById('failed').textContent = data.failed;
      log(`bench -> qps=${data.qps}, p95=${data.p95_latency_us}us`);
    };

    refreshNodes();
    setInterval(refreshNodes, 5000);
  </script>
</body>
</html>
"""


def send_command(port, command, host="127.0.0.1", timeout=1.5):
    payload = command.rstrip("\r\n") + "\n"
    with socket.create_connection((host, int(port)), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(payload.encode())
        sock.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                break
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks).decode(errors="replace")


def find_node(node_id):
    for node in NODES:
        if node["id"] == node_id:
            return node
    return None


def node_is_up(node):
    try:
        return send_command(node["port"], "PING", timeout=0.35).strip() == "PONG"
    except OSError:
        return False


def parse_metrics(text):
    metrics = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        key, value = parts
        if value.isdigit():
            metrics[key] = int(value)
            continue
        try:
            metrics[key] = float(value)
        except ValueError:
            metrics[key] = value
    return metrics


def node_metrics(node):
    try:
        return parse_metrics(send_command(node["port"], "METRICS", timeout=0.8))
    except OSError:
        return {}


def managed_status(node_id):
    with PROCESS_LOCK:
        proc = MANAGED_PROCESSES.get(node_id)
        if proc is None:
            return False
        if proc.poll() is not None:
            MANAGED_PROCESSES.pop(node_id, None)
            return False
        return True


def start_node(node_id):
    node = find_node(node_id)
    if node is None:
        return {"ok": False, "error": "unknown node"}
    if node_is_up(node):
        return {"ok": True, "status": "already_running"}

    config = ROOT / node["config"]
    if not config.exists():
        return {"ok": False, "error": f"missing config: {node['config']}"}
    binary = ROOT / "kvserver"
    if not binary.exists():
        return {"ok": False, "error": "missing ./kvserver, run make kvserver first"}

    log_dir = ROOT / "data" / "dashboard"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = open(log_dir / f"{node_id}.log", "ab", buffering=0)
    proc = subprocess.Popen(
        [str(binary), "-f", str(config)],
        cwd=str(ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    with PROCESS_LOCK:
        MANAGED_PROCESSES[node_id] = proc

    time.sleep(0.35)
    if node_is_up(node):
        return {"ok": True, "status": "started", "pid": proc.pid}
    if proc.poll() is not None:
        with PROCESS_LOCK:
            MANAGED_PROCESSES.pop(node_id, None)
        return {"ok": False, "error": f"process exited, see data/dashboard/{node_id}.log"}
    return {"ok": True, "status": "starting", "pid": proc.pid}


def listening_inodes_for_port(port):
    target = f"{int(port):04X}"
    inodes = set()
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(table, "r", encoding="utf-8") as handle:
                next(handle, None)
                for line in handle:
                    parts = line.split()
                    if len(parts) < 10:
                        continue
                    local_address = parts[1]
                    state = parts[3]
                    inode = parts[9]
                    if state != "0A":
                        continue
                    _, local_port = local_address.rsplit(":", 1)
                    if local_port.upper() == target:
                        inodes.add(inode)
        except OSError:
            continue
    return inodes


def cmdline_for_pid(pid):
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return ""
    return raw.replace(b"\x00", b" ").decode(errors="replace").strip()


def find_kvserver_pid_by_port(port):
    inodes = listening_inodes_for_port(port)
    if not inodes:
        return None

    proc_root = Path("/proc")
    for pid_dir in proc_root.iterdir():
        if not pid_dir.name.isdigit():
            continue
        fd_dir = pid_dir / "fd"
        try:
            for fd in fd_dir.iterdir():
                try:
                    target = os.readlink(fd)
                except OSError:
                    continue
                if not target.startswith("socket:[") or not target.endswith("]"):
                    continue
                inode = target[len("socket:["):-1]
                if inode not in inodes:
                    continue
                cmdline = cmdline_for_pid(pid_dir.name)
                if "kvserver" in cmdline:
                    return int(pid_dir.name)
        except OSError:
            continue
    return None


def wait_until_down(node, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not node_is_up(node):
            return True
        time.sleep(0.1)
    return not node_is_up(node)


def stop_node(node_id):
    node = find_node(node_id)
    if node is None:
        return {"ok": False, "error": "unknown node"}

    with PROCESS_LOCK:
        proc = MANAGED_PROCESSES.pop(node_id, None)
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
        return {"ok": True, "status": "stopped"}

    pid = find_kvserver_pid_by_port(node["port"])
    if pid is None:
        if node_is_up(node):
            return {"ok": False, "error": "node is running but pid was not found"}
        return {"ok": True, "status": "already_stopped"}

    try:
        os.kill(pid, 15)
    except ProcessLookupError:
        return {"ok": True, "status": "already_stopped"}
    except PermissionError:
        return {"ok": False, "error": f"permission denied stopping pid {pid}"}

    if not wait_until_down(node):
        try:
            os.kill(pid, 9)
        except OSError:
            pass
    return {"ok": True, "status": "stopped", "pid": pid}


def bench(port, requests, clients):
    requests = max(1, int(requests))
    clients = max(1, int(clients))
    next_index = 0
    lock = threading.Lock()
    latencies = []
    ok = 0
    failed = 0
    started = time.perf_counter()

    def worker():
        nonlocal next_index, ok, failed
        while True:
            with lock:
                if next_index >= requests:
                    return
                index = next_index
                next_index += 1
            key = f"dash:{index}"
            command = f"SET {key} value:{index}" if index % 2 == 0 else f"GET dash:{index - 1}"
            begin = time.perf_counter()
            try:
                response = send_command(port, command, timeout=2.0)
                success = bool(response) and not response.startswith("ERROR")
            except OSError:
                success = False
            elapsed_us = int((time.perf_counter() - begin) * 1_000_000)
            with lock:
                latencies.append(elapsed_us)
                if success:
                    ok += 1
                else:
                    failed += 1

    threads = [threading.Thread(target=worker) for _ in range(clients)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    elapsed = max(time.perf_counter() - started, 0.000001)
    latencies.sort()

    def percentile(p):
        if not latencies:
            return 0
        return latencies[int((len(latencies) - 1) * p)]

    return {
        "ok": True,
        "requests": requests,
        "clients": clients,
        "success": ok,
        "failed": failed,
        "qps": int(ok / elapsed),
        "avg_latency_us": int(sum(latencies) / len(latencies)) if latencies else 0,
        "p95_latency_us": percentile(0.95),
        "p99_latency_us": percentile(0.99),
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def json_response(self, data, status=200):
        raw = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            raw = INDEX_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return
        if path == "/api/nodes":
            nodes = []
            for node in NODES:
                begin = time.perf_counter()
                try:
                    response = send_command(node["port"], "PING", timeout=0.35)
                    up = response.strip() == "PONG"
                    metrics = node_metrics(node) if up else {}
                    error = ""
                except OSError as exc:
                    up = False
                    metrics = {}
                    error = str(exc)
                nodes.append({
                    **node,
                    "up": up,
                    "metrics": metrics,
                    "managed": managed_status(node["id"]),
                    "latency_ms": round((time.perf_counter() - begin) * 1000, 2),
                    "error": error,
                })
            self.json_response({"nodes": nodes})
            return
        self.send_error(404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        path = urlparse(self.path).path
        if path == "/api/command":
            try:
                response = send_command(body.get("port", 9006), body.get("command", "PING"))
                self.json_response({"ok": True, "response": response})
            except OSError as exc:
                self.json_response({"ok": False, "error": str(exc)}, 502)
            return
        if path == "/api/bench":
            try:
                self.json_response(bench(body.get("port", 9006), body.get("requests", 1000), body.get("clients", 20)))
            except OSError as exc:
                self.json_response({"ok": False, "error": str(exc)}, 502)
            return
        if path == "/api/node/start":
            self.json_response(start_node(body.get("id", "")))
            return
        if path == "/api/node/stop":
            self.json_response(stop_node(body.get("id", "")))
            return
        self.send_error(404)


def main():
    host = os.environ.get("TINYKV_DASHBOARD_HOST", "127.0.0.1")
    port = int(os.environ.get("TINYKV_DASHBOARD_PORT", "8080"))
    autostart = os.environ.get("TINYKV_DASHBOARD_AUTOSTART", "")
    for node_id in [item.strip() for item in autostart.split(",") if item.strip()]:
        result = start_node(node_id)
        print(f"autostart {node_id}: {result}")
    server = ThreadingHTTPServer((host, port), Handler)
    display_host = "127.0.0.1" if host == "0.0.0.0" else host
    print(f"TinyKV Dashboard: http://{display_host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
