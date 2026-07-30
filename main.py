#!/usr/bin/env python3
"""
BT_test — 天轶2.0 行为树编排独立项目。

功能:
  - JSON-RPC MCP server (端口 15800)
  - 接收 JSON 行为树定义
  - 使用 py_trees 驱动执行
  - action 节点通过 HTTP 调用天轶驱动 MCP (15799)
  - 逐 tick 返回树状态快照

用法:
    python3 main.py

环境变量:
    CONFIG_PATH — config.yaml 路径
    DRIVER_MCP_URL — 天轶驱动 MCP URL (默认 http://localhost:15799/mcp)
"""

import json
import os
import signal
import sys
import threading
import time
import urllib.request as _urllib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml
import py_trees


# ── Config ────────────────────────────────────────────────────────────────────

def _load_config() -> dict:
    config_path = os.environ.get("CONFIG_PATH", str(Path(__file__).parent / "config.yaml"))
    with open(config_path) as f:
        return yaml.safe_load(f)


cfg = _load_config()
DRIVER_MCP_URL = os.environ.get("DRIVER_MCP_URL", cfg.get("driver_mcp_url", "http://localhost:15799/mcp"))
MCP_PORT = int(cfg.get("mcp_port", 15800))


# ══════════════════════════════════════════════════════════════════════════════
# Custom py_trees nodes
# ══════════════════════════════════════════════════════════════════════════════

class _BTAction(py_trees.behaviour.Behaviour):
    """Action 节点: 通过 MCP 调用天轶驱动卡片。"""

    def __init__(self, name: str, plugin: str, params: dict):
        super().__init__(name)
        self.plugin = plugin
        self.params = params
        self._result = None

    def initialise(self):
        self._result = None

    def _call_driver(self, tool_name: str, arguments: dict) -> dict | None:
        """调用天轶驱动 MCP 执行卡片动作。"""
        payload = json.dumps({
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }).encode()

        try:
            req = _urllib.Request(
                DRIVER_MCP_URL,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with _urllib.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                if "result" in data and "content" in data["result"]:
                    text = data["result"]["content"][0].get("text", "{}")
                    return json.loads(text)
                return data.get("result")
        except Exception as e:
            self._result = {"error": str(e)}
            return None

    def update(self):
        try:
            self._result = self._call_driver(self.plugin, self.params.copy())
            if self._result is not None and "error" not in str(self._result).lower():
                return py_trees.common.Status.SUCCESS
            return py_trees.common.Status.FAILURE
        except Exception as e:
            self._result = {"error": str(e)}
            return py_trees.common.Status.FAILURE

    def terminate(self, new_status):
        pass


class _BTCondition(py_trees.behaviour.Behaviour):
    """Condition 节点: 调用卡片检查期望值。"""

    def __init__(self, name: str, plugin: str, params: dict, expected: dict):
        super().__init__(name)
        self.plugin = plugin
        self.params = params
        self.expected = expected
        self._result = None

    def initialise(self):
        self._result = None

    def _call_driver(self, tool_name: str, arguments: dict) -> dict | None:
        payload = json.dumps({
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }).encode()
        try:
            req = _urllib.Request(
                DRIVER_MCP_URL,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with _urllib.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                if "result" in data and "content" in data["result"]:
                    text = data["result"]["content"][0].get("text", "{}")
                    return json.loads(text)
                return data.get("result")
        except Exception as e:
            self._result = {"error": str(e)}
            return None

    def update(self):
        try:
            result = self._call_driver(self.plugin, self.params.copy())
            if result is None:
                return py_trees.common.Status.FAILURE
            self._result = result
            for key, val in self.expected.items():
                actual = result
                for part in key.split("."):
                    if isinstance(actual, dict):
                        actual = actual.get(part)
                    else:
                        actual = None
                        break
                if actual != val:
                    return py_trees.common.Status.FAILURE
            return py_trees.common.Status.SUCCESS
        except Exception as e:
            self._result = {"error": str(e)}
            return py_trees.common.Status.FAILURE

    def terminate(self, new_status):
        pass


class _BTWait(py_trees.behaviour.Behaviour):
    """Wait 节点: 等待指定秒数。"""

    def __init__(self, name: str, duration: float):
        super().__init__(name)
        self.duration = duration
        self._start = 0.0

    def initialise(self):
        self._start = time.time()

    def update(self):
        if time.time() - self._start >= self.duration:
            return py_trees.common.Status.SUCCESS
        return py_trees.common.Status.RUNNING

    def terminate(self, new_status):
        pass


# ══════════════════════════════════════════════════════════════════════════════
# BT Orchestrator
# ══════════════════════════════════════════════════════════════════════════════

class BTOrchestrator:
    """行为树编排器。"""

    def __init__(self):
        self._tree = None
        self._root = None
        self._tree_def = None
        self._running = False
        self._tick_hz = 10.0
        self._tick_thread = None
        self._lock = threading.Lock()

    # ── Tree building ─────────────────────────────────────────────────────

    @classmethod
    def _build_node(cls, node_def: dict) -> py_trees.behaviour.Behaviour:
        """从 JSON 节点定义递归构建 py_trees 节点。"""
        node_type = node_def.get("type", "action")
        name = node_def.get("name", node_type)
        children_def = node_def.get("children", [])

        if node_type == "sequence":
            node = py_trees.composites.Sequence(name=name, memory=True)
            for child_def in children_def:
                node.add_child(cls._build_node(child_def))
            return node

        elif node_type == "fallback":
            node = py_trees.composites.Selector(name=name, memory=True)
            for child_def in children_def:
                node.add_child(cls._build_node(child_def))
            return node

        elif node_type == "parallel":
            policy = node_def.get("policy", "all")
            p_map = {
                "all":      py_trees.common.ParallelPolicy.SuccessOnAll,
                "one":      py_trees.common.ParallelPolicy.SuccessOnOne,
                "selected": py_trees.common.ParallelPolicy.SuccessOnSelected,
            }
            node = py_trees.composites.Parallel(
                name=name, policy=p_map.get(policy, py_trees.common.ParallelPolicy.SuccessOnAll))
            for child_def in children_def:
                node.add_child(cls._build_node(child_def))
            return node

        elif node_type == "action":
            plugin = node_def.get("plugin", "")
            params = node_def.get("params", {})
            return _BTAction(name=name, plugin=plugin, params=params)

        elif node_type == "condition":
            plugin = node_def.get("plugin", "")
            params = node_def.get("params", {})
            expected = node_def.get("expected", {"healthy": True})
            return _BTCondition(name=name, plugin=plugin, params=params, expected=expected)

        elif node_type == "wait":
            duration = float(node_def.get("duration", 1.0))
            return _BTWait(name=name, duration=duration)

        elif node_type == "repeat":
            count = int(node_def.get("count", -1))
            child = cls._build_node(children_def[0]) if children_def else None
            if child is None:
                raise ValueError("repeat node must have at least one child")
            return py_trees.decorators.Repeat(name=name, child=child, num_repeats=count)

        else:
            raise ValueError(f"unknown node type: {node_type}")

    @staticmethod
    def _count_nodes(root) -> int:
        count = 1
        if hasattr(root, 'children'):
            for child in root.children:
                count += BTOrchestrator._count_nodes(child)
        return count

    def load_tree(self, tree_def: dict) -> str:
        name = tree_def.get("name", "unnamed")
        root_def = tree_def.get("root")
        if not root_def:
            return "错误: tree.root 字段缺失"

        if "tick_hz" in tree_def:
            self._tick_hz = float(tree_def["tick_hz"])

        try:
            self._root = self._build_node(root_def)
        except Exception as e:
            self._root = None
            import traceback
            traceback.print_exc()
            return f"构建失败: {e}"

        self._tree = py_trees.trees.BehaviourTree(self._root)
        self._tree_def = tree_def
        return f"已加载行为树 '{name}' ({self._count_nodes(self._root)} 个节点)"

    # ── Tick loop ──────────────────────────────────────────────────────────

    def _tick_loop(self):
        period = 1.0 / max(self._tick_hz, 1)
        while self._running:
            try:
                with self._lock:
                    if self._tree and self._root:
                        self._tree.tick()
                if self._root and self._root.status != py_trees.common.Status.RUNNING:
                    break
            except Exception as e:
                print(f"[BT] tick error: {e}", flush=True)
            time.sleep(period)
        self._running = False
        print(f"[BT] tree finished, status={self._root.status if self._root else '?'}")

    def run(self) -> dict:
        if not self._tree or not self._root:
            return {"error": "请先 load 行为树定义"}
        if self._running:
            return {"error": "行为树已在运行中, 请先 stop"}

        self._running = True
        self._tick_thread = threading.Thread(target=self._tick_loop, daemon=True)
        self._tick_thread.start()

        return {
            "state": "running",
            "tree": self._tree_def.get("name", "unnamed") if self._tree_def else "unnamed",
            "tick_hz": self._tick_hz,
        }

    def stop(self) -> dict:
        was_running = self._running
        self._running = False
        if self._tree:
            try:
                self._tree.interrupt()
            except Exception:
                pass
        if self._tick_thread:
            self._tick_thread.join(timeout=1.0)
        return {"state": "stopped", "was_running": was_running}

    def status(self) -> dict:
        if not self._tree or not self._root:
            return {"state": "idle", "loaded": False}

        with self._lock:
            snapshot = self._build_snapshot(self._root)

        return {
            "state": "running" if self._running else "idle",
            "loaded": True,
            "tree": self._tree_def.get("name", "unnamed") if self._tree_def else "unnamed",
            "snapshot": snapshot,
        }

    def _build_snapshot(self, root) -> dict:
        def _snap(node, depth=0):
            status_map = {
                py_trees.common.Status.SUCCESS:  "success",
                py_trees.common.Status.FAILURE:  "failure",
                py_trees.common.Status.RUNNING:  "running",
                py_trees.common.Status.INVALID:  "invalid",
            }
            info = {
                "name": getattr(node, 'name', str(node)),
                "type": type(node).__name__,
                "status": status_map.get(node.status, "unknown"),
                "depth": depth,
            }
            if hasattr(node, 'children'):
                info["children"] = [_snap(c, depth + 1) for c in node.children]
            if hasattr(node, '_result') and node._result:
                info["result"] = node._result
            return info

        overall = "running"
        if root.status == py_trees.common.Status.SUCCESS:
            overall = "success"
        elif root.status == py_trees.common.Status.FAILURE:
            overall = "failure"

        return {
            "tree_name": self._tree_def.get("name", "unnamed") if self._tree_def else "unnamed",
            "overall": overall,
            "node_count": self._count_nodes(root),
            "tick_hz": self._tick_hz,
            "root": _snap(root),
        }


# ══════════════════════════════════════════════════════════════════════════════
# MCP HTTP Server
# ══════════════════════════════════════════════════════════════════════════════

_orchestrator = BTOrchestrator()


def _tool_def() -> dict:
    return {
        "name": "behavior_tree",
        "type": "actuator",
        "description": (
            "天轶2.0 行为树编排 — JSON 定义任务序列, py_trees 驱动执行。\n"
            "支持的节点类型: sequence, fallback, parallel, action, condition, wait, repeat。\n"
            "action/condition 节点通过 HTTP 调用天轶驱动 MCP (主驱动) 执行实际动作。\n\n"
            "JSON 定义示例:\n"
            '{\n  "name": "巡逻",\n  "tick_hz": 10,\n  "root": {\n'
            '    "type": "sequence",\n    "name": "巡逻序列",\n    "children": [\n'
            '      {"type": "action", "name": "前进3秒", "plugin": "chassis_raw",\n'
            '       "params": {"action": "move", "direction": "forward", "duration": 3}},\n'
            '      {"type": "wait",  "name": "等2秒", "duration": 2},\n'
            '      {"type": "action", "name": "说话", "plugin": "tts",\n'
            '       "params": {"action": "speak", "text": "巡逻完成"}}\n'
            '    ]\n  }\n}'
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["load", "run", "stop", "status"],
                    "description": "控制动作",
                },
                "tree": {
                    "type": "object",
                    "description": "行为树 JSON 定义 (load 时必填)",
                },
            },
            "required": ["action"],
            "x-action-params": {
                "load":   {"params": ["tree"], "description": "加载行为树 JSON 定义"},
                "run":    {"params": [], "description": "开始执行已加载的行为树"},
                "stop":   {"params": [], "description": "停止执行"},
                "status": {"params": [], "description": "查看当前状态和树快照"},
            },
        },
    }


def make_handler():
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            msg = fmt % args
            if '"POST /mcp' in msg and '200' in msg:
                return
            print(f"[mcp] {self.address_string()} {msg}")

        def _send(self, status: int, body: str):
            encoded = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept")
            self.end_headers()
            self.wfile.write(encoded)

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept")
            self.end_headers()

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            try:
                rpc = json.loads(raw)
            except Exception:
                self._send(400, json.dumps({"jsonrpc": "2.0", "id": None,
                                             "error": {"code": -32700, "message": "Parse error"}}))
                return

            rid    = rpc.get("id")
            method = rpc.get("method", "")
            params = rpc.get("params") or {}

            if rid is None:
                self.send_response(202)
                self.end_headers()
                return

            def ok(result):
                self._send(200, json.dumps({"jsonrpc": "2.0", "id": rid, "result": result}))

            def err(code, msg):
                self._send(200, json.dumps({"jsonrpc": "2.0", "id": rid,
                                             "error": {"code": code, "message": msg}}))

            try:
                if method == "initialize":
                    ok({
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "tianyi2-bt-orchestrator", "version": "1.0.0"},
                    })
                elif method == "tools/list":
                    ok({"tools": [_tool_def()]})
                elif method == "tools/call":
                    name = params.get("name", "")
                    args = params.get("arguments") or {}

                    if name != "behavior_tree":
                        err(-32601, f"Unknown tool: {name}")
                        return

                    action = args.get("action", "status")
                    if action == "load":
                        tree_def = args.get("tree", {})
                        if not tree_def:
                            err(-32602, "tree 参数为必填")
                            return
                        # Stop running tree first
                        if _orchestrator._running:
                            _orchestrator.stop()
                        msg = _orchestrator.load_tree(tree_def)
                        result = {"state": "loaded", "message": msg}
                    elif action == "run":
                        result = _orchestrator.run()
                    elif action == "stop":
                        result = _orchestrator.stop()
                    elif action == "status":
                        result = _orchestrator.status()
                    else:
                        result = {"error": f"unknown action: {action}"}

                    ok({"content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}]})
                else:
                    err(-32601, f"Method not found: {method}")
            except Exception as e:
                import traceback
                traceback.print_exc()
                err(-32603, str(e))

    return Handler


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"[BT_test] driver MCP → {DRIVER_MCP_URL}")
    print(f"[BT_test] MCP server → http://localhost:{MCP_PORT}")

    server = ThreadingHTTPServer(("", MCP_PORT), make_handler())

    def _shutdown(signum, frame):
        print(f"[BT_test] signal {signum}, shutting down")
        _orchestrator.stop()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        server.serve_forever()
    finally:
        _orchestrator.stop()


if __name__ == "__main__":
    main()
