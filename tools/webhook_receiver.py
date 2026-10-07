#!/usr/bin/env python3
"""Minimal webhook receiver for local testing.

Appends each POST body (one JSON line) to the output file and prints it.
Usage: python3 tools/webhook_receiver.py [--port 8787] [--out received.jsonl]
"""
import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--out", default="received.jsonl")
    args = ap.parse_args()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            try:
                payload = json.loads(body)
            except ValueError:
                payload = {"raw": body.decode("utf-8", "replace")}
            line = json.dumps({"received_at": int(time.time()), "payload": payload}, ensure_ascii=False)
            with open(args.out, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            print(line, flush=True)
            self.send_response(204)
            self.end_headers()

        def log_message(self, *a):
            pass

    print(f"listening on http://127.0.0.1:{args.port}/ -> {args.out}", flush=True)
    HTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
