#!/usr/bin/env python3
"""End-to-end withdrawal (撤档) demo against a running API (stdlib only).

Walks through the complete workflow:

  1. upload a document and confirm it is searchable;
  2. withdraw it with a client-chosen withdrawal number (撤档号) and watch it
     disappear from the catalog while new searches return 404;
  3. re-submit the same withdrawal number to observe idempotent receipt replay;
  4. poll the receipt until background reclamation reports ``completed``;
  5. re-archive the SAME document id with corrected content and confirm the new
     generation is searchable while the old receipt stays scoped to old data.

    ARCHIVE_API_BASE=http://localhost:8000 python -I scripts/demo_withdrawal.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = os.environ.get("ARCHIVE_API_BASE", "http://localhost:8000").rstrip("/")
DEMO_DIR = Path(__file__).resolve().parent.parent / "demo"
DOC_ID = "gazette"
WITHDRAWAL_ID = "WD-DEMO-2026-0001"


def call(method: str, path: str, data: bytes | None = None,
         content_type: str = "application/x-ndjson") -> tuple[int, dict]:
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"content-type": content_type} if data is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def heading(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def show_receipt(label: str, receipt: dict) -> None:
    print(f"\n{label}")
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


def main() -> int:
    heading("0. Health")
    print(call("GET", "/health"))

    heading("1. Upload the gazette and confirm it is searchable")
    payload = (DEMO_DIR / "gazette.ndjson").read_bytes()
    status, body = call("PUT", f"/documents/{DOC_ID}", payload)
    print(f"PUT /documents/{DOC_ID} -> HTTP {status}: {body}")
    q = urllib.parse.quote("可用。第二條")
    status, body = call("GET", f"/documents/{DOC_ID}/search?q={q}")
    print(f"search before withdrawal -> HTTP {status}, hits={body.get('result_count')}")

    heading("2. Withdraw the document (撤档号 = " + WITHDRAWAL_ID + ")")
    status, receipt = call(
        "POST",
        f"/documents/{DOC_ID}/withdrawals",
        json.dumps({"withdrawal_id": WITHDRAWAL_ID}).encode(),
        content_type="application/json",
    )
    print(f"POST withdrawal -> HTTP {status}")
    show_receipt("first receipt:", receipt)

    heading("3. Idempotent replay with the same withdrawal number")
    status, replay = call(
        "POST",
        f"/documents/{DOC_ID}/withdrawals",
        json.dumps({"withdrawal_id": WITHDRAWAL_ID}).encode(),
        content_type="application/json",
    )
    print(f"replay -> HTTP {status}, replayed={replay['replayed']}")

    heading("4. Document disappears; fresh search returns 404")
    status, docs = call("GET", "/documents")
    ids = {d["doc_id"] for d in docs}
    print(f"GET /documents contains {DOC_ID!r}: {DOC_ID in ids}")
    status, body = call("GET", f"/documents/{DOC_ID}/search?q={q}")
    print(f"search after withdrawal -> HTTP {status}: {body}")

    heading("5. Poll the receipt until reclamation completes")
    deadline = time.time() + 30
    final = None
    while time.time() < deadline:
        status, final = call("GET", f"/withdrawals/{WITHDRAWAL_ID}")
        print(f"  receipt state: {final['state']} "
              f"items={[i['state'] for i in final['items']]} "
              f"error={final['error']}")
        if final["state"] == "completed":
            break
        time.sleep(0.5)
    if not final or final["state"] != "completed":
        print("withdrawal did not complete in time", file=sys.stderr)
        return 1

    heading("6. Re-archive the SAME document id with corrected content")
    corrected = (DEMO_DIR / "gazette.ndjson").read_text().replace(
        "本規範自公 布之日起施行。",
        "本規範自公布之日起施行，並換發正確掃描件。",
    ).encode("utf-8")
    status, reup = call("PUT", f"/documents/{DOC_ID}", corrected)
    print(f"re-upload -> HTTP {status}: {reup}")
    time.sleep(1.0)
    status, body = call(
        "GET",
        f"/documents/{DOC_ID}/search?q=" + urllib.parse.quote("換發正確掃描件"),
    )
    print(f"new generation search -> HTTP {status}, hits={body.get('result_count')}, "
          f"version={body.get('version_id')}")

    heading("7. Old receipt remains scoped to the old generation")
    status, old = call("GET", f"/withdrawals/{WITHDRAWAL_ID}")
    show_receipt("old receipt (unchanged scope):", old)
    scoped = [i["version_id"] for i in old["items"]]
    assert reup["version_id"] not in scoped, "new archive must never be reclaimed!"
    print(f"\nnew version {reup['version_id']} is outside old scope {scoped}: OK")
    print("\nWITHDRAWAL DEMO COMPLETED SUCCESSFULLY")
    return 0


if __name__ == "__main__":
    sys.exit(main())
