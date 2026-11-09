#!/usr/bin/env python3
"""Host-side probe for the Doubao Seed-ASR 2.0 plan-gateway wire protocol.

Mirrors the framing/gzip logic implemented in
components/muse/muse_cn_chat.cpp so the endpoint, headers, binary frames and
response parsing can be verified against a live key without flashing hardware.

Usage:
    python asr_protocol_probe.py "ark-..." [path-to-16k-mono-s16le.pcm]

With no PCM file it sends a short tone (expected to transcribe empty, which
still proves the handshake, framing, gzip and final-frame handling work).

The LLM side uses the same key against the plan gateway and is checked with a
plain HTTPS POST (OpenAI-compatible):
    POST https://ark.cn-beijing.volces.com/api/plan/v3/chat/completions
    Authorization: Bearer <key>
    {"model": "ark-code-latest", "stream": false,
     "messages": [{"role": "user", "content": "..."}]}
"""
import gzip
import json
import struct
import sys
import uuid

import websocket  # websocket-client

HOST = "wss://openspeech.bytedance.com"
ENDPOINT = "/api/v3/plan/sauc/bigmodel_async"
RESOURCE_ID = "volc.seedasr.sauc.duration"

FULL_CLIENT, AUDIO_CLIENT, FULL_SERVER, ERROR = 0x1, 0x2, 0x9, 0xF
FLAG_SEQ_POS, FLAG_SEQ_NEG, FLAG_SERVER_LAST = 0x1, 0x3, 0x2
SER_NONE, SER_JSON = 0x0, 0x1
COMP_GZIP = 0x1


def frame(msg_type, flags, serial, comp, seq, payload):
    head = bytes([0x11, (msg_type << 4) | (flags & 0x0F),
                  (serial << 4) | (comp & 0x0F), 0x00])
    out = head
    if flags & 0x1:
        out += struct.pack(">i", seq)
    out += struct.pack(">I", len(payload)) + payload
    return out


def parse(msg):
    header_size = (msg[0] & 0x0F) * 4
    mtype = msg[1] >> 4
    flags = msg[1] & 0x0F
    serial, comp = (msg[2] >> 4), (msg[2] & 0x0F)
    off = header_size
    if flags & 0x1:
        off += 4
    if mtype == ERROR:
        code = struct.unpack(">i", msg[off:off + 4])[0]
        off += 4
        size = struct.unpack(">I", msg[off:off + 4])[0]
        off += 4
        body = msg[off:off + size]
        if comp == COMP_GZIP:
            try:
                body = gzip.decompress(body)
            except Exception:
                pass
        return ("error", code, body)
    size = struct.unpack(">I", msg[off:off + 4])[0]
    off += 4
    body = msg[off:off + size]
    if comp == COMP_GZIP:
        body = gzip.decompress(body)
    last = bool(flags & FLAG_SERVER_LAST)
    data = json.loads(body) if serial == SER_JSON and body else None
    return ("response" if mtype == FULL_SERVER else "other", last, data)


def main():
    key = sys.argv[1]
    pcm_path = sys.argv[2] if len(sys.argv) > 2 else None
    if pcm_path:
        pcm = open(pcm_path, "rb").read()
    else:
        # 1 s of a 220 Hz tone, 16 kHz mono s16le (not speech: empty transcript).
        import math
        pcm = b"".join(struct.pack("<h", int(8000 * math.sin(2 * math.pi * 220 * i / 16000)))
                      for i in range(16000))

    connect_id = uuid.uuid4().hex
    url = HOST + ENDPOINT
    headers = [
        f"X-Api-Key: {key}",
        f"X-Api-Resource-Id: {RESOURCE_ID}",
        f"X-Api-Connect-Id: {connect_id}",
        f"X-Api-Request-Id: {connect_id}",
        "X-Api-Sequence: -1",
    ]
    ws = websocket.create_connection(url, header=headers, timeout=20)
    print("connected:", url)

    cfg = {
        "user": {"uid": "muse-gadget"},
        "audio": {"format": "pcm", "codec": "raw", "rate": 16000,
                  "bits": 16, "channel": 1},
        "request": {"model_name": "bigmodel", "enable_itn": True,
                    "enable_punc": True, "enable_ddc": False,
                    "result_type": "full"},
    }
    gz = gzip.compress(json.dumps(cfg).encode())
    ws.send_binary(frame(FULL_CLIENT, FLAG_SEQ_POS, SER_JSON, COMP_GZIP, 1, gz))

    seg = 16000 * 2 * 200 // 1000
    seq = 2
    last_text = ""
    for off in range(0, len(pcm), seg):
        chunk = pcm[off:off + seg]
        last = off + seg >= len(pcm)
        agz = gzip.compress(chunk)
        ws.send_binary(frame(AUDIO_CLIENT, FLAG_SEQ_NEG if last else FLAG_SEQ_POS,
                             SER_NONE, COMP_GZIP, -seq if last else seq, agz))
        seq += 1

    ws.settimeout(30)
    final = False
    while not final:
        try:
            msg = ws.recv()
        except websocket.WebSocketTimeoutException:
            print("TIMEOUT waiting for final frame")
            break
        if isinstance(msg, str):
            print("unexpected text:", msg)
            continue
        kind, *rest = parse(msg)
        if kind == "error":
            print("ERROR frame code=%s body=%r" % (rest[0], rest[1]))
            break
        last, data = rest
        if data:
            result = data.get("result", {})
            text = result.get("text", "")
            if text:
                last_text = text
            print(("LAST " if last else "     ") + "json:", json.dumps(data, ensure_ascii=False)[:300])
        if last:
            final = True
    ws.close()
    print("final transcript:", repr(last_text))


if __name__ == "__main__":
    main()
