import httpx
import json
import hmac
import hashlib

BASE_URL = "http://localhost:8000"
SECRET = "testsecret"


def sign(body: dict):
    raw = json.dumps(body, separators=(",", ":")).encode()
    sig = hmac.new(
        SECRET.encode(),
        raw,
        hashlib.sha256
    ).hexdigest()
    return raw, sig


def test_invalid_signature():
    body = {
        "message_id": "m_test_1",
        "from": "+911111111111",
        "to": "+922222222222",
        "ts": "2025-01-15T10:00:00Z",
        "text": "Hello"
    }

    raw = json.dumps(body).encode()

    r = httpx.post(
        f"{BASE_URL}/webhook",
        headers={"X-Signature": "bad"},
        content=raw
    )

    assert r.status_code == 401


def test_valid_insert():
    body = {
        "message_id": "m_test_2",
        "from": "+911111111111",
        "to": "+922222222222",
        "ts": "2025-01-15T10:01:00Z",
        "text": "Hello again"
    }

    raw, sig = sign(body)

    r = httpx.post(
        f"{BASE_URL}/webhook",
        headers={"X-Signature": sig},
        content=raw
    )

    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_duplicate_insert():
    body = {
        "message_id": "m_test_3",
        "from": "+911111111111",
        "to": "+922222222222",
        "ts": "2025-01-15T10:02:00Z",
        "text": "Duplicate test"
    }

    raw, sig = sign(body)

    r1 = httpx.post(
        f"{BASE_URL}/webhook",
        headers={"X-Signature": sig},
        content=raw
    )
    r2 = httpx.post(
        f"{BASE_URL}/webhook",
        headers={"X-Signature": sig},
        content=raw
    )

    assert r1.status_code == 200
    assert r2.status_code == 200
