import httpx

BASE_URL = "http://localhost:8000"


def test_stats():
    r = httpx.get(f"{BASE_URL}/stats")
    assert r.status_code == 200

    stats = r.json()

    assert "total_messages" in stats
    assert "senders_count" in stats
    assert "messages_per_sender" in stats
    assert "first_message_ts" in stats
    assert "last_message_ts" in stats
