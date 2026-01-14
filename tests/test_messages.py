import httpx

BASE_URL = "http://localhost:8000"


def test_list_messages():
    r = httpx.get(f"{BASE_URL}/messages")
    assert r.status_code == 200

    data = r.json()
    assert "data" in data
    assert "total" in data
    assert "limit" in data
    assert "offset" in data


def test_pagination():
    r = httpx.get(f"{BASE_URL}/messages?limit=1&offset=0")
    assert r.status_code == 200
    assert len(r.json()["data"]) <= 1


def test_filter_by_sender():
    r = httpx.get(
        f"{BASE_URL}/messages",
        params={"from": "+911111111111"}
    )
    assert r.status_code == 200
