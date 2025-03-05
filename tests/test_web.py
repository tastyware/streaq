import os

from fastapi.testclient import TestClient

from streaq.web import build_app

app = build_app()
client = TestClient(app)


def test_get_root(redis_url: str):
    os.environ["REDIS_URL"] = redis_url
    res = client.get("/")
    assert res.status_code == 200


def test_get_queue():
    res = client.get("/queues/default")
    assert res.status_code == 200
