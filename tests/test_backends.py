import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from mdp.analytics.text2sql import backends


class FakeGenai:
    """Stands in for google.genai.Client; remembers what it was asked."""

    def __init__(self, text="```sql\nSELECT 1\n```"):
        self.calls = []
        self.text = text
        self.models = self

    def generate_content(self, model, contents, config):
        self.calls.append({"model": model, "contents": contents, "config": config})

        class Resp:
            pass

        r = Resp()
        r.text = self.text
        return r


def test_vertex_defaults_to_a_gemini_3_model_at_temperature_zero(monkeypatch):
    monkeypatch.delenv("MDP_SQL_MODEL", raising=False)
    fake = FakeGenai()
    b = backends.VertexBackend(client=fake)
    assert b.model == "gemini-3.5-flash"
    assert b.complete("hello") == "```sql\nSELECT 1\n```"
    call = fake.calls[0]
    assert call["model"] == "gemini-3.5-flash"
    assert call["contents"] == "hello"
    assert call["config"]["temperature"] == 0


def test_vertex_model_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("MDP_SQL_MODEL", "gemini-3.5-flash-lite")
    assert backends.VertexBackend(client=FakeGenai()).model == "gemini-3.5-flash-lite"


@pytest.mark.parametrize("model", ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite"])
def test_vertex_refuses_models_that_retire(model):
    with pytest.raises(ValueError, match="retired"):
        backends.VertexBackend(model=model, client=FakeGenai())


def test_vertex_needs_a_project(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    with pytest.raises(ValueError, match="GOOGLE_CLOUD_PROJECT"):
        backends.VertexBackend()


def test_empty_placeholder_credentials_are_reported(monkeypatch, tmp_path):
    f = tmp_path / "adc.json"
    f.write_text("{}")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "some-project")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(f))
    with pytest.raises(ValueError, match="GCP_ADC_FILE"):
        backends.VertexBackend()


def test_none_text_from_the_model_becomes_an_empty_string():
    fake = FakeGenai()
    fake.text = None
    assert backends.VertexBackend(client=fake).complete("x") == ""


class OllamaHandler(BaseHTTPRequestHandler):
    seen: list = []

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        OllamaHandler.seen.append((self.path, body))
        payload = json.dumps({"response": "```sql\nSELECT 2\n```"}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


@pytest.fixture
def ollama_server():
    OllamaHandler.seen = []
    server = HTTPServer(("127.0.0.1", 0), OllamaHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def test_ollama_posts_the_prompt_at_temperature_zero(ollama_server):
    b = backends.OllamaBackend(model="some-model:7b", host=ollama_server)
    assert b.complete("the prompt") == "```sql\nSELECT 2\n```"
    path, body = OllamaHandler.seen[0]
    assert path == "/api/generate"
    assert body["model"] == "some-model:7b"
    assert body["prompt"] == "the prompt"
    assert body["stream"] is False
    assert body["options"]["temperature"] == 0


def test_ollama_needs_a_model(monkeypatch):
    monkeypatch.delenv("OLLAMA_MODEL", raising=False)
    with pytest.raises(ValueError, match="OLLAMA_MODEL"):
        backends.OllamaBackend()


def test_ollama_http_errors_surface(ollama_server):
    class Bad(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_response(500)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Bad)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        b = backends.OllamaBackend(model="m", host=f"http://127.0.0.1:{server.server_port}")
        with pytest.raises(Exception):
            b.complete("x")
    finally:
        server.shutdown()


def test_from_env_picks_the_backend(monkeypatch):
    monkeypatch.setenv("OLLAMA_MODEL", "m")
    assert isinstance(backends.from_env("ollama"), backends.OllamaBackend)
    with pytest.raises(ValueError, match="unknown backend"):
        backends.from_env("nope")
