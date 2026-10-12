"""Model backends for text-to-SQL. Both are called at temperature 0."""
import json
import os
from typing import Protocol

DEFAULT_VERTEX_MODEL = "gemini-3.5-flash"
DEFAULT_LOCATION = "global"


class Backend(Protocol):
    name: str

    def complete(self, prompt: str) -> str: ...


class VertexBackend:
    """Gemini on Vertex AI through the google-genai SDK.

    The project comes from GOOGLE_CLOUD_PROJECT and the location from GOOGLE_CLOUD_LOCATION.
    Credentials are the application default credentials (GOOGLE_APPLICATION_CREDENTIALS).
    """

    def __init__(self, model: str | None = None, project: str | None = None,
                 location: str | None = None, client=None):
        self.model = model or os.environ.get("MDP_SQL_MODEL") or DEFAULT_VERTEX_MODEL
        if self.model.startswith("gemini-2.5"):
            raise ValueError(f"{self.model} is retired on 2026-10-20; use a gemini-3.x model")
        self.name = f"vertex:{self.model}"
        if client is None:
            project = project or os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not project:
                raise ValueError("GOOGLE_CLOUD_PROJECT is not set")
            _check_credentials_file()
            from google import genai
            client = genai.Client(vertexai=True, project=project,
                                  location=location or os.environ.get("GOOGLE_CLOUD_LOCATION") or DEFAULT_LOCATION)
        self._client = client

    def complete(self, prompt: str) -> str:
        resp = self._client.models.generate_content(
            model=self.model, contents=prompt, config={"temperature": 0})
        return resp.text or ""


def _check_credentials_file() -> None:
    """Fail early and clearly when the mounted credentials are the empty placeholder."""
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path or not os.path.exists(path):
        return
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, ValueError):
        raise ValueError(f"{path} is not readable JSON") from None
    if not isinstance(doc, dict) or "type" not in doc:
        raise ValueError("no Google credentials mounted: set GCP_ADC_FILE to your "
                         "application_default_credentials.json before starting the driver")


class OllamaBackend:
    """A local model served by Ollama."""

    def __init__(self, model: str | None = None, host: str | None = None, timeout: float = 600.0):
        self.model = model or os.environ.get("OLLAMA_MODEL")
        if not self.model:
            raise ValueError("set OLLAMA_MODEL (or pass a model) to use the Ollama backend")
        self.host = (host or os.environ.get("OLLAMA_HOST") or "http://localhost:11434").rstrip("/")
        self.timeout = timeout
        self.name = f"ollama:{self.model}"

    def complete(self, prompt: str) -> str:
        import requests

        resp = requests.post(
            f"{self.host}/api/generate", timeout=self.timeout,
            json={"model": self.model, "prompt": prompt, "stream": False, "options": {"temperature": 0}})
        resp.raise_for_status()
        return resp.json().get("response", "")


def from_env(kind: str | None = None) -> Backend:
    kind = kind or os.environ.get("MDP_SQL_BACKEND") or "vertex"
    if kind == "vertex":
        return VertexBackend()
    if kind == "ollama":
        return OllamaBackend()
    raise ValueError(f"unknown backend {kind!r} (vertex or ollama)")
