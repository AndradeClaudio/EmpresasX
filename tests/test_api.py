# tests/test_api.py
#
# Teste live do endpoint /ask.
# Rode antes:  uvicorn main:app --port 8000 --reload
# Rode depois: pytest -q tests/test_api.py

import pytest
import requests
from requests.exceptions import ConnectionError, ReadTimeout


def test_ask_endpoint_live():
    url = "http://localhost:8000/ask"
    payload = {"q": "Qual o CNPJ da SCORAS TECNOLOGIA?"}

    try:
        # timeout=(connect, read)
        resp = requests.post(url, json=payload, timeout=(3, 60))  # ← 60 s para a resposta
    except ConnectionError:
        pytest.skip("API não está rodando em http://localhost:8000")
    except ReadTimeout:
        pytest.fail("API respondeu, mas excedeu o tempo-limite de 60 s")

    assert resp.status_code == 200
    data = resp.json()
    assert "cnpj" in data or "erro" in data, f"Resposta inesperada: {data}"
