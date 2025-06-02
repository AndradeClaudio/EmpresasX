"""tests/test_api.py
Script de testes automatizados (pytest) para o backend FastAPI.
Pré‑requisitos:
  pip install pytest requests
Antes de rodar: inicie a API local
  uvicorn backend.main:app --reload
Execute tests:
  pytest -q tests/test_api.py
"""

import requests
import pytest

API_URL = "http://localhost:8000/ask"
TIMEOUT = 15  # segundos


def _ask(question: str):
    """Envia pergunta ao endpoint /ask e devolve o JSON."""
    resp = requests.post(API_URL, json={"q": question}, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


@pytest.mark.parametrize(
    "question,expected_keys",
    [
        ("Onde fica a empresa Scoras Tecnologia?", {"logradouro", "municipio", "uf"}),
        ("Quantas filiais tem a Vaccinar?", {"filiais"}),
    ],
)
def test_enderecos_e_filiais(question, expected_keys):
    data = _ask(question)
    assert expected_keys.issubset(data.keys()), data


def test_similaridade_cnae():
    data = _ask("Empresas parecidas com Natura")
    assert isinstance(data, list) and len(data) > 0, data
    first = data[0]
    assert {"razao_social", "score"}.issubset(first), first
    assert 0 <= first["score"] <= 1, first


def test_rag_fallback():
    data = _ask("Qual o capital social da Ambev?")
    assert {"sql", "answer"}.issubset(data), data
    assert "SELECT" in data["sql"].upper()
    assert len(data["answer"]) > 0
