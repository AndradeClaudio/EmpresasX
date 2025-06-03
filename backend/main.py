# ────────────────────────────────────────────────────────────────────────────────
#  Chat Empresas CNPJ — devolve {"cnpj": "...", "razao_social": "..."}           #
# ────────────────────────────────────────────────────────────────────────────────
#
#  • Pergunta em linguagem natural.
#  • O agente pydantic-ai aciona a tool `busca_empresa` para mapear nome → dados.
#  • Resposta JSON:
#        { "cnpj": "12345678000190", "razao_social": "ACME S/A" }
#        { "erro": "Empresa não encontrada" }
# ────────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import os
import duckdb
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from pydantic_ai import Agent

# ─── 1. Configuração DuckDB ─────────────────────────────────────────────────────
load_dotenv()

DB_FILE = os.getenv("CNPJ_DB", "./cnpj.duckdb")
con: duckdb.DuckDBPyConnection = duckdb.connect(DB_FILE)

# Tenta carregar extensão FTS (ignora se ausente)
try:
    con.execute("LOAD fts;")
except duckdb.CatalogException:
    pass

# ─── 2. Tool: busca CNPJ + razão social ────────────────────────────────────────
class EmpresaRow(BaseModel):
    cnpj: str | None
    razao_social: str | None

def busca_empresa(nome: str) -> EmpresaRow:
    """
    Retorna (cnpj_basico, razao_social) da empresa com maior score BM25
    ou via fallback ILIKE.
    """
    # ---- Tentativa 1: BM25 ----------------------------------------------------
    try:
        row = con.execute(
            """
            SELECT cnpj_basico, razao_social
            FROM (
              SELECT cnpj_basico,
                     razao_social,
                     fts_main_empresas.match_bm25(cnpj_basico, ?) AS score
              FROM empresas
              WHERE score IS NOT NULL
            )
            ORDER BY score DESC
            LIMIT 1
            """,
            [nome],
        ).fetchone()
        if row:
            return EmpresaRow(cnpj=row[0], razao_social=row[1])
    except duckdb.CatalogException:
        # Extensão FTS não carregada
        pass

    # ---- Tentativa 2: ILIKE fallback -----------------------------------------
    row = con.execute(
        """
        SELECT cnpj_basico, razao_social
        FROM empresas
        WHERE razao_social ILIKE '%' || ? || '%'
        LIMIT 1
        """,
        [nome],
    ).fetchone()

    return (
        EmpresaRow(cnpj=row[0], razao_social=row[1]) if row else EmpresaRow(cnpj=None, razao_social=None)
    )

# ─── 3. Agente Pydantic-AI que invoca a tool ────────────────────────────────────
busca_agent = Agent(
    "groq:gemma2-9b-it",
    result_type=EmpresaRow,
    system_prompt=(
        "Seu objetivo é devolver o CNPJ **e** a razão social de uma empresa.\n"
        "Caso não encontre, responda {\"cnpj\": null, \"razao_social\": null}.\n"
        "Para extrair esses dados use APENAS a ferramenta `busca_empresa`."
    ),
    tools=[busca_empresa],
)

# ─── 4. FastAPI ────────────────────────────────────────────────────────────────
app = FastAPI(title="Chat Empresas CNPJ — CNPJ + Razão Social")

class Pergunta(BaseModel):
    q: str

@app.post("/ask")
async def ask(payload: Pergunta):
    """
    Recebe pergunta em linguagem natural e devolve JSON com
    cnpj + razão social da empresa mencionada (ou erro).
    """
    texto = payload.q.strip()
    res = await busca_agent.run(texto)

    if res.data.cnpj:
        return {"cnpj": res.data.cnpj, "razao_social": res.data.razao_social}

    return {"erro": "Empresa não encontrada"}
