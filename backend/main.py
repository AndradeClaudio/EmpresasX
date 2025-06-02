"""
Chat Empresas CNPJ — FastAPI + DuckDB + Pydantic-AI
===================================================

• Extrai o schema do DuckDB em tempo de execução (tabelas + colunas apenas).
• Usa pydantic-ai:
  – sql_agent ..... gera SQL DuckDB a partir de linguagem natural.
  – answer_agent .. resume a tabela resultante em português.
  – classify_agent  detecta intenção da pergunta e extrai o nome da empresa.
"""

from __future__ import annotations

import os
import re
import textwrap
from typing import Any

import duckdb
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from pydantic_ai import Agent

# ─────────────────────────────  Configuração inicial  ────────────────────────────
load_dotenv()

DB_FILE = os.getenv("CNPJ_DB", "./cnpj.duckdb")
con: duckdb.DuckDBPyConnection = duckdb.connect(DB_FILE)

# extensões opcionais
for ext in ("fts", "vss"):
    try:
        con.execute(f"LOAD {ext};")
    except duckdb.CatalogException:
        pass


# ────────────────────────────  Util – schema condensado  ─────────────────────────
def _schema_as_text(
    cnx: duckdb.DuckDBPyConnection,
    *,
    max_tables: int | None = None,
) -> str:
    """
    Retorna algo como:
        empresas(cnpj_basico, razao_social, natureza_juridica, porte, ...)
        estabelecimentos(cnpj_basico, identificador_matriz_filial, municipio, uf, ...)
        ...
    — sem tipos, apenas nomes.
    """
    rows: list[tuple[str, str]] = cnx.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY table_name, ordinal_position
        """
    ).fetchall()

    out: list[str] = []
    last_tbl: str | None = None
    tables_seen = 0

    for tbl, col in rows:
        if tbl != last_tbl:
            if max_tables is not None and tables_seen >= max_tables:
                break
            if last_tbl is not None:
                out.append(")")
            out.append(f"{tbl}(")
            last_tbl = tbl
            tables_seen += 1
        out.append(f"    {col},")
    if rows:
        out.append(")")

    return textwrap.dedent("\n".join(out))


SCHEMA_TEXT: str = _schema_as_text(con)

# ──────────────────────────────  Pydantic-AI Agents  ─────────────────────────────
class SQLQuery(BaseModel):
    sql: str


class NaturalAnswer(BaseModel):
    answer: str


class IntentResult(BaseModel):
    intent: str          # "local" | "filial" | "cnae_sim" | "rag"
    empresa: str | None  # nome exato ou null


sql_agent = Agent(
    "groq:gemma2-9b-it",
    result_type=SQLQuery,
    system_prompt=(
        "Você gera consultas **SQL DuckDB**. "
        "Use EXCLUSIVAMENTE o schema abaixo e NÃO invente tabelas ou colunas.\n\n"
        + SCHEMA_TEXT
        + "\n\nRetorne apenas JSON no formato {\"sql\": \"...\"}"
    ),
)

answer_agent = Agent(
    "groq:gemma2-9b-it",
    result_type=NaturalAnswer,
    system_prompt="Resuma a tabela em português, de forma concisa e clara.",
)

classify_agent = Agent(
    "groq:gemma2-9b-it",
    result_type=IntentResult,
    system_prompt=(
        "Você é um classificador de consultas sobre empresas CNPJ.\n"
        "Analise a pergunta e devolva JSON no formato:\n"
        '{"intent":"<local|filial|cnae_sim|rag>", "empresa":"<nome ou null>"}\n'
        "• 'local'  → usuário quer o endereço/matriz.\n"
        "• 'filial' → quantas filiais tem.\n"
        "• 'cnae_sim' → empresas semelhantes pelo CNAE.\n"
        "• 'rag'    → qualquer outra pergunta (RAG/SQL).\n"
        "Extraia o nome da empresa da frase (se houver)."
    ),
)


# ─────────────────────────────  FastAPI & modelos  ───────────────────────────────
app = FastAPI(title="Chat Empresas CNPJ")


class Question(BaseModel):
    q: str


# ───────────────────────── helpers de negócio ────────────────────────────────────
def _cnpj_by_nome(nome: str) -> str | None:
    """
    1º tenta FTS/BM25   → rápido e preciso
    2º fallback ILIKE   → sempre disponível
    """
    try:
        row = con.execute(
            """
            SELECT cnpj_basico
            FROM (
              SELECT cnpj_basico,
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
            return row[0]
    except duckdb.CatalogException:
        pass  # extensão FTS não carregada

    row = con.execute(
        """
        SELECT cnpj_basico
        FROM empresas
        WHERE razao_social ILIKE '%' || ? || '%'
        LIMIT 1
        """,
        [nome],
    ).fetchone()
    return row[0] if row else None


def empresa_local(nome_empresa: str) -> dict[str, Any]:
    cnpj = _cnpj_by_nome(nome_empresa)
    print(f"Empresa '{nome_empresa}' → CNPJ: {cnpj}")
    if not cnpj:
        return {"erro": "Empresa não encontrada"}
    df = con.execute(
        """
        SELECT logradouro, numero, bairro, municipio, uf
        FROM estabelecimentos
        WHERE cnpj_basico = ? AND identificador_matriz_filial = '1'
        """,
        [cnpj],
    ).df()
    return df.to_dict(orient="records")[0] if not df.empty else {"erro": "Endereço não encontrado"}


def conta_filiais(nome_empresa: str) -> dict[str, Any]:
    cnpj = _cnpj_by_nome(nome_empresa)
    if not cnpj:
        return {"erro": "Empresa não encontrada"}
    n = con.execute(
        """
        SELECT COUNT(*)
        FROM estabelecimentos
        WHERE cnpj_basico = ? AND identificador_matriz_filial = '2'
        """,
        [cnpj],
    ).fetchone()[0]
    return {"filiais": int(n)}


def similares_cnae(nome_empresa: str) -> dict[str, Any]:
    cnpj = _cnpj_by_nome(nome_empresa)
    if not cnpj:
        return {"erro": "Empresa não encontrada"}
    vec = con.execute(
        "SELECT cnae_vec FROM empresas_vec WHERE cnpj_basico = ?", [cnpj]
    ).fetchone()
    if not vec:
        return {"erro": "Empresa não possui vetor CNAE"}
    vec = vec[0]
    df = con.execute(
        """
        SELECT e.razao_social, 1 - (v.cnae_vec <=> ?) AS score
        FROM empresas_vec v
        JOIN empresas e USING (cnpj_basico)
        WHERE v.cnpj_basico <> ?
        ORDER BY v.cnae_vec <=> ? LIMIT 10
        """,
        [vec, cnpj, vec],
    ).df()
    return df.to_dict(orient="records")


# ────────────────────────────  Endpoint principal  ───────────────────────────────
@app.post("/ask")
async def ask(payload: Question):
    q = payload.q.strip()

    # 1. Detecta intenção + extrai nome
    intent_res = await classify_agent.run(q)
    intent = intent_res.data.intent
    nome_extraido = intent_res.data.empresa

    # 2. Se o modelo achou um nome confiável, use-o nas funções de negócio
    alvo_nome = nome_extraido or q

    # 3. Despacha conforme a intenção
    if intent == "local":
        return empresa_local(alvo_nome)
    if intent == "filial":
        return conta_filiais(alvo_nome)
    if intent == "cnae_sim":
        return similares_cnae(alvo_nome)

    # ---------- RAG fallback (intent == "rag") ----------
    sql_obj = await sql_agent.run(q)
    print("SQL gerado:", sql_obj.data.sql)

    df = con.execute(sql_obj.data.sql).df()
    # to_markdown requer 'tabulate'; caso absent, cair para string
    try:
        tabela = df.head(15).to_markdown(index=False)
    except ImportError:
        tabela = df.head(15).to_string(index=False)

    answer = answer_agent.run(f"Tabela:\n{tabela}")

    return {"sql": sql_obj.data.sql, "answer": answer.data}
