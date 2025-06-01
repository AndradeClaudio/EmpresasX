# backend/main.py
# ==========================================================
# FastAPI + PydanticAI gateway sobre o banco DuckDB cnpj.duckdb
# ==========================================================
import os, re, duckdb, pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from pydantic_ai import Agent

DB_FILE = os.getenv("CNPJ_DB", "../cnpj.duckdb")
MODEL_TEXT2SQL = os.getenv("LLM_SQL", "openai:gpt-4o-mini")
MODEL_SUMMARY  = os.getenv("LLM_SUM", "openai:gpt-4o-mini")

# ──────────────── conexão global ────────────────
con = duckdb.connect(DB_FILE)

# ──────────────── Agents PydanticAI ─────────────
class SQLQuery(BaseModel):
    sql: str

class NaturalAnswer(BaseModel):
    answer: str

sql_agent = Agent(
    MODEL_TEXT2SQL,
    result_type=SQLQuery,
    system_prompt=(
        "Você é um gerador de SQL DuckDB para o Cadastro Nacional de Empresas.\n"
        "Responda apenas com JSON: {\"sql\": \"SELECT ...\"}"
    ),
)

answer_agent = Agent(
    MODEL_SUMMARY,
    result_type=NaturalAnswer,
    system_prompt=(
        "Você é um assistente que responde em português. "
        "Receberá uma pequena tabela e deve responder ao usuário de forma clara."
    ),
)

# ──────────────── FastAPI app ───────────────────
app = FastAPI(title="Chat Empresas CNPJ")

# ---------- helpers ----------
def classify(text: str) -> str:
    text = text.lower()
    if re.search(r"\b(on[de]|localiza|fica)\b", text):
        return "local"
    if "filial" in text or "filiais" in text:
        return "filial"
    if re.search(r"parecid|similar|semelh", text):
        return "cnae_sim"
    return "rag"

def _cnpj_by_nome(nome: str) -> str | None:
    row = con.execute(
        """
        SELECT cnpj_basico
        FROM (
            SELECT
              fts_main_empresas.match_bm25(cnpj_basico, ?) AS score,
              cnpj_basico
            FROM empresas
        )
        WHERE score IS NOT NULL
        ORDER BY score DESC
        LIMIT 1
        """,
        [nome],
    ).fetchone()
    return row[0] if row else None


# ---------- rotas dedicadas ----------
def empresa_local(pergunta: str):
    cnpj = _cnpj_by_nome(pergunta)
    if not cnpj:
        return {"erro": "Empresa não encontrada"}
    df = con.execute(
        """
        SELECT logradouro, numero, bairro, municipio, uf
        FROM estabelecimentos
        WHERE cnpj_basico=? AND identificador_matriz_filial='1'
        """,
        [cnpj],
    ).df()
    return df.to_dict(orient="records")[0]

def conta_filiais(pergunta: str):
    cnpj = _cnpj_by_nome(pergunta)
    if not cnpj:
        return {"erro": "Empresa não encontrada"}
    n = con.execute(
        """
        SELECT COUNT(*) FROM estabelecimentos
        WHERE cnpj_basico=? AND identificador_matriz_filial='2'
        """,
        [cnpj],
    ).fetchone()[0]
    return {"filiais": int(n)}

def similares_cnae(pergunta: str):
    cnpj = _cnpj_by_nome(pergunta)
    if not cnpj:
        return {"erro": "Empresa não encontrada"}
    vec = con.execute(
        "SELECT cnae_vec FROM empresas WHERE cnpj_basico=?", [cnpj]
    ).fetchone()[0]
    df = con.execute(
        """
        SELECT razao_social, 1 - (cnae_vec <=> ?) AS score
        FROM empresas
        ORDER BY cnae_vec <=> ? LIMIT 10
        """,
        [vec, vec],
    ).df()
    return df.to_dict(orient="records")

# ---------- modelo de requisição ----------
class Question(BaseModel):
    q: str

# ---------- endpoint principal ----------
@app.post("/ask")
def ask(payload: Question):
    q = payload.q.strip()
    match classify(q):
        case "local":
            return empresa_local(q)
        case "filial":
            return conta_filiais(q)
        case "cnae_sim":
            return similares_cnae(q)
        case _:
            # ------------ rota RAG ----------------
            sql_obj = sql_agent.run(q)       # ← retorna SQLQuery
            df = con.execute(sql_obj.sql).df()
            markdown_table = df.head(15).to_markdown(index=False)
            natural = answer_agent.run(f"Tabela:\n{markdown_table}")
            return {"sql": sql_obj.sql, "answer": natural.answer}
