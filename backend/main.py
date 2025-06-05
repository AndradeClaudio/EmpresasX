# backend/main.py — Chat Empresas CNPJ v5: memória curta, longa e boas-vindas
# ------------------------------------------------------------------------------
# • Memória curta: armazena cada turno (session_id + user_name) em SQLite.
# • Memória longa: armazena respostas relevantes em FTS5 (SQLite).
# • Quando o usuário informa o nome pela primeira vez em cada sessão, o back-end
#   envia “greeting” e, se houver, “previous” com a última conversa desse nome.
# • SQLite migra automaticamente a tabela chat_history caso não tenha user_name.
# ------------------------------------------------------------------------------
from __future__ import annotations

import os
import re
import time
import uuid
import sqlite3
from typing import List, Tuple, Dict, Any

import duckdb
from dotenv import load_dotenv
from fastapi import FastAPI, Header
from pydantic import BaseModel
from pydantic_ai import Agent

# ─── Configuração de bancos ----------------------------------------------------
load_dotenv()

# DuckDB (empresas)
DB_FILE = os.getenv("CNPJ_DB", "./cnpj.duckdb")
con: duckdb.DuckDBPyConnection = duckdb.connect(DB_FILE)
try:
    con.execute("LOAD fts;")
except duckdb.CatalogException:
    pass

# SQLite (memória)
MEM_FILE = os.getenv("MEM_DB", "./memory.sqlite")
mem_con = sqlite3.connect(MEM_FILE, check_same_thread=False)

# Migração automática: adiciona coluna user_name se faltar
cols = [row[1] for row in mem_con.execute("PRAGMA table_info(chat_history);").fetchall()]
if cols and "user_name" not in cols:
    mem_con.execute("ALTER TABLE chat_history ADD COLUMN user_name TEXT;")
    mem_con.commit()

# Cria tabelas de histórico e memória longa
mem_con.execute(
    """
    CREATE TABLE IF NOT EXISTS chat_history (
        session_id TEXT,
        user_name  TEXT,
        ts         REAL,
        role       TEXT,
        content    TEXT
    )
    """
)
mem_con.execute(
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS long_term_memory USING fts5(
        content,
        metadata
    )
    """
)
mem_con.commit()

# ─── Pydantic Models -----------------------------------------------------------

class SimplesRow(BaseModel):
    cnpj_basico: str | None
    opcao_simples: str | None
    data_opcao_simples: str | None
    data_exclusao_simples: str | None
    opcao_mei: str | None
    data_opcao_mei: str | None
    data_exclusao_mei: str | None


class SocioRow(BaseModel):
    identificador_socio: str | None
    nome_socio_razao_social: str | None
    cpf_cnpj_socio: str | None
    qualificacao_socio: str | None
    data_entrada_sociedade: str | None
    pais: str | None
    nome_representante: str | None
    qualificacao_representante: str | None
    faixa_etaria: str | None


class NaturezaRow(BaseModel):
    codigo: str | None
    descricao: str | None


class CnaeRow(BaseModel):
    cnae_principal: str | None
    desc_principal: str | None
    cnae_secundario: str | None
    desc_secundario: str | None


class ContatoRow(BaseModel):
    telefone: str | None
    fax: str | None
    email: str | None


# -------------- modelos -----------------
class Result(BaseModel):
    cnpj: str | None = None
    razao_social: str | None = None
    explicacao: str | None = None

    simples: list[SimplesRow] | None = None
    socios: list[SocioRow]   | None = None
    natureza: NaturezaRow    | None = None
    cnaes: CnaeRow           | None = None
    contato: ContatoRow      | None = None
    endereco: EmpresaEnderecoRow | None = None


class EmpresaRow(BaseModel):
    cnpj: str | None
    razao_social: str | None


class EmpresaEnderecoRow(BaseModel):
    cnpj_mascarado: str | None
    nome_fantasia: str | None
    endereco: str | None


# ─── DuckDB Helpers ------------------------------------------------------------

def busca_empresa(nome: str) -> EmpresaRow:
    try:
        row = con.execute(
            """
            SELECT cnpj_basico, razao_social
            FROM (
              SELECT cnpj_basico,
                     razao_social,
                     fts_main_empresas.match_bm25(cnpj_basico, ?, fields := 'razao_social', k := 0.5, b := 1.2, conjunctive := 1) AS score
              FROM empresas
              WHERE score IS NOT NULL
            )
            ORDER BY score DESC
            LIMIT 1;
            """,
            [nome],
        ).fetchone()
        if row:
            return EmpresaRow(cnpj=row[0], razao_social=row[1])
    except duckdb.CatalogException:
        pass

    row = con.execute(
        """
        SELECT cnpj_basico, razao_social
        FROM empresas
        WHERE razao_social ILIKE '%' || ? || '%'
        LIMIT 1;
        """,
        [nome],
    ).fetchone()
    return EmpresaRow(cnpj=row[0], razao_social=row[1]) if row else EmpresaRow(cnpj=None, razao_social=None)


def busca_endereco(cnpj: str) -> EmpresaEnderecoRow:
    row = con.execute(
        """
        SELECT
            substr(cnpj_basico||cnpj_ordem||cnpj_dv, 1, 2) || '.' ||
            substr(cnpj_basico||cnpj_ordem||cnpj_dv, 3, 3) || '.' ||
            substr(cnpj_basico||cnpj_ordem||cnpj_dv, 6, 3) || '/' ||
            substr(cnpj_basico||cnpj_ordem||cnpj_dv, 9, 4) || '-' ||
            substr(cnpj_basico||cnpj_ordem||cnpj_dv,13, 2) AS cnpj_mascarado,
            nome_fantasia,
            'Endereço: ' || tipo_logradouro || ' ' || logradouro || ', ' || numero ||
            ' Complemento: ' || complemento || ' Bairro: ' || bairro ||
            ' Cidade: ' || M.descricao || ' Estado: ' || uf AS endereco
        FROM estabelecimentos E
        JOIN municipios M ON E.municipio = M.codigo
        WHERE cnpj_basico = ?
        LIMIT 1;
        """,
        [cnpj],
    ).fetchone()
    return (
        EmpresaEnderecoRow(cnpj_mascarado=row[0], nome_fantasia=row[1], endereco=row[2])
        if row
        else EmpresaEnderecoRow(cnpj_mascarado=None, nome_fantasia=None, endereco=None)
    )


def busca_simples(cnpj: str) -> List[SimplesRow]:
    rows = con.execute(
        """
        SELECT
          cnpj_basico,
          opcao_simples,
          CAST(data_opcao_simples AS VARCHAR) AS data_opcao_simples,
          CAST(data_exclusao_simples AS VARCHAR) AS data_exclusao_simples,
          opcao_mei,
          CAST(data_opcao_mei AS VARCHAR) AS data_opcao_mei,
          CAST(data_exclusao_mei AS VARCHAR) AS data_exclusao_mei
        FROM simples
        WHERE cnpj_basico = ?
        ORDER BY data_opcao_simples DESC
        """,
        [cnpj],
    ).fetchall()
    return [
        SimplesRow(
            cnpj_basico=r[0],
            opcao_simples=r[1],
            data_opcao_simples=r[2],
            data_exclusao_simples=r[3],
            opcao_mei=r[4],
            data_opcao_mei=r[5],
            data_exclusao_mei=r[6],
        )
        for r in rows
    ]


def lista_socios(cnpj: str) -> List[SocioRow]:
    rows = con.execute(
        """
        SELECT
          identificador_socio,
          nome_socio_razao_social,
          cpf_cnpj_socio,
          qualificacao_socio,
          CAST(data_entrada_sociedade AS VARCHAR) AS data_entrada_sociedade,
          pais,
          nome_representante,
          qualificacao_representante,
          faixa_etaria
        FROM socios
        WHERE cnpj_basico = ?
        ORDER BY data_entrada_sociedade DESC
        """,
        [cnpj],
    ).fetchall()
    return [
        SocioRow(
            identificador_socio=r[0],
            nome_socio_razao_social=r[1],
            cpf_cnpj_socio=r[2],
            qualificacao_socio=r[3],
            data_entrada_sociedade=r[4],
            pais=r[5],
            nome_representante=r[6],
            qualificacao_representante=r[7],
            faixa_etaria=r[8],
        )
        for r in rows
    ]


def busca_natureza(cnpj: str) -> NaturezaRow:
    row = con.execute(
        """
        SELECT
          e.natureza_juridica,
          n.descricao
        FROM empresas e
        LEFT JOIN naturezas n ON e.natureza_juridica = n.codigo
        WHERE e.cnpj_basico = ?
        LIMIT 1
        """,
        [cnpj],
    ).fetchone()
    return NaturezaRow(codigo=row[0], descricao=row[1]) if row else NaturezaRow(codigo=None, descricao=None)


def busca_cnaes(cnpj: str) -> CnaeRow:
    row = con.execute(
        """
        SELECT
          est.cnae_fiscal_principal,
          cp.descricao AS desc_principal,
          est.cnae_fiscal_secundaria,
          cs.descricao AS desc_secundario
        FROM estabelecimentos est
        LEFT JOIN cnaes cp ON est.cnae_fiscal_principal = cp.codigo
        LEFT JOIN cnaes cs ON est.cnae_fiscal_secundaria = cs.codigo
        WHERE est.cnpj_basico = ?
        LIMIT 1
        """,
        [cnpj],
    ).fetchone()
    if row:
        return CnaeRow(
            cnae_principal=row[0],
            desc_principal=row[1],
            cnae_secundario=row[2],
            desc_secundario=row[3],
        )
    return CnaeRow(cnae_principal=None, desc_principal=None, cnae_secundario=None, desc_secundario=None)


def busca_contato(cnpj: str) -> ContatoRow:
    row = con.execute(
        """
        SELECT
          CASE WHEN ddd1 IS NOT NULL AND telefone1 IS NOT NULL THEN ddd1 || telefone1 ELSE NULL END AS telefone,
          CASE WHEN ddd_fax IS NOT NULL AND fax IS NOT NULL THEN ddd_fax || fax ELSE NULL END AS fax,
          email
        FROM estabelecimentos
        WHERE cnpj_basico = ?
        LIMIT 1
        """,
        [cnpj],
    ).fetchone()
    return ContatoRow(telefone=row[0], fax=row[1], email=row[2]) if row else ContatoRow(telefone=None, fax=None, email=None)


# ─── Memória Utilitários -------------------------------------------------------

def _add_msg(session_id: str, user_name: str | None, role: str, content: str) -> None:
    mem_con.execute(
        "INSERT INTO chat_history VALUES (?,?,?,?,?)",
        (session_id, user_name, time.time(), role, content),
    )
    mem_con.commit()


def _get_history(session_id: str, limit: int = 10) -> List[Tuple[str, str]]:
    rows = mem_con.execute(
        "SELECT role, content FROM chat_history WHERE session_id = ? ORDER BY ts DESC LIMIT ?",
        (session_id, limit),
    ).fetchall()
    return list(reversed(rows))


def _get_last_conv_by_user(user_name: str, limit: int = 6) -> List[Tuple[str, str]]:
    rows = mem_con.execute(
        "SELECT role, content FROM chat_history WHERE user_name = ? ORDER BY ts DESC LIMIT ?",
        (user_name, limit),
    ).fetchall()
    return list(reversed(rows))


def _add_long_term(content: str, metadata: str = "{}") -> None:
    mem_con.execute(
        "INSERT INTO long_term_memory (content, metadata) VALUES (?, ?)",
        (content, metadata),
    )
    mem_con.commit()


def _search_long_term(query: str, k: int = 3) -> List[str]:
    if not query.strip():
        return []
    safe_q = re.sub(r"[^\w\s]", " ", query)
    try:
        sql = f"SELECT content FROM long_term_memory WHERE long_term_memory MATCH ? LIMIT {k}"
        rows = mem_con.execute(sql, (safe_q,)).fetchall()
        return [r[0] for r in rows]
    except sqlite3.OperationalError:
        return []
# Assinatura compatível com o seu modelo Result

# ─── Agente Pydantic-AI --------------------------------------------------------

busca_agent = Agent(
    "groq:meta-llama/llama-4-scout-17b-16e-instruct",           # modelo que suporte Function Calling
    result_type=Result,
    function_calling=True,
    system_prompt=(
        "Você é um assistente para dados de empresas brasileiras. NÃO gere texto direto. "
        "Responda sempre com uma chamada de função válida em JSON, sem nada além disso. \n"
        "Usar apenas as funções listada abaixo.\n"
        
        "Formato EXATO:\n"
        "{\"name\": \"<nome_da_função>\", \"arguments\": { /* argumentos */ }}\n"
        "funções:\n"
        "- busca_empresa(nome: str)\n"
        "- busca_endereco(cnpj: str)\n"
        "- busca_simples(cnpj: str)\n"
        "- lista_socios(cnpj: str)\n"
        "- busca_natureza(cnpj: str)\n"
        "- busca_cnaes(cnpj: str)\n"
        "- busca_contato(cnpj: str)\n"
        "- final_result(cnpj?: str, razao_social?: str, explicacao?: str, "
        "             simples?: List[SimplesRow], socios?: List[SocioRow], "
        "             natureza?: NaturezaRow, cnaes?: CnaeRow, contato?: ContatoRow, endereco?: EmpresaEnderecoRow) \n\n"
        "Regras:\n"
        "1. Use as funções de busca quantas vezes precisar.\n"
        "2. Quando terminar, chame **somente** `final_result` com todos os campos relevantes."
    ),
    tools=[
        busca_empresa,
        busca_endereco,
        busca_simples,
        lista_socios,
        busca_natureza,
        busca_cnaes,
        busca_contato,
    ],
)

# ─── FastAPI -------------------------------------------------------------------

app = FastAPI(title="Chat Empresas CNPJ — v5")


class Pergunta(BaseModel):
    q: str
    user: str | None = None


@app.post("/ask")
async def ask(
    payload: Pergunta,
    x_session_id: str | None = Header(default=None, alias="X-Session-ID"),
):
    session_id = x_session_id or str(uuid.uuid4())
    question = payload.q.strip()
    user_name = (payload.user or "").strip() or None

    # 1. Memória curta
    short_mem = _get_history(session_id)
    short_ctx = "\n".join([f"{r.upper()}: {c}" for r, c in short_mem])

    # 2. Memória longa
    long_hits = _search_long_term(question)
    long_ctx = "\n".join(long_hits)

    # 3. Saudação + última conversa
    greeting_block: Dict[str, Any] | None = None
    if user_name and not short_mem:
        prev = _get_last_conv_by_user(user_name)
        if prev:
            greeting_block = {
                "greeting": f"Bem-vindo(a) de volta, {user_name}!",
                "previous": [{"role": r, "content": c} for r, c in prev],
            }
        else:
            greeting_block = {"greeting": f"Olá, {user_name}! Como posso ajudar hoje?"}

    # 4. Constrói a “mensagem do usuário” para o agente
    partes = []
    if long_ctx:
        partes.append("Memória longa:\n" + long_ctx)
    if short_ctx:
        partes.append("Histórico recente:\n" + short_ctx)
    partes.append("Pergunta atual:\n" + question)
    mensagem_usuario = "\n\n".join(partes)
    print(f"Mensagem do usuário:\n{mensagem_usuario}\n")

    # 5. Chama o agente (somente se houver pergunta)
    agent_result: Dict[str, Any] | None = None
    if question:
        res = await busca_agent.run(mensagem_usuario.upper())

        # Se não retornou cnpj, grava erro
        if not res.data.cnpj:
            answer_text = "Empresa não encontrada"
        else:
            answer_text = f"CNPJ: {res.data.cnpj}, Razão Social: {res.data.razao_social}"

        # Persiste na memória curta
        _add_msg(session_id, user_name, "user", question)
        _add_msg(session_id, user_name, "assistant", answer_text)

        # Persiste na memória longa (só se achou cnpj)
        if res.data.cnpj:
            _add_long_term(answer_text, metadata=f"{{'user':'{user_name or ''}'}}")

        # Monta resposta final
        agent_result = {
            "cnpj": res.data.cnpj,
            "razao_social": res.data.razao_social,
            "explicacao": res.data.explicacao,
        }
        if res.data.simples is not None:
            agent_result["simples"] = [row.dict() for row in res.data.simples]
        if res.data.socios is not None:
            agent_result["socios"] = [row.dict() for row in res.data.socios]
        if res.data.natureza is not None:
            agent_result["natureza"] = res.data.natureza.dict()
        if res.data.cnaes is not None:
            agent_result["cnaes"] = res.data.cnaes.dict()
        if res.data.contato is not None:
            agent_result["contato"] = res.data.contato.dict()

        agent_result["session_id"] = session_id

    # 6. Retorna saudação + resultado
    response: Dict[str, Any] = {}
    if greeting_block:
        response.update(greeting_block)
    if agent_result:
        response.update(agent_result)
    if not response:
        response = {
            "erro": "Nada para processar — envie uma pergunta.",
            "session_id": session_id,
        }

    return response
