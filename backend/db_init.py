#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
db_init.py ― Gera a tabela `empresas_vec` com vetores de CNAE e cria um
índice VSS (HNSW) no DuckDB.

Pré-requisitos:
    pip install duckdb pandas numpy
    • O arquivo ./cnpj.duckdb deve existir e conter a tabela `estabelecimentos`.
    • DuckDB ≥ 0.10.0 com as extensões vss e fts instaladas.
"""

import duckdb
import numpy as np
import pandas as pd

# ── Configurações básicas ───────────────────────────────────────────────
DB_PATH        = "./cnpj.duckdb"
TABLE_NAME     = "empresas_vec"
INDEX_NAME     = "idx_emp_cnae_vec"
METRIC         = "cosine"
SEC_WEIGHT     = 0.30          # peso atribuído às CNAEs secundárias

# ── Conexão e extensões ─────────────────────────────────────────────────
con = duckdb.connect(DB_PATH)
con.execute("LOAD vss;")   # Vetor-Similarity Search
con.execute("LOAD fts;")   # (caso você queira FTS depois)
con.execute("PRAGMA hnsw_enable_experimental_persistence = true;")

con.execute("""
    PRAGMA create_fts_index('empresas','cnpj_basico','razao_social', stemmer = 'portuguese',overwrite =1,lower=0);
""")
# ── Extração de dados CNAE ──────────────────────────────────────────────
df_raw = con.execute("""
    SELECT cnpj_basico,
           cnae_fiscal_principal,
           cnae_fiscal_secundaria
      FROM estabelecimentos;
""").df()

# ── Construção do dicionário de códigos ─────────────────────────────────
codes = set()

def first_5(txt: str | None) -> str:
    """Mantém só os 5 primeiros dígitos (ou '')"""
    return (txt or "")[:5]

for pri, sec in zip(df_raw.cnae_fiscal_principal,
                    df_raw.cnae_fiscal_secundaria):
    if pri:
        codes.add(first_5(pri))
    for part in (sec or "").split(","):
        part = part.strip()
        if part:
            codes.add(first_5(part))

code2idx = {code: idx for idx, code in enumerate(sorted(codes))}
DIM = len(code2idx)

# ── Helpers para vetorização ────────────────────────────────────────────
def split_sec(txt: str | None) -> list[str]:
    return [first_5(p) for p in (txt or "").split(",") if p.strip()]

def make_vec(pri: str | None, sec_list: list[str]) -> list[float]:
    v = np.zeros(DIM, dtype=np.float32)
    if pri:
        v[code2idx[first_5(pri)]] = 1.0
    for c in sec_list:
        v[code2idx[c]] = max(v[code2idx[c]], SEC_WEIGHT)
    return v.tolist()

# ── DataFrame com vetores ───────────────────────────────────────────────
records = [
    (row.cnpj_basico,
     make_vec(row.cnae_fiscal_principal,
              split_sec(row.cnae_fiscal_secundaria)))
    for row in df_raw.itertuples(index=False)
]

emp_vec = (pd.DataFrame(records,
                        columns=["cnpj_basico", "cnae_vec"])
           .drop_duplicates(subset="cnpj_basico"))

# ── Materialização em DuckDB ────────────────────────────────────────────
con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
con.register("emp_vec_df", emp_vec)

con.execute(f"""
    CREATE TABLE {TABLE_NAME} AS
    SELECT
        cnpj_basico,
        -- cast list → ARRAY fixo
        cnae_vec::FLOAT[{DIM}] AS cnae_vec
    FROM emp_vec_df;
""")

# ── Índice VSS (HNSW) ───────────────────────────────────────────────────
con.execute(f"DROP INDEX IF EXISTS {INDEX_NAME};")
con.execute(f"""
    CREATE INDEX {INDEX_NAME}
      ON {TABLE_NAME} USING HNSW (cnae_vec)
      WITH (metric = '{METRIC}');
""")

# ── Resumo ──────────────────────────────────────────────────────────────
total = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};").fetchone()[0]
print(f"✅  {TABLE_NAME} criada com {total:,} vetores (dim={DIM}) "
      f"e índice '{INDEX_NAME}' pronto.")

con.close()
