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
con.close()
