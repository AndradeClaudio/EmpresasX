# backend/db_init.py
import duckdb, numpy as np

con = duckdb.connect("./cnpj.duckdb")

# 1. carregar extensões já instaladas -----------------------------
con.execute("LOAD fts;")      # wheel duckdb-extension-fts precisa estar instalado
con.execute("LOAD vss;")      # idem para vetor

# 2. índice FTS (id = cnpj_basico, coluna = razao_social) ---------
con.execute("""
    PRAGMA create_fts_index('empresas', 'cnpj_basico', 'razao_social');
""")

# 3. função-Python e índice vetorial CNAE -------------------------
def to_cnae_vector(main, sec):
    v = np.zeros(600, dtype=np.float32)
    if main: v[int(main[:5])] = 1.0
    for c in (sec or "").split(","):
        if c: v[int(c[:5])] = 0.3
    return v

duckdb.create_function("to_cnae_vector", to_cnae_vector, return_type="FLOAT[]")
con.execute("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS cnae_vec FLOAT[600];")
con.execute("""
    UPDATE empresas
    SET cnae_vec = to_cnae_vector(cnae_fiscal_principal, cnae_fiscal_secundaria)
    WHERE cnae_vec IS NULL;
""")
con.execute("CALL vss_create_index('empresas','cnae_vec');")

print("✅  FTS (create_fts_index) e VSS prontos.")
