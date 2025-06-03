# frontend/chat_app.py
# ------------------------------------------------------------------------------
# • Envia perguntas para http://localhost:8000/ask
# • Mostra resposta JSON
# • Exibe a latência (ms e s) logo abaixo da resposta
# ------------------------------------------------------------------------------

import time
import streamlit as st
import requests
import pandas as pd  # só se você quiser exibir DataFrames no futuro

API_URL = "http://localhost:8000/ask"

st.title("🗣️ Chat Empresas - CNPJ")

if "history" not in st.session_state:
    st.session_state.history = []

# ────────────────────────────── Entrada do usuário ─────────────────────────────
user_msg = st.chat_input("Pergunte algo…")
if user_msg:
    # guarda pergunta do usuário
    st.session_state.history.append(("user", user_msg))

    # mede o tempo de ida-e-volta
    t0 = time.perf_counter()
    resp_json = requests.post(API_URL, json={"q": user_msg}).json()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    elapsed_s  = elapsed_ms / 1000

    # armazena resposta + latência
    st.session_state.history.append(("bot", resp_json))
    st.session_state.history.append(
        ("latency", f"⏱️ {elapsed_ms:.0f} ms  ({elapsed_s:.2f} s)")
    )

# ────────────────────────────── Renderização histórica ─────────────────────────
for role, content in st.session_state.history:
    # latência aparece como legenda fora dos balões
    if role == "latency":
        st.caption(content)
        continue

    with st.chat_message(role):
        if isinstance(content, dict):
            st.json(content)
        else:
            st.markdown(content)
