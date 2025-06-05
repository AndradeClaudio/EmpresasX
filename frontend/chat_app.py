# frontend/chat_app.py
# ------------------------------------------------------------------------------
# • Interface Streamlit para o Chat Empresas CNPJ (v3 — saudação e memória).   #
# • Envia o nome do usuário; se ele já tiver histórico, o back‑end responde     #
#   com `greeting` e `previous` logo na primeira requisição.                    #
# • Mostra resposta JSON e latência.                                            #
# ------------------------------------------------------------------------------

import time
import streamlit as st
import requests
from typing import Dict, Any, List, Tuple

# ────────────────────────────── Sessão ‑ inicialização ─────────────────────────
DEFAULT_API_URL = "http://localhost:8000/ask"
state_defaults = {
    "api_url": DEFAULT_API_URL,
    "user_name": "",
    "history": [],  # List[Tuple[str, Any]]
    "session_id": None,
    "greeted": False,
}
for k, v in state_defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ───────────────────────────── Sidebar de configuração ─────────────────────────
st.sidebar.title("🔧 Configurações")
st.session_state.api_url = st.sidebar.text_input(
    "URL da API",
    value=st.session_state.api_url,
    placeholder="http://meu-servidor:8000/ask",
)

# Campo nome do usuário; se mudar, reinicia saudação
user_name_input = st.sidebar.text_input(
    "Seu nome (opcional)", value=st.session_state.user_name, placeholder="Maria, João…"
)
if user_name_input != st.session_state.user_name:
    st.session_state.user_name = user_name_input
    st.session_state.greeted = False  # força nova saudação

# ───────────────────────────── Funções auxiliares ──────────────────────────────

def _post(payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = {}
    if st.session_state.session_id:
        headers["X-Session-ID"] = st.session_state.session_id

    resp = requests.post(
        st.session_state.api_url,
        json=payload,
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def _handle_backend_resp(resp_json: Dict[str, Any]):
    """Interpreta campos especiais (greeting, previous, session_id)."""
    # session_id gerenciado
    if "session_id" in resp_json:
        st.session_state.session_id = resp_json["session_id"]

    # Saudação
    if resp_json.get("greeting") and not st.session_state.greeted:
        st.session_state.history.append(("bot", resp_json["greeting"]))
        st.session_state.greeted = True

    # Previous conversation
    if "previous" in resp_json and resp_json["previous"]:
        for msg in resp_json["previous"]:
            st.session_state.history.append((msg["role"], msg["content"]))

    # Resposta principal (remove campos já tratados)
    main_payload = {
        k: v
        for k, v in resp_json.items()
        if k not in {"greeting", "previous", "session_id"}
    }
    if main_payload:
        st.session_state.history.append(("bot", main_payload))

# ───────────────────────────── Saudação inicial ────────────────────────────────
if st.session_state.user_name and not st.session_state.greeted:
    try:
        greet_resp = _post({"q": "", "user": st.session_state.user_name})
        _handle_backend_resp(greet_resp)
    except Exception as e:
        st.session_state.history.append(("bot", {"erro": str(e)}))

# ───────────────────────────── Entrada do usuário ─────────────────────────────
st.title("🗣️ Chat Empresas - CNPJ")
user_msg = st.chat_input("Pergunte algo…")
if user_msg:
    with st.status("Processando..."):
        st.session_state.history.append(("user", user_msg))
        payload = {"q": user_msg}
        if st.session_state.user_name:
            payload["user"] = st.session_state.user_name

        # Medir latência
        t0 = time.perf_counter()
        try:
            resp_json = _post(payload)
        except Exception as e:
            resp_json = {"erro": str(e)}
        elapsed_ms = (time.perf_counter() - t0) * 1000
        elapsed_s = elapsed_ms / 1000

        # Trata resposta
        _handle_backend_resp(resp_json)
        st.session_state.history.append(
            ("latency", f"⏱️ {elapsed_ms:.0f} ms  ({elapsed_s:.2f} s)")
        )

# ───────────────────────────── Renderização do histórico ───────────────────────
for role, content in st.session_state.history:
    if role == "latency":
        st.caption(content)
        continue

    with st.chat_message(role):
        if isinstance(content, dict):
            st.json(content)
        else:
            st.markdown(content)
