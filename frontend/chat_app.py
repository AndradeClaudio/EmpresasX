# frontend/chat_app.py
# -------------------------------------------
import streamlit as st, requests, json, pandas as pd

API_URL = "http://localhost:8000/ask"

st.title("🗣️ Chat Empresas - CNPJ")

if "history" not in st.session_state:
    st.session_state.history = []

user_msg = st.chat_input("Pergunte algo…")
if user_msg:
    st.session_state.history.append(("user", user_msg))
    resp = requests.post(API_URL, json={"q": user_msg}).json()
    st.session_state.history.append(("bot", resp))

for role, content in st.session_state.history:
    with st.chat_message(role):
        if isinstance(content, dict):
            st.json(content)
        else:
            st.markdown(content)
