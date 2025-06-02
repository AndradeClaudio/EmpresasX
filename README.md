# EmpresasX

EmpresasX é uma aplicação que permite consultas inteligentes ao Cadastro Nacional de Empresas utilizando FastAPI, DuckDB, PydanticAI e Streamlit. O sistema suporta perguntas em linguagem natural sobre empresas, localização, filiais e empresas similares, retornando respostas em português.

## Estrutura do Projeto

```
.
├── main.py                # Entrada principal do projeto
├── backend/
│   ├── main.py            # API FastAPI com integração PydanticAI e DuckDB
│   └── db_init.py         # Inicialização do banco e índices FTS/VSS
├── frontend/
│   └── chat_app.py        # (Em desenvolvimento) Interface Streamlit
├── cnpj.duckdb            # Banco de dados DuckDB com dados das empresas
├── pyproject.toml         # Dependências e configuração do projeto
└── README.md              # Este arquivo
```

## Instalação

1. **Pré-requisitos**  
   - Python 3.13+
   - DuckDB e extensões (`duckdb-extension-fts`, `duckdb-extension-vss`)
   - [uv](https://github.com/astral-sh/uv) (opcional, para instalar dependências rapidamente)

2. **Instale as dependências:**
   ```sh
    uv sync
   ```

3. **Configure o banco de dados:**  
   Certifique-se de que o arquivo `cnpj.duckdb` está presente na raiz do projeto.  
   Para criar índices FTS e VSS, execute:
   ```sh
   python backend/db_init.py
   ```

## Como rodar

### Backend (API)

Execute o servidor FastAPI:
```sh
uvicorn backend.main:app --reload
```
A API estará disponível em [http://localhost:8000/docs](http://localhost:8000/docs).

### Frontend (Chat)

(Em desenvolvimento)  
Para rodar a interface Streamlit:
```sh
streamlit run frontend/chat_app.py
```

## Endpoints principais

- `POST /ask`  
  Recebe uma pergunta em linguagem natural e retorna resposta estruturada ou em linguagem natural.

  **Exemplo de payload:**
  ```json
  { "q": "Onde fica a sede da Petrobras?" }
  ```

## Tecnologias

- [FastAPI](https://fastapi.tiangolo.com/)
- [DuckDB](https://duckdb.org/)
- [PydanticAI](https://github.com/pydantic/pydantic-ai)
- [Streamlit](https://streamlit.io/)
- [NumPy](https://numpy.org/)
- [Pandas](https://pandas.pydata.org/)

## Observações

- O projeto utiliza modelos LLM para geração de SQL e sumarização de respostas.
- As extensões FTS e VSS do DuckDB são necessárias para busca textual e vetorial.
- O banco de dados `cnpj.duckdb` deve conter as tabelas `empresas` e `estabelecimentos` com os campos esperados.

---

Desenvolvido por Cláudio Andrade.