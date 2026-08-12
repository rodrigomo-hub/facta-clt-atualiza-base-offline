import os
import sqlite3
import threading
import json
from datetime import datetime, date
from contextlib import contextmanager
from typing import Optional

DB_PATH = os.getenv("DB_PATH", "facta_clt.db")

_lock = threading.Lock()


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def get_conn():
    with _lock:
        conn = _connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS equipes (
                api_key TEXT PRIMARY KEY,
                nome TEXT NOT NULL,
                ativo INTEGER NOT NULL DEFAULT 1,
                prioridade INTEGER NOT NULL DEFAULT 0,
                criado_em TEXT NOT NULL
            )
        """)
        colunas_equipes = [r["name"] for r in conn.execute("PRAGMA table_info(equipes)").fetchall()]
        if "prioridade" not in colunas_equipes:
            conn.execute("ALTER TABLE equipes ADD COLUMN prioridade INTEGER NOT NULL DEFAULT 0")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS requisicoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                equipe TEXT NOT NULL,
                cpf TEXT NOT NULL,
                status TEXT NOT NULL,
                tipo_erro TEXT,
                detalhe_erro TEXT,
                tentativas INTEGER,
                chamadas_realizadas INTEGER,
                job_id TEXT,
                criado_em TEXT NOT NULL,
                finalizado_em TEXT,
                resultado_resumo TEXT
            )
        """)
        # Migracao leve para bancos ja existentes sem a coluna nova
        colunas = [r["name"] for r in conn.execute("PRAGMA table_info(requisicoes)").fetchall()]
        if "chamadas_realizadas" not in colunas:
            conn.execute("ALTER TABLE requisicoes ADD COLUMN chamadas_realizadas INTEGER")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                chave TEXT PRIMARY KEY,
                valor TEXT NOT NULL
            )
        """)
        conn.execute(
            "INSERT OR IGNORE INTO config (chave, valor) VALUES ('concorrencia', '5')"
        )


def get_config(chave: str, default: str = "") -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT valor FROM config WHERE chave = ?", (chave,)).fetchone()
        return row["valor"] if row else default


def set_config(chave: str, valor: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO config (chave, valor) VALUES (?, ?) "
            "ON CONFLICT(chave) DO UPDATE SET valor = excluded.valor",
            (chave, valor),
        )


def get_equipe(api_key: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM equipes WHERE api_key = ? AND ativo = 1", (api_key,)
        ).fetchone()
        return dict(row) if row else None


def criar_equipe(api_key: str, nome: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO equipes (api_key, nome, ativo, criado_em) VALUES (?, ?, 1, ?)",
            (api_key, nome, datetime.utcnow().isoformat()),
        )


def listar_equipes() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT api_key, nome, ativo, prioridade, criado_em FROM equipes").fetchall()
        return [dict(r) for r in rows]


def atualizar_prioridade_equipe(api_key: str, prioridade: int):
    with get_conn() as conn:
        conn.execute(
            "UPDATE equipes SET prioridade = ? WHERE api_key = ?", (prioridade, api_key)
        )


def registrar_requisicao_criada(equipe: str, cpf: str, job_id: str) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO requisicoes (equipe, cpf, status, job_id, criado_em) "
            "VALUES (?, ?, 'na_fila', ?, ?)",
            (equipe, cpf, job_id, datetime.utcnow().isoformat()),
        )
        return cur.lastrowid


def finalizar_requisicao(
    req_id: int,
    status: str,
    tentativas: Optional[int] = None,
    tipo_erro: Optional[str] = None,
    detalhe_erro: Optional[str] = None,
    resultado_resumo: Optional[dict] = None,
    chamadas_realizadas: Optional[int] = None,
):
    with get_conn() as conn:
        conn.execute(
            "UPDATE requisicoes SET status=?, tentativas=?, tipo_erro=?, detalhe_erro=?, "
            "finalizado_em=?, resultado_resumo=?, chamadas_realizadas=? WHERE id=?",
            (
                status,
                tentativas,
                tipo_erro,
                detalhe_erro,
                datetime.utcnow().isoformat(),
                json.dumps(resultado_resumo, ensure_ascii=False) if resultado_resumo else None,
                chamadas_realizadas,
                req_id,
            ),
        )


def get_requisicao(req_id: int) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM requisicoes WHERE id = ?", (req_id,)).fetchone()
        return dict(row) if row else None


def stats_hoje(equipe: Optional[str] = None) -> dict:
    hoje = date.today().isoformat()
    with get_conn() as conn:
        query = "SELECT status, COUNT(*) as qtd FROM requisicoes WHERE date(criado_em) = ?"
        params: list = [hoje]
        if equipe:
            query += " AND equipe = ?"
            params.append(equipe)
        query += " GROUP BY status"
        rows = conn.execute(query, params).fetchall()

        por_status = {r["status"]: r["qtd"] for r in rows}

        query_equipe = "SELECT equipe, status, COUNT(*) as qtd FROM requisicoes WHERE date(criado_em) = ?"
        params_e: list = [hoje]
        if equipe:
            query_equipe += " AND equipe = ?"
            params_e.append(equipe)
        query_equipe += " GROUP BY equipe, status"
        rows_equipe = conn.execute(query_equipe, params_e).fetchall()

        por_equipe: dict = {}
        for r in rows_equipe:
            por_equipe.setdefault(r["equipe"], {})[r["status"]] = r["qtd"]

        total = sum(por_status.values())

        return {
            "data": hoje,
            "total": total,
            "por_status": por_status,
            "por_equipe": por_equipe,
        }


def stats_capacidade_hoje() -> dict:
    """
    Agrega chamadas HTTP reais feitas hoje (nao jobs, chamadas de fato) para
    calcular throughput real vs capacidade teorica do rate limiter.
    """
    hoje = date.today().isoformat()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as total_jobs, "
            "SUM(COALESCE(chamadas_realizadas, 0)) as total_chamadas, "
            "AVG(chamadas_realizadas) as media_chamadas_por_job "
            "FROM requisicoes WHERE date(criado_em) = ? AND chamadas_realizadas IS NOT NULL",
            (hoje,),
        ).fetchone()
        return {
            "total_jobs_processados_hoje": row["total_jobs"] or 0,
            "total_chamadas_http_hoje": row["total_chamadas"] or 0,
            "media_chamadas_por_job": round(row["media_chamadas_por_job"], 2) if row["media_chamadas_por_job"] else 0,
        }


def ultimas_requisicoes(limite: int = 50, equipe: Optional[str] = None) -> list[dict]:
    with get_conn() as conn:
        query = "SELECT * FROM requisicoes"
        params: list = []
        if equipe:
            query += " WHERE equipe = ?"
            params.append(equipe)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limite)
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
