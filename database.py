import os
import sqlite3
import threading
import json
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from contextlib import contextmanager
from typing import Optional

DB_PATH = os.getenv("DB_PATH", "facta_clt.db")

# Todos os timestamps (criado_em, finalizado_em) sao salvos em UTC
# (datetime.utcnow()), mas o "dia" que importa pro dashboard e o dia no
# horario de Brasilia, nao em UTC - sem isso, as estatisticas de "hoje"
# zeravam as 21h (horario de Brasilia = meia-noite UTC), mesmo ainda
# sendo o mesmo dia aqui.
FUSO_BR = ZoneInfo("America/Sao_Paulo")


def _limites_dia_br_em_utc(dia: Optional[date] = None) -> tuple[str, str]:
    """
    Retorna (inicio_utc_iso, fim_utc_iso) correspondentes ao dia completo
    (00:00 a 23:59:59) no horario de Brasilia, convertido para UTC - usado
    para filtrar criado_em (que esta em UTC) corretamente por "dia local".
    """
    if dia is None:
        dia = datetime.now(FUSO_BR).date()
    inicio_br = datetime(dia.year, dia.month, dia.day, 0, 0, 0, tzinfo=FUSO_BR)
    fim_br = inicio_br + timedelta(days=1)
    inicio_utc = inicio_br.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat()
    fim_utc = fim_br.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat()
    return inicio_utc, fim_utc

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
        if "usou_proxy" not in colunas:
            conn.execute("ALTER TABLE requisicoes ADD COLUMN usou_proxy INTEGER NOT NULL DEFAULT 0")
        if "caiu_para_direto" not in colunas:
            conn.execute("ALTER TABLE requisicoes ADD COLUMN caiu_para_direto INTEGER NOT NULL DEFAULT 0")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                chave TEXT PRIMARY KEY,
                valor TEXT NOT NULL
            )
        """)
        conn.execute(
            "INSERT OR IGNORE INTO config (chave, valor) VALUES ('concorrencia', '5')"
        )
        # Config de proxy - valores default vazios/desligado. Se existirem
        # env vars FACTA_* no primeiro boot, elas sao usadas soh como
        # fallback em tempo de leitura (ver facta_clt.get_proxy_config),
        # nao sao copiadas pro banco automaticamente - editar via
        # /admin/config/proxy e a forma oficial de configurar.
        conn.execute(
            "INSERT OR IGNORE INTO config (chave, valor) VALUES ('usar_proxy', 'false')"
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


# ------------------------------------------------------------------
# Config de proxy - chaves dedicadas na mesma tabela `config`, editaveis
# via /admin/config/proxy sem precisar de redeploy. A senha fica salva
# no banco (mesmo padrao ja usado para api_key das equipes), mas
# get_proxy_config_publico() nunca a devolve - so get_proxy_config()
# (uso interno do facta_clt.py) tem acesso a ela.
# ------------------------------------------------------------------

_CHAVES_PROXY = ("usar_proxy", "proxy_host", "proxy_port", "proxy_user", "proxy_pass")


def get_proxy_config() -> dict:
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT chave, valor FROM config WHERE chave IN ({','.join('?' * len(_CHAVES_PROXY))})",
            _CHAVES_PROXY,
        ).fetchall()
        cfg = {r["chave"]: r["valor"] for r in rows}
        return {
            "usar_proxy": cfg.get("usar_proxy", "false").lower() in ("1", "true", "yes"),
            "proxy_host": cfg.get("proxy_host", ""),
            "proxy_port": cfg.get("proxy_port", ""),
            "proxy_user": cfg.get("proxy_user", ""),
            "proxy_pass": cfg.get("proxy_pass", ""),
        }


def get_proxy_config_publico() -> dict:
    cfg = get_proxy_config()
    return {
        "usar_proxy": cfg["usar_proxy"],
        "proxy_host": cfg["proxy_host"],
        "proxy_port": cfg["proxy_port"],
        "proxy_user": cfg["proxy_user"],
        "proxy_pass_configurada": bool(cfg["proxy_pass"]),
    }


def set_proxy_config(
    usar_proxy: Optional[bool] = None,
    proxy_host: Optional[str] = None,
    proxy_port: Optional[str] = None,
    proxy_user: Optional[str] = None,
    proxy_pass: Optional[str] = None,
):
    if usar_proxy is not None:
        set_config("usar_proxy", "true" if usar_proxy else "false")
    if proxy_host is not None:
        set_config("proxy_host", proxy_host)
    if proxy_port is not None:
        set_config("proxy_port", str(proxy_port))
    if proxy_user is not None:
        set_config("proxy_user", proxy_user)
    if proxy_pass is not None:
        set_config("proxy_pass", proxy_pass)


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


def marcar_processando(req_id: int):
    """
    Atualiza o status no banco quando um worker pega o job da fila para
    processar de fato - sem isso, o dashboard mostrava 'na_fila' para
    jobs que ja estavam sendo processados havia tempo, ja que o banco so
    era atualizado no final (finalizar_requisicao).
    """
    with get_conn() as conn:
        conn.execute(
            "UPDATE requisicoes SET status = 'processando' WHERE id = ?", (req_id,)
        )


def finalizar_requisicao(
    req_id: int,
    status: str,
    tentativas: Optional[int] = None,
    tipo_erro: Optional[str] = None,
    detalhe_erro: Optional[str] = None,
    resultado_resumo: Optional[dict] = None,
    chamadas_realizadas: Optional[int] = None,
    usou_proxy: bool = False,
    caiu_para_direto: bool = False,
):
    with get_conn() as conn:
        conn.execute(
            "UPDATE requisicoes SET status=?, tentativas=?, tipo_erro=?, detalhe_erro=?, "
            "finalizado_em=?, resultado_resumo=?, chamadas_realizadas=?, usou_proxy=?, "
            "caiu_para_direto=? WHERE id=?",
            (
                status,
                tentativas,
                tipo_erro,
                detalhe_erro,
                datetime.utcnow().isoformat(),
                json.dumps(resultado_resumo, ensure_ascii=False) if resultado_resumo else None,
                chamadas_realizadas,
                int(usou_proxy),
                int(caiu_para_direto),
                req_id,
            ),
        )


def get_requisicao(req_id: int) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM requisicoes WHERE id = ?", (req_id,)).fetchone()
        return dict(row) if row else None


def stats_hoje(equipe: Optional[str] = None) -> dict:
    hoje = datetime.now(FUSO_BR).date().isoformat()
    inicio_utc, fim_utc = _limites_dia_br_em_utc()
    with get_conn() as conn:
        query = "SELECT status, COUNT(*) as qtd FROM requisicoes WHERE criado_em >= ? AND criado_em < ?"
        params: list = [inicio_utc, fim_utc]
        if equipe:
            query += " AND equipe = ?"
            params.append(equipe)
        query += " GROUP BY status"
        rows = conn.execute(query, params).fetchall()

        por_status = {r["status"]: r["qtd"] for r in rows}

        query_equipe = "SELECT equipe, status, COUNT(*) as qtd FROM requisicoes WHERE criado_em >= ? AND criado_em < ?"
        params_e: list = [inicio_utc, fim_utc]
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
    "Hoje" e calculado no horario de Brasilia (ver _limites_dia_br_em_utc),
    nao em UTC puro.
    """
    inicio_utc, fim_utc = _limites_dia_br_em_utc()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as total_jobs, "
            "SUM(COALESCE(chamadas_realizadas, 0)) as total_chamadas, "
            "AVG(chamadas_realizadas) as media_chamadas_por_job "
            "FROM requisicoes WHERE criado_em >= ? AND criado_em < ? AND chamadas_realizadas IS NOT NULL",
            (inicio_utc, fim_utc),
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
