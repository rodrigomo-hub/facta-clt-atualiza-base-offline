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
        # Fila persistida: o que foi pedido (tentativas/prioridade) precisa
        # sobreviver a restart para o job poder ser recolocado na fila no boot.
        if "tentativas_max" not in colunas:
            conn.execute("ALTER TABLE requisicoes ADD COLUMN tentativas_max INTEGER NOT NULL DEFAULT 5")
        if "prioridade" not in colunas:
            conn.execute("ALTER TABLE requisicoes ADD COLUMN prioridade INTEGER NOT NULL DEFAULT 0")
        if "ref" not in colunas:
            conn.execute("ALTER TABLE requisicoes ADD COLUMN ref TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_requisicoes_status ON requisicoes(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_requisicoes_job_id ON requisicoes(job_id)")
        # Outbox de webhooks de retorno: uma linha por (job, url). Fica em
        # 'aguardando' ate o job terminar, vira 'pendente' com o payload
        # pronto e o entregador faz POST com retry. Persistido para nao
        # perder aviso em restart.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS webhooks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                req_id INTEGER NOT NULL,
                job_id TEXT NOT NULL,
                url TEXT NOT NULL,
                ref TEXT,
                status TEXT NOT NULL DEFAULT 'aguardando',
                payload_json TEXT,
                tentativas INTEGER NOT NULL DEFAULT 0,
                ultimo_erro TEXT,
                proximo_envio TEXT,
                criado_em TEXT NOT NULL,
                entregue_em TEXT
            )
        """)
        colunas_wh = [r["name"] for r in conn.execute("PRAGMA table_info(webhooks)").fetchall()]
        if "status_http" not in colunas_wh:
            conn.execute("ALTER TABLE webhooks ADD COLUMN status_http INTEGER")
        if "resposta" not in colunas_wh:
            conn.execute("ALTER TABLE webhooks ADD COLUMN resposta TEXT")
        # Resultado FINAL do fluxo do n8n (pre-aprovado, nao aprovado + motivo,
        # falha...) reportado de volta via POST /resultado. Uma linha por job.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS resultados_fluxo (
                job_id TEXT PRIMARY KEY,
                req_id INTEGER,
                equipe TEXT,
                cpf TEXT,
                resultado TEXT NOT NULL,
                motivo TEXT,
                banco TEXT,
                margem REAL,
                valor_liquido_max REAL,
                detalhes_json TEXT,
                criado_em TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_resultados_criado ON resultados_fluxo(criado_em)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_webhooks_status ON webhooks(status, proximo_envio)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_webhooks_req ON webhooks(req_id)")
        # Fotos periodicas do saldo de trafego do proxy (API do provedor) -
        # servem para estimar consumo/dia, dias restantes e custo.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS proxy_saldo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consultado_em TEXT NOT NULL,
                traffic_limit INTEGER,
                traffic_usage INTEGER,
                traffic_left INTEGER
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_proxy_saldo_em ON proxy_saldo(consultado_em)")
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
    """
    Campos de texto vazios sao IGNORADOS (nao apagam o valor salvo): um
    salvamento em branco no painel ja deixou o proxy "ligado" sem host/porta/
    usuario, e todas as chamadas passaram a sair direto e tomar 403. Para
    parar de usar o proxy, desligue `usar_proxy`.
    """
    if usar_proxy is not None:
        set_config("usar_proxy", "true" if usar_proxy else "false")
    for chave, valor in (
        ("proxy_host", proxy_host), ("proxy_port", proxy_port),
        ("proxy_user", proxy_user), ("proxy_pass", proxy_pass),
    ):
        if valor is not None and str(valor).strip() != "":
            set_config(chave, str(valor).strip())


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


def registrar_requisicao_criada(
    equipe: str, cpf: str, job_id: str,
    tentativas_max: int = 5, prioridade: int = 0, ref: Optional[str] = None,
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO requisicoes (equipe, cpf, status, job_id, criado_em, tentativas_max, prioridade, ref) "
            "VALUES (?, ?, 'na_fila', ?, ?, ?, ?, ?)",
            (equipe, cpf, job_id, datetime.utcnow().isoformat(), tentativas_max, prioridade, ref),
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



# ------------------------------------------------------------------
# Fila persistida - recuperacao no boot
# ------------------------------------------------------------------

def listar_pendentes_fila() -> list[dict]:
    """Jobs que ainda nao terminaram (na_fila ou processando), na ordem de chegada."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, equipe, cpf, job_id, tentativas_max, prioridade, ref "
            "FROM requisicoes WHERE status IN ('na_fila', 'processando') ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]


def voltar_processando_para_fila() -> int:
    """No boot: o que estava 'processando' morreu junto com o processo anterior."""
    with get_conn() as conn:
        cur = conn.execute("UPDATE requisicoes SET status = 'na_fila' WHERE status = 'processando'")
        return cur.rowcount


def get_requisicao_por_job(job_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM requisicoes WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row) if row else None


# ------------------------------------------------------------------
# Webhooks de retorno (outbox)
# ------------------------------------------------------------------

BACKOFF_WEBHOOK_SEGUNDOS = (5, 30, 120, 600, 3600)  # 5 tentativas no total
MAX_TENTATIVAS_WEBHOOK = len(BACKOFF_WEBHOOK_SEGUNDOS)


def adicionar_webhook(req_id: int, job_id: str, url: str, ref: Optional[str]) -> Optional[int]:
    """Registra um callback para o job. Ignora duplicata exata (mesmo job+url+ref)."""
    with get_conn() as conn:
        existe = conn.execute(
            "SELECT id FROM webhooks WHERE req_id = ? AND url = ? AND COALESCE(ref, '') = COALESCE(?, '')",
            (req_id, url, ref),
        ).fetchone()
        if existe:
            return None
        cur = conn.execute(
            "INSERT INTO webhooks (req_id, job_id, url, ref, status, criado_em) "
            "VALUES (?, ?, ?, ?, 'aguardando', ?)",
            (req_id, job_id, url, ref, datetime.utcnow().isoformat()),
        )
        return cur.lastrowid


def liberar_webhooks(req_id: int, payload_base: dict) -> int:
    """Job terminou: monta o payload de cada webhook aguardando e marca como pendente de envio."""
    agora = datetime.utcnow().isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, ref FROM webhooks WHERE req_id = ? AND status = 'aguardando'", (req_id,)
        ).fetchall()
        for r in rows:
            payload = {**payload_base, "ref": r["ref"]}
            conn.execute(
                "UPDATE webhooks SET status = 'pendente', payload_json = ?, proximo_envio = ? WHERE id = ?",
                (json.dumps(payload, ensure_ascii=False, default=str), agora, r["id"]),
            )
        return len(rows)


def liberar_webhooks_orfaos() -> int:
    """
    Boot: webhooks ainda 'aguardando' cujo job ja foi finalizado no banco
    (processo caiu entre finalizar_requisicao e liberar_webhooks). Monta o
    payload a partir da linha da requisicao (versao resumida: so uuid).
    """
    with get_conn() as conn:
        reqs = conn.execute(
            "SELECT DISTINCT r.* FROM requisicoes r JOIN webhooks w ON w.req_id = r.id "
            "WHERE w.status = 'aguardando' AND r.status NOT IN ('na_fila', 'processando')"
        ).fetchall()
    total = 0
    for r in reqs:
        r = dict(r)
        resumo = json.loads(r["resultado_resumo"]) if r.get("resultado_resumo") else {}
        ok = r["status"] == "autorizado"
        base = {
            "job_id": r["job_id"], "cpf": r["cpf"], "status": "ok" if ok else "falha",
            "uuid": resumo.get("uuid"), "resultado": None,
            "tipo_erro": r.get("tipo_erro"), "detalhe_erro": r.get("detalhe_erro"),
            "resultado_completo": False,
        }
        total += liberar_webhooks(r["id"], base)
    return total


def webhooks_para_enviar(limite: int = 50) -> list[dict]:
    agora = datetime.utcnow().isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM webhooks WHERE status = 'pendente' AND proximo_envio <= ? "
            "ORDER BY proximo_envio LIMIT ?",
            (agora, limite),
        ).fetchall()
        return [dict(r) for r in rows]


def marcar_webhook_entregue(webhook_id: int, obs: Optional[str] = None,
                            status_http: Optional[int] = None, resposta: Optional[str] = None):
    """`obs` guarda um aviso (ex: entregue sem confirmacao por timeout) em ultimo_erro."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE webhooks SET status = 'entregue', entregue_em = ?, tentativas = tentativas + 1, "
            "ultimo_erro = ?, status_http = ?, resposta = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), obs, status_http, (resposta or "")[:1000] or None, webhook_id),
        )


def marcar_webhook_falha(webhook_id: int, erro: str, definitivo: bool = False,
                         status_http: Optional[int] = None, resposta: Optional[str] = None):
    """
    Registra a falha. Reagenda com backoff, ou marca 'falhou' ao esgotar as
    tentativas OU quando `definitivo` (erro que reenviar nao resolve e pode
    rodar o fluxo do destino de novo - ex: HTTP 500 do n8n).
    """
    resposta = (resposta or "")[:1000] or None
    with get_conn() as conn:
        row = conn.execute("SELECT tentativas FROM webhooks WHERE id = ?", (webhook_id,)).fetchone()
        if not row:
            return
        tentativas = row["tentativas"] + 1
        if definitivo or tentativas >= MAX_TENTATIVAS_WEBHOOK:
            conn.execute(
                "UPDATE webhooks SET status = 'falhou', tentativas = ?, ultimo_erro = ?, status_http = ?, resposta = ? WHERE id = ?",
                (tentativas, erro[:300], status_http, resposta, webhook_id),
            )
        else:
            espera = BACKOFF_WEBHOOK_SEGUNDOS[tentativas - 1]
            proximo = (datetime.utcnow() + timedelta(seconds=espera)).isoformat()
            conn.execute(
                "UPDATE webhooks SET tentativas = ?, ultimo_erro = ?, proximo_envio = ?, status_http = ?, resposta = ? WHERE id = ?",
                (tentativas, erro[:300], proximo, status_http, resposta, webhook_id),
            )


def listar_webhooks(status: Optional[str] = None, limite: int = 50) -> list[dict]:
    with get_conn() as conn:
        query = "SELECT id, req_id, job_id, url, ref, status, tentativas, ultimo_erro, status_http, resposta, proximo_envio, criado_em, entregue_em FROM webhooks"
        params: list = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limite)
        return [dict(r) for r in conn.execute(query, params).fetchall()]


def contar_webhooks() -> dict:
    with get_conn() as conn:
        rows = conn.execute("SELECT status, COUNT(*) AS qtd FROM webhooks GROUP BY status").fetchall()
        return {r["status"]: r["qtd"] for r in rows}


def reenviar_webhooks_falhos() -> int:
    """Recoloca os 'falhou' na fila de envio (zera tentativas)."""
    agora = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE webhooks SET status = 'pendente', tentativas = 0, proximo_envio = ? "
            "WHERE status = 'falhou' AND payload_json IS NOT NULL",
            (agora,),
        )
        return cur.rowcount



# ------------------------------------------------------------------
# Saldo de trafego do proxy (snapshots)
# ------------------------------------------------------------------

def salvar_saldo(traffic_limit: Optional[int], traffic_usage: Optional[int], traffic_left: Optional[int]):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO proxy_saldo (consultado_em, traffic_limit, traffic_usage, traffic_left) VALUES (?, ?, ?, ?)",
            (datetime.utcnow().isoformat(), traffic_limit, traffic_usage, traffic_left),
        )
        # Mantem ~90 dias de historico (1 foto por hora = ~2200 linhas)
        conn.execute(
            "DELETE FROM proxy_saldo WHERE consultado_em < ?",
            ((datetime.utcnow() - timedelta(days=90)).isoformat(),),
        )


def ultimo_saldo() -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM proxy_saldo ORDER BY id DESC LIMIT 1").fetchone()
        return dict(row) if row else None


def saldos_desde(desde_iso: str) -> list[dict]:
    """Fotos a partir de `desde_iso`, mais a ultima anterior a ela (ponto de referencia da janela)."""
    with get_conn() as conn:
        antes = conn.execute(
            "SELECT * FROM proxy_saldo WHERE consultado_em < ? ORDER BY consultado_em DESC LIMIT 1", (desde_iso,)
        ).fetchone()
        rows = conn.execute(
            "SELECT * FROM proxy_saldo WHERE consultado_em >= ? ORDER BY consultado_em", (desde_iso,)
        ).fetchall()
        return ([dict(antes)] if antes else []) + [dict(r) for r in rows]


def contar_requisicoes_proxy_desde(desde_iso: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM requisicoes WHERE usou_proxy = 1 AND finalizado_em >= ?", (desde_iso,)
        ).fetchone()
        return row["n"] or 0



# ------------------------------------------------------------------
# Resultado final do fluxo (n8n -> POST /resultado) e funil do painel
# ------------------------------------------------------------------

def registrar_resultado(
    job_id: str, resultado: str, motivo: Optional[str] = None, banco: Optional[str] = None,
    margem: Optional[float] = None, valor_liquido_max: Optional[float] = None,
    detalhes: Optional[dict] = None,
) -> Optional[dict]:
    """Grava (ou substitui) o resultado do job. Devolve a requisicao ou None se o job nao existe."""
    req = get_requisicao_por_job(job_id)
    if not req:
        return None
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO resultados_fluxo (job_id, req_id, equipe, cpf, resultado, motivo, banco, margem, "
            "valor_liquido_max, detalhes_json, criado_em) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(job_id) DO UPDATE SET resultado=excluded.resultado, motivo=excluded.motivo, "
            "banco=excluded.banco, margem=excluded.margem, valor_liquido_max=excluded.valor_liquido_max, "
            "detalhes_json=excluded.detalhes_json, criado_em=excluded.criado_em",
            (
                job_id, req["id"], req["equipe"], req["cpf"], resultado, motivo, banco, margem, valor_liquido_max,
                json.dumps(detalhes, ensure_ascii=False, default=str)[:4000] if detalhes else None,
                datetime.utcnow().isoformat(),
            ),
        )
    return req


def stats_funil_hoje(equipe: Optional[str] = None) -> dict:
    """
    Funil do dia (horario de Brasilia): CPFs que entraram -> base atualizada ->
    resultado final do fluxo (pre-aprovado / nao aprovado por motivo / falha).
    """
    inicio, fim = _limites_dia_br_em_utc()
    filtro_eq = " AND equipe = ?" if equipe else ""
    p_eq = [equipe] if equipe else []
    with get_conn() as conn:
        entraram = conn.execute(
            f"SELECT COUNT(*) AS n FROM requisicoes WHERE criado_em >= ? AND criado_em < ?{filtro_eq}",
            [inicio, fim] + p_eq,
        ).fetchone()["n"]
        base_atualizada = conn.execute(
            f"SELECT COUNT(*) AS n FROM requisicoes WHERE criado_em >= ? AND criado_em < ? AND status = 'autorizado'{filtro_eq}",
            [inicio, fim] + p_eq,
        ).fetchone()["n"]
        # Base atualizada hoje mas o fluxo ainda nao reportou resultado (em andamento ou fluxo falhou)
        sem_resultado = conn.execute(
            "SELECT COUNT(*) AS n FROM requisicoes r WHERE r.criado_em >= ? AND r.criado_em < ? AND r.status = 'autorizado' "
            f"AND NOT EXISTS (SELECT 1 FROM resultados_fluxo f WHERE f.job_id = r.job_id){filtro_eq.replace('equipe', 'r.equipe')}",
            [inicio, fim] + p_eq,
        ).fetchone()["n"]
        rows = conn.execute(
            f"SELECT resultado, motivo, COUNT(*) AS n FROM resultados_fluxo WHERE criado_em >= ? AND criado_em < ?{filtro_eq} "
            "GROUP BY resultado, motivo ORDER BY n DESC",
            [inicio, fim] + p_eq,
        ).fetchall()
        pre = conn.execute(
            "SELECT COUNT(*) AS n, AVG(margem) AS margem_media, AVG(valor_liquido_max) AS valor_medio, "
            f"MAX(valor_liquido_max) AS valor_max FROM resultados_fluxo WHERE criado_em >= ? AND criado_em < ? "
            f"AND resultado = 'PRE_APROVADO'{filtro_eq}",
            [inicio, fim] + p_eq,
        ).fetchone()
        por_equipe = conn.execute(
            "SELECT equipe, resultado, COUNT(*) AS n FROM resultados_fluxo WHERE criado_em >= ? AND criado_em < ? "
            f"{filtro_eq} GROUP BY equipe, resultado",
            [inicio, fim] + p_eq,
        ).fetchall()

    por_resultado: dict = {}
    motivos: dict = {}
    for r in rows:
        por_resultado[r["resultado"]] = por_resultado.get(r["resultado"], 0) + r["n"]
        if r["resultado"] != "PRE_APROVADO":
            chave = f"{r['resultado']}|{r['motivo'] or 'sem_motivo'}"
            motivos[chave] = {"resultado": r["resultado"], "motivo": r["motivo"] or "sem_motivo", "qtd": r["n"]}
    total_resultados = sum(por_resultado.values())
    pre_n = pre["n"] or 0
    eq: dict = {}
    for r in por_equipe:
        eq.setdefault(r["equipe"], {})[r["resultado"]] = r["n"]
    return {
        "data": datetime.now(FUSO_BR).date().isoformat(),
        "entraram": entraram,
        "base_atualizada": base_atualizada,
        "com_resultado": total_resultados,
        "sem_resultado_ainda": sem_resultado,
        "por_resultado": por_resultado,
        "pre_aprovados": pre_n,
        "taxa_pre_aprovacao_pct": round(pre_n / total_resultados * 100, 1) if total_resultados else None,
        "margem_media_pre_aprovados": round(pre["margem_media"], 2) if pre["margem_media"] is not None else None,
        "valor_liquido_medio_pre_aprovados": round(pre["valor_medio"], 2) if pre["valor_medio"] is not None else None,
        "valor_liquido_maximo": round(pre["valor_max"], 2) if pre["valor_max"] is not None else None,
        "motivos": sorted(motivos.values(), key=lambda m: -m["qtd"]),
        "por_equipe": eq,
    }
