import asyncio
import ipaddress
import itertools
import logging
import os
import socket
import uuid as uuid_lib
from typing import Optional
from urllib.parse import urlparse

import httpx

import database
from facta_clt import simular_clt

logger = logging.getLogger("queue_worker")

_contador_sequencia = itertools.count()

# Quanto tempo um job ja finalizado continua na memoria (para quem esta
# esperando/consultando logo em seguida). Depois disso so existe no banco -
# evita o dicionario crescer para sempre com 30-40 mil CPFs por dia.
SEGUNDOS_JOB_FINALIZADO_EM_MEMORIA = 600

PERMITIR_REDE_PRIVADA = os.getenv("WEBHOOK_PERMITIR_REDE_PRIVADA", "false").lower() in ("1", "true", "yes")
WEBHOOK_TIMEOUT_SEGUNDOS = float(os.getenv("WEBHOOK_TIMEOUT_SEGUNDOS", "60"))
MAX_CONCORRENCIA = int(os.getenv("MAX_CONCORRENCIA", "500"))
STATUS_RETRY_WEBHOOK = {408, 425, 429, 502, 503, 504}


# ------------------------------------------------------------------
# Validacao de URL de webhook (evita usar o servico para bater em rede
# interna - SSRF). Chamada na entrada (sem DNS) e no envio (com DNS).
# ------------------------------------------------------------------

def _ip_bloqueado(ip: str) -> bool:
    try:
        a = ipaddress.ip_address(ip)
    except ValueError:
        return True
    return (
        a.is_private or a.is_loopback or a.is_link_local or a.is_reserved
        or a.is_multicast or a.is_unspecified
    )


def validar_url_webhook(url: str) -> str:
    """Levanta ValueError se a URL nao serve; devolve a URL normalizada (strip)."""
    url = (url or "").strip()
    if len(url) > 2000:
        raise ValueError("webhook_url muito longa")
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        raise ValueError("webhook_url deve comecar com http:// ou https://")
    host = (p.hostname or "").lower()
    if not host:
        raise ValueError("webhook_url sem host")
    if PERMITIR_REDE_PRIVADA:
        return url
    if host == "localhost" or host.endswith((".localhost", ".local", ".internal")):
        raise ValueError("webhook_url nao pode apontar para rede interna")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass  # nome de dominio - checado de novo (com DNS) na hora do envio
    else:
        if _ip_bloqueado(host):
            raise ValueError("webhook_url nao pode apontar para IP interno/reservado")
    return url


def _resolve_e_checa(url: str) -> None:
    """No envio: resolve o DNS e recusa se qualquer endereco for interno."""
    host = urlparse(url).hostname or ""
    porta = urlparse(url).port or (443 if url.startswith("https") else 80)
    for info in socket.getaddrinfo(host, porta, proto=socket.IPPROTO_TCP):
        if _ip_bloqueado(info[4][0]):
            raise ValueError(f"webhook_url resolve para endereco interno ({info[4][0]})")


class Job:
    def __init__(self, job_id: str, req_id: int, cpf: str, tentativas: int,
                 ref: Optional[str] = None):
        self.job_id = job_id
        self.req_id = req_id
        self.cpf = cpf
        self.tentativas = tentativas
        self.ref = ref
        self.status = "na_fila"  # na_fila | processando | sucesso | falha
        self.resultado: Optional[dict] = None
        self.evento = asyncio.Event()


class _Worker:
    """Estado de um worker: permite encerrar sem matar um job no meio."""

    def __init__(self):
        self.task: Optional[asyncio.Task] = None
        self.ocupado = False
        self.parar = False


def _payload_webhook(job: Job, resultado: dict) -> dict:
    ok = resultado.get("status") == "ok"
    tipo_erro = None
    if not ok:
        tipo_erro = resultado.get("tipo_erro") or (
            "erro_interno" if resultado.get("status") == "erro_interno" else "desconhecido"
        )
    return {
        "job_id": job.job_id,
        "cpf": job.cpf,
        "status": "ok" if ok else "falha",
        "uuid": resultado.get("uuid"),
        "tentativa": resultado.get("tentativa"),
        "resultado": resultado.get("resultado") if ok else None,
        "tipo_erro": tipo_erro,
        "detalhe_erro": None if ok else (resultado.get("detalhe_erro") or resultado.get("mensagem")),
        "resultado_completo": True,
    }


class FilaSimulacao:
    """
    Fila com prioridade + N workers concorrentes (padrao definido no banco,
    editavel via painel). Jobs de equipes com prioridade maior furam a fila
    na frente dos que ja estao esperando, sem alterar a capacidade total de
    processamento.

    A fila e persistida no banco (tabela requisicoes): no boot, o que estava
    'na_fila' ou 'processando' volta para a fila. Webhooks de retorno ficam
    numa outbox (tabela webhooks) entregue por uma task propria com retry.
    """

    def __init__(self):
        # item: (prioridade_invertida, sequencia, job) - PriorityQueue e min-heap,
        # entao usamos -prioridade para que maior prioridade saia primeiro; a
        # sequencia garante ordem FIFO dentro do mesmo nivel de prioridade.
        self._fila: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._jobs: dict[str, Job] = {}
        self._cpfs_ativos: dict[str, str] = {}  # cpf -> job_id (na_fila/processando)
        self._workers: list[_Worker] = []
        self._concorrencia_atual = 0
        self._lock = asyncio.Lock()
        self._entregador_task: Optional[asyncio.Task] = None
        # Pausa: com o evento "fechado" os workers nao pegam novos jobs (o que ja
        # esta rodando termina). A fila continua aceitando e guardando CPFs.
        self._liberado = asyncio.Event()
        self._liberado.set()

    def pausado(self) -> bool:
        return not self._liberado.is_set()

    def definir_pausa(self, pausar: bool):
        database.set_config("pausado", "1" if pausar else "0")
        if pausar:
            self._liberado.clear()
        else:
            self._liberado.set()
        logger.warning("Fila PAUSADA" if pausar else "Fila RETOMADA")

    async def iniciar(self):
        if database.get_config("pausado", "0") == "1":
            self._liberado.clear()
            logger.warning("Fila inicia PAUSADA (estado salvo)")
        recuperados = self.recuperar_pendentes()
        liberados = database.liberar_webhooks_orfaos()
        if recuperados or liberados:
            logger.info(f"Recuperacao no boot: {recuperados} job(s) de volta na fila, {liberados} webhook(s) liberado(s)")
        concorrencia = int(database.get_config("concorrencia", "5"))
        await self._ajustar_workers(concorrencia)
        if self._entregador_task is None or self._entregador_task.done():
            self._entregador_task = asyncio.create_task(self._entregador())
        logger.info(f"Fila iniciada com concorrencia={concorrencia}")

    def recuperar_pendentes(self) -> int:
        """Recoloca na fila o que ficou pendente no banco (restart/deploy)."""
        database.voltar_processando_para_fila()
        total = 0
        for r in database.listar_pendentes_fila():
            if r["job_id"] in self._jobs:
                continue
            if r["cpf"] in self._cpfs_ativos:
                # Duplicata antiga no banco: ja existe um job ativo para esse CPF.
                database.finalizar_requisicao(
                    r["id"], status="falha", tipo_erro="duplicada",
                    detalhe_erro="CPF ja estava na fila na recuperacao do boot",
                )
                database.liberar_webhooks(r["id"], {
                    "job_id": r["job_id"], "cpf": r["cpf"], "status": "falha", "uuid": None,
                    "resultado": None, "tipo_erro": "duplicada",
                    "detalhe_erro": "CPF ja estava na fila na recuperacao do boot",
                    "resultado_completo": True,
                })
                continue
            job = Job(r["job_id"], r["id"], r["cpf"], r["tentativas_max"], r["ref"])
            self._jobs[job.job_id] = job
            self._cpfs_ativos[job.cpf] = job.job_id
            self._fila.put_nowait((-(r["prioridade"] or 0), next(_contador_sequencia), job))
            total += 1
        return total

    async def _ajustar_workers(self, novo_total: int):
        async with self._lock:
            atual = len(self._workers)
            if novo_total > atual:
                for i in range(atual, novo_total):
                    estado = _Worker()
                    estado.task = asyncio.create_task(self._worker(i, estado))
                    self._workers.append(estado)
            elif novo_total < atual:
                for _ in range(atual - novo_total):
                    estado = self._workers.pop()
                    if estado.ocupado:
                        # Termina o job em andamento e so entao sai - cancelar
                        # no meio deixava o job preso em 'processando'.
                        estado.parar = True
                    else:
                        estado.task.cancel()
            self._concorrencia_atual = novo_total

    async def atualizar_concorrencia(self, novo_total: int):
        # Teto alinhado com o limite validado em main.py (ConcorrenciaRequest,
        # le=150) - antes esse cap interno de 50 sobrescrevia silenciosamente
        # qualquer valor maior enviado pelo painel/API.
        novo_total = max(1, min(novo_total, MAX_CONCORRENCIA))
        database.set_config("concorrencia", str(novo_total))
        await self._ajustar_workers(novo_total)
        return novo_total

    def concorrencia_atual(self) -> int:
        return self._concorrencia_atual

    def tamanho_fila(self) -> int:
        return self._fila.qsize()

    async def enfileirar(
        self, equipe: str, cpf: str, tentativas: int = 5, prioridade: int = 0,
        webhook_url: Optional[str] = None, ref: Optional[str] = None,
    ) -> tuple[str, bool]:
        """
        Retorna (job_id, ja_existia). Se o CPF ja estiver na fila ou sendo
        processado, devolve o job_id existente em vez de criar duplicata -
        evita gastar chamadas (e rate limit) repetidas para o mesmo CPF. Se
        vier webhook_url nessa chamada duplicada, ele tambem e registrado no
        job existente (todos os chamadores sao avisados quando terminar).
        `prioridade` maior fura a frente dos jobs de prioridade menor ja
        esperando na fila.
        """
        job_existente = self._cpfs_ativos.get(cpf)
        if job_existente and job_existente in self._jobs:
            job = self._jobs[job_existente]
            if job.status in ("na_fila", "processando"):
                if webhook_url:
                    database.adicionar_webhook(job.req_id, job.job_id, webhook_url, ref)
                return job_existente, True

        job_id = uuid_lib.uuid4().hex[:12]
        req_id = database.registrar_requisicao_criada(
            equipe, cpf, job_id, tentativas_max=tentativas, prioridade=prioridade, ref=ref
        )
        if webhook_url:
            database.adicionar_webhook(req_id, job_id, webhook_url, ref)
        job = Job(job_id, req_id, cpf, tentativas, ref)
        self._jobs[job_id] = job
        self._cpfs_ativos[cpf] = job_id
        seq = next(_contador_sequencia)
        await self._fila.put((-prioridade, seq, job))
        return job_id, False

    def get_job(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def consultar_job(self, job_id: str) -> Optional[dict]:
        """Status do job: memoria se ainda estiver la, senao o que o banco guardou."""
        job = self._jobs.get(job_id)
        if job:
            return {"job_id": job.job_id, "status": job.status, "resultado": job.resultado}
        r = database.get_requisicao_por_job(job_id)
        if not r:
            return None
        if r["status"] in ("na_fila", "processando"):
            return {"job_id": job_id, "status": r["status"], "resultado": None}
        if r["status"] == "autorizado":
            resumo = {}
            if r.get("resultado_resumo"):
                import json
                resumo = json.loads(r["resultado_resumo"])
            return {"job_id": job_id, "status": "sucesso",
                    "resultado": {"status": "ok", "uuid": resumo.get("uuid"), "resultado_completo": False}}
        return {"job_id": job_id, "status": "falha",
                "resultado": {"status": "falha", "tipo_erro": r.get("tipo_erro"),
                              "detalhe_erro": r.get("detalhe_erro")}}

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    async def _worker(self, worker_num: int, estado: _Worker):
        logger.info(f"Worker {worker_num} iniciado")
        try:
            while True:
                await self._liberado.wait()
                item = await self._fila.get()
                if not self._liberado.is_set():  # pausou enquanto esperava o job: devolve
                    self._fila.put_nowait(item)
                    self._fila.task_done()
                    continue
                _prioridade_invertida, _seq, job = item
                estado.ocupado = True
                try:
                    await self._processar(job)
                finally:
                    estado.ocupado = False
                    self._fila.task_done()
                if estado.parar:
                    break
        except asyncio.CancelledError:
            logger.info(f"Worker {worker_num} finalizado")
            raise
        logger.info(f"Worker {worker_num} finalizado (apos concluir o job em andamento)")

    async def _processar(self, job: Job):
        job.status = "processando"
        database.marcar_processando(job.req_id)
        try:
            resultado = await asyncio.to_thread(simular_clt, job.cpf, job.tentativas)
            job.resultado = resultado
            if resultado["status"] == "ok":
                job.status = "sucesso"
                database.finalizar_requisicao(
                    job.req_id,
                    status="autorizado",
                    tentativas=resultado.get("tentativa"),
                    resultado_resumo={"uuid": resultado.get("uuid")},
                    chamadas_realizadas=resultado.get("chamadas_realizadas"),
                    usou_proxy=resultado.get("usou_proxy", False),
                    caiu_para_direto=resultado.get("caiu_para_direto", False),
                )
            else:
                job.status = "falha"
                tipo_erro = resultado.get("tipo_erro", "desconhecido")
                if tipo_erro in ("bloqueio_ip", "rate_limit", "sem_retorno", "erro_proxy"):
                    status_final = tipo_erro
                else:
                    status_final = "falha"
                database.finalizar_requisicao(
                    job.req_id,
                    status=status_final,
                    tipo_erro=tipo_erro,
                    detalhe_erro=resultado.get("detalhe_erro"),
                    chamadas_realizadas=resultado.get("chamadas_realizadas"),
                    usou_proxy=resultado.get("usou_proxy", False),
                    caiu_para_direto=resultado.get("caiu_para_direto", False),
                )
                if tipo_erro == "bloqueio_ip":
                    logger.warning(
                        f"BLOQUEIO DE IP detectado para cpf={job.cpf}: "
                        f"{resultado.get('detalhe_erro')}"
                    )
                elif tipo_erro == "rate_limit":
                    logger.info(
                        f"Rate limit (throttle esperado) para cpf={job.cpf}: "
                        f"{resultado.get('detalhe_erro')}"
                    )
                elif tipo_erro == "erro_proxy":
                    logger.warning(
                        f"Falha de PROXY (nao da Facta) para cpf={job.cpf}: "
                        f"{resultado.get('detalhe_erro')}"
                    )
        except Exception as e:
            job.status = "falha"
            job.resultado = {"status": "erro_interno", "mensagem": str(e)}
            database.finalizar_requisicao(
                job.req_id, status="erro_interno", tipo_erro="erro_interno", detalhe_erro=str(e)
            )
            logger.exception(f"Erro processando job {job.job_id}")
        finally:
            # Sem await entre a finalizacao e a liberacao dos webhooks: um
            # enfileirar() concorrente nao consegue entrar no meio.
            try:
                database.liberar_webhooks(job.req_id, _payload_webhook(job, job.resultado or {}))
            except Exception:
                logger.exception(f"Falha ao liberar webhooks do job {job.job_id}")
            if self._cpfs_ativos.get(job.cpf) == job.job_id:
                del self._cpfs_ativos[job.cpf]
            job.evento.set()
            asyncio.get_running_loop().call_later(
                SEGUNDOS_JOB_FINALIZADO_EM_MEMORIA, self._jobs.pop, job.job_id, None
            )

    # ------------------------------------------------------------------
    # Entrega de webhooks (outbox persistida, com retry/backoff)
    # ------------------------------------------------------------------

    async def _entregador(self):
        logger.info("Entregador de webhooks iniciado")
        timeout = httpx.Timeout(10.0, read=WEBHOOK_TIMEOUT_SEGUNDOS, pool=WEBHOOK_TIMEOUT_SEGUNDOS)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
            while True:
                try:
                    pendentes = database.webhooks_para_enviar(50)
                    if pendentes:
                        await asyncio.gather(*(self._enviar_webhook(client, w) for w in pendentes))
                    else:
                        await asyncio.sleep(2)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Erro no entregador de webhooks")
                    await asyncio.sleep(5)

    async def _enviar_webhook(self, client: httpx.AsyncClient, w: dict):
        """
        Regras de reenvio (o destino costuma ser um fluxo do n8n que EXECUTA algo a
        cada POST - reenviar sem necessidade roda o fluxo duas vezes):
          - 2xx                      -> entregue.
          - ReadTimeout (n8n recebeu, so nao respondeu a tempo) -> entregue (com aviso), SEM reenviar.
          - erro de conexao / 408 / 425 / 429 / 502 / 503 / 504 -> retry com backoff.
          - qualquer outro non-2xx (500, 404, 400...) -> 'falhou' definitivo (reenvio manual no painel).
        """
        try:
            if not PERMITIR_REDE_PRIVADA:
                await asyncio.to_thread(_resolve_e_checa, w["url"])
            resp = await client.post(
                w["url"],
                content=w["payload_json"],
                headers={"Content-Type": "application/json", "User-Agent": "facta-clt-webhook/1.0"},
            )
            trecho = resp.text[:300]
            if 200 <= resp.status_code < 300:
                database.marcar_webhook_entregue(w["id"], status_http=resp.status_code, resposta=trecho)
            else:
                transitorio = resp.status_code in STATUS_RETRY_WEBHOOK
                database.marcar_webhook_falha(
                    w["id"], f"HTTP {resp.status_code}: {trecho[:150]}",
                    definitivo=not transitorio, status_http=resp.status_code, resposta=trecho,
                )
        except httpx.ReadTimeout:
            logger.warning(f"Webhook {w['id']} (job {w['job_id']}): sem resposta em {WEBHOOK_TIMEOUT_SEGUNDOS:.0f}s; "
                           "considerado entregue (nao reenviado para nao duplicar a execucao)")
            database.marcar_webhook_entregue(
                w["id"], obs=f"sem resposta em {WEBHOOK_TIMEOUT_SEGUNDOS:.0f}s (provavelmente recebido; nao reenviado)")
        except Exception as e:
            logger.warning(f"Webhook {w['id']} (job {w['job_id']}) falhou: {e}")
            database.marcar_webhook_falha(w["id"], f"{type(e).__name__}: {e}")


fila_global = FilaSimulacao()
