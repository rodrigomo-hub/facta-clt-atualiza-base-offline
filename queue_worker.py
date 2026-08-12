import asyncio
import logging
import uuid as uuid_lib
from typing import Optional

import database
from facta_clt import simular_clt

logger = logging.getLogger("queue_worker")


class Job:
    def __init__(self, job_id: str, req_id: int, cpf: str, tentativas: int):
        self.job_id = job_id
        self.req_id = req_id
        self.cpf = cpf
        self.tentativas = tentativas
        self.status = "na_fila"  # na_fila | processando | sucesso | falha
        self.resultado: Optional[dict] = None
        self.evento = asyncio.Event()


class FilaSimulacao:
    """
    Fila com N workers concorrentes (padrao definido no banco, editavel via
    painel). Evita disparar todas as simulacoes de uma vez (que e o que
    causava sobrecarga vindo do n8n).
    """

    def __init__(self):
        self._fila: asyncio.Queue[Job] = asyncio.Queue()
        self._jobs: dict[str, Job] = {}
        self._cpfs_ativos: dict[str, str] = {}  # cpf -> job_id (na_fila/processando)
        self._workers: list[asyncio.Task] = []
        self._concorrencia_atual = 0
        self._lock = asyncio.Lock()

    async def iniciar(self):
        concorrencia = int(database.get_config("concorrencia", "5"))
        await self._ajustar_workers(concorrencia)
        logger.info(f"Fila iniciada com concorrencia={concorrencia}")

    async def _ajustar_workers(self, novo_total: int):
        async with self._lock:
            atual = len(self._workers)
            if novo_total > atual:
                for i in range(atual, novo_total):
                    task = asyncio.create_task(self._worker(i))
                    self._workers.append(task)
            elif novo_total < atual:
                for _ in range(atual - novo_total):
                    task = self._workers.pop()
                    task.cancel()
            self._concorrencia_atual = novo_total

    async def atualizar_concorrencia(self, novo_total: int):
        novo_total = max(1, min(novo_total, 50))
        database.set_config("concorrencia", str(novo_total))
        await self._ajustar_workers(novo_total)
        return novo_total

    def concorrencia_atual(self) -> int:
        return self._concorrencia_atual

    def tamanho_fila(self) -> int:
        return self._fila.qsize()

    async def enfileirar(self, equipe: str, cpf: str, tentativas: int = 5) -> tuple[str, bool]:
        """
        Retorna (job_id, ja_existia). Se o CPF ja estiver na fila ou sendo
        processado, devolve o job_id existente em vez de criar duplicata -
        evita gastar chamadas (e rate limit) repetidas para o mesmo CPF.
        """
        job_existente = self._cpfs_ativos.get(cpf)
        if job_existente and job_existente in self._jobs:
            job = self._jobs[job_existente]
            if job.status in ("na_fila", "processando"):
                return job_existente, True

        job_id = uuid_lib.uuid4().hex[:12]
        req_id = database.registrar_requisicao_criada(equipe, cpf, job_id)
        job = Job(job_id, req_id, cpf, tentativas)
        self._jobs[job_id] = job
        self._cpfs_ativos[cpf] = job_id
        await self._fila.put(job)
        return job_id, False

    def get_job(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    async def _worker(self, worker_num: int):
        logger.info(f"Worker {worker_num} iniciado")
        try:
            while True:
                job = await self._fila.get()
                job.status = "processando"
                try:
                    resultado = await asyncio.to_thread(
                        simular_clt, job.cpf, job.tentativas
                    )
                    job.resultado = resultado
                    if resultado["status"] == "ok":
                        job.status = "sucesso"
                        database.finalizar_requisicao(
                            job.req_id,
                            status="autorizado",
                            tentativas=resultado.get("tentativa"),
                            resultado_resumo={"uuid": resultado.get("uuid")},
                            chamadas_realizadas=resultado.get("chamadas_realizadas"),
                        )
                    else:
                        job.status = "falha"
                        tipo_erro = resultado.get("tipo_erro", "desconhecido")
                        if tipo_erro == "bloqueio_suspeito":
                            status_final = "bloqueio_suspeito"
                        elif tipo_erro == "sem_retorno":
                            status_final = "sem_retorno"
                        else:
                            status_final = "falha"
                        database.finalizar_requisicao(
                            job.req_id,
                            status=status_final,
                            tipo_erro=tipo_erro,
                            detalhe_erro=resultado.get("detalhe_erro"),
                            chamadas_realizadas=resultado.get("chamadas_realizadas"),
                        )
                        if tipo_erro == "bloqueio_suspeito":
                            logger.warning(
                                f"POSSIVEL BLOQUEIO detectado para cpf={job.cpf}: "
                                f"{resultado.get('detalhe_erro')}"
                            )
                except Exception as e:
                    job.status = "falha"
                    job.resultado = {"status": "erro_interno", "mensagem": str(e)}
                    database.finalizar_requisicao(
                        job.req_id, status="erro_interno", detalhe_erro=str(e)
                    )
                    logger.exception(f"Erro processando job {job.job_id}")
                finally:
                    if self._cpfs_ativos.get(job.cpf) == job.job_id:
                        del self._cpfs_ativos[job.cpf]
                    job.evento.set()
                    self._fila.task_done()
        except asyncio.CancelledError:
            logger.info(f"Worker {worker_num} finalizado")
            raise


fila_global = FilaSimulacao()
