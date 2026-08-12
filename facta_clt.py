"""
Replica o fluxo n8n: Code4 (gera dados aleatorios) -> TENTATIVAS3 (5x)
-> autorizar facta -> simular
"""
import random
import unicodedata
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import httpx

logger = logging.getLogger("facta_clt")

BASE_URL = "https://api-autocontratacao.factafinanceira.com.br"
URL_AUTORIZAR = f"{BASE_URL}/clt/analisa-cliente"
URL_SIMULAR = f"{BASE_URL}/clt/simulando"
ORIGIN = "https://autocontratacao.factafinanceira.com.br"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"

DDD_MAP = {
    "0": "51", "1": "61", "2": "92", "3": "85", "4": "81",
    "5": "71", "6": "31", "7": "21", "8": "11", "9": "48",
}

NOMES = ['Carlos', 'Ana', 'Paulo', 'Fernanda', 'Ricardo', 'Juliana', 'Marcos',
         'Patricia', 'Bruno', 'Camila', 'Diego', 'Aline', 'Rafael', 'Tatiane',
         'Leandro', 'Vanessa', 'Thiago', 'Priscila', 'Rodrigo', 'Debora']
SOBRENOMES = ['Silva', 'Santos', 'Oliveira', 'Souza', 'Lima', 'Pereira', 'Costa',
              'Ferreira', 'Rodrigues', 'Alves', 'Nascimento', 'Carvalho',
              'Mendes', 'Barbosa', 'Moreira']
PROVEDORES = ['gmail.com', 'hotmail.com', 'yahoo.com.br', 'outlook.com', 'icloud.com']
SEPARADORES = ['.', '_', '']


def get_random_phone_number() -> str:
    segundo = random.randint(7, 9)
    restante = random.randint(1000000, 9999999)
    return f"9{segundo}{restante}"


def get_ddd_from_cpf(cpf: str) -> str:
    digito = cpf[8]
    return DDD_MAP.get(digito, "11")


def get_random_name() -> Tuple[str, str]:
    return random.choice(NOMES), random.choice(SOBRENOMES)


def remove_diacritics(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    stripped = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    return "".join(c for c in stripped if c.isalpha()).lower()


def get_random_email(nome: str, sobrenome: str) -> str:
    provedor = random.choice(PROVEDORES)
    n = remove_diacritics(nome)
    s = remove_diacritics(sobrenome)
    sep = random.choice(SEPARADORES)
    numero = random.randint(1, 999)
    return f"{n}{sep}{s}{numero}@{provedor}"


def get_random_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def format_phone(ddd: str, tel: str) -> str:
    return f"({ddd}) {tel[:5]}-{tel[5:]}"


@dataclass
class DadosGerados:
    nome_completo: str
    email: str
    telefone_formatado: str
    ip: str


def gerar_dados(cpf: str) -> DadosGerados:
    ddd = get_ddd_from_cpf(cpf)
    nome, sobrenome = get_random_name()
    telefone = get_random_phone_number()
    email = get_random_email(nome, sobrenome)
    ip = get_random_ip()
    telefone_formatado = format_phone(ddd, telefone)
    return DadosGerados(
        nome_completo=f"{nome} {sobrenome}",
        email=email,
        telefone_formatado=telefone_formatado,
        ip=ip,
    )


class FactaCltError(Exception):
    pass


# Limitador de taxa global para chamadas a Facta - evita disparar varias
# requisicoes quase simultaneas mesmo com concorrencia > 1 na fila, que
# estava causando 429 "Numero excessivo de requisicoes" no lado da Facta.
_ultimo_disparo = 0.0
_lock_rate_limit = threading.Lock()
INTERVALO_MINIMO_SEGUNDOS = float(os.getenv("FACTA_INTERVALO_MINIMO_SEGUNDOS", "1.2"))


def _aguardar_rate_limit():
    global _ultimo_disparo
    with _lock_rate_limit:
        agora = time.monotonic()
        espera = INTERVALO_MINIMO_SEGUNDOS - (agora - _ultimo_disparo)
        if espera > 0:
            time.sleep(espera)
        _ultimo_disparo = time.monotonic()


# Codigos/paravras que sinalizam possivel bloqueio de IP ou rate-limit,
# em vez de erro generico de rede. Usado so para classificar e alertar,
# nao para tentar contornar.
STATUS_BLOQUEIO = {403, 429}
PALAVRAS_BLOQUEIO = ("blocked", "bloqueado", "rate limit", "too many requests", "forbidden")


@dataclass
class TentativaResultado:
    ok: bool
    tipo_erro: Optional[str] = None  # "bloqueio_suspeito" | "erro_rede" | "sem_uuid" | None
    detalhe: Optional[str] = None
    status_code: Optional[int] = None


def _classificar_erro(e: Exception) -> TentativaResultado:
    status_code = None
    detalhe = str(e)
    if isinstance(e, httpx.HTTPStatusError):
        status_code = e.response.status_code
        detalhe = f"HTTP {status_code}: {e.response.text[:200]}"
        if status_code in STATUS_BLOQUEIO:
            return TentativaResultado(ok=False, tipo_erro="bloqueio_suspeito", detalhe=detalhe, status_code=status_code)
        corpo_lower = e.response.text.lower()
        if any(p in corpo_lower for p in PALAVRAS_BLOQUEIO):
            return TentativaResultado(ok=False, tipo_erro="bloqueio_suspeito", detalhe=detalhe, status_code=status_code)
    return TentativaResultado(ok=False, tipo_erro="erro_rede", detalhe=detalhe, status_code=status_code)


def autorizar_facta(client: httpx.Client, cpf: str, dados: DadosGerados) -> Tuple[Optional[dict], Optional[TentativaResultado]]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Origin": ORIGIN,
        "Referer": f"{ORIGIN}/",
    }
    body = {
        "nome": dados.nome_completo,
        "cpf": cpf,
        "celular": dados.telefone_formatado,
        "email": dados.email,
        "termos": "S",
        "ip": dados.ip,
    }
    try:
        _aguardar_rate_limit()
        resp = client.post(URL_AUTORIZAR, headers=headers, data=body, timeout=30)
        resp.raise_for_status()
        return resp.json(), None
    except Exception as e:
        erro = _classificar_erro(e)
        logger.warning(f"Erro em autorizar facta [{erro.tipo_erro}]: {erro.detalhe}")
        return None, erro


def simular_uuid(client: httpx.Client, uuid: str) -> Tuple[Optional[dict], Optional[TentativaResultado]]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Origin": ORIGIN,
        "Referer": f"{ORIGIN}/",
    }
    body = {"uuid": uuid}
    try:
        _aguardar_rate_limit()
        resp = client.post(URL_SIMULAR, headers=headers, data=body, timeout=30)
        resp.raise_for_status()
        return resp.json(), None
    except Exception as e:
        erro = _classificar_erro(e)
        logger.warning(f"Erro em simular [{erro.tipo_erro}]: {erro.detalhe}")
        return None, erro


def simular_clt(cpf: str, tentativas: int = 5) -> dict:
    """
    Executa o fluxo completo (sincrono, roda em thread via asyncio.to_thread):
    gera dados aleatorios uma unica vez, tenta autorizar + simular ate
    `tentativas` vezes. Retorna dict com status/resultado e detalhe de erro
    classificado (util para o log/dashboard identificar bloqueio de IP x
    falha de rede pontual x CPF sem uuid).
    """
    dados = gerar_dados(cpf)
    logger.info(f"Dados gerados: nome={dados.nome_completo} email={dados.email} tel={dados.telefone_formatado}")

    ultimo_erro: Optional[TentativaResultado] = None

    with httpx.Client() as client:
        for i in range(1, tentativas + 1):
            logger.info(f"--- Tentativa {i} de {tentativas} ---")

            aut_resp, erro = autorizar_facta(client, cpf, dados)
            if aut_resp is None:
                ultimo_erro = erro
                if erro and erro.tipo_erro == "bloqueio_suspeito":
                    # Nao adianta insistir se o sinal e de bloqueio/rate-limit;
                    # aborta as tentativas restantes e reporta pra ficar visivel.
                    break
                continue

            uuid = aut_resp.get("uuid")
            if not uuid:
                ultimo_erro = TentativaResultado(ok=False, tipo_erro="sem_uuid", detalhe=str(aut_resp)[:200])
                logger.info(f"Resposta sem uuid: {aut_resp}")
                continue

            logger.info(f"UUID obtido: {uuid}")

            sim_resp, erro = simular_uuid(client, uuid)
            if sim_resp is not None:
                return {
                    "status": "ok",
                    "resultado": sim_resp,
                    "uuid": uuid,
                    "tentativa": i,
                    "dados_gerados": {
                        "nome": dados.nome_completo,
                        "email": dados.email,
                        "telefone": dados.telefone_formatado,
                        "ip": dados.ip,
                    },
                }
            ultimo_erro = erro
            if erro and erro.tipo_erro == "bloqueio_suspeito":
                break

    return {
        "status": "falha",
        "mensagem": f"Nenhuma tentativa retornou resultado apos {tentativas} tentativas.",
        "tipo_erro": ultimo_erro.tipo_erro if ultimo_erro else "desconhecido",
        "detalhe_erro": ultimo_erro.detalhe if ultimo_erro else None,
    }
