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

# ------------------------------------------------------------------
# Proxy residencial (Proxy-Seller) - usado para variar o IP de saida
# das chamadas a Facta e evitar 429 (rate limit por IP) sob volume alto.
# Config vem do banco (tabela `config`, editavel via /admin/config/proxy
# sem redeploy) - as env vars FACTA_* servem so de fallback/bootstrap
# inicial, caso o banco ainda nao tenha sido configurado.
#
# A lista no painel do proxy foi deixada em modo "novo IP a cada
# requisicao" (testamos rotacao por tempo mas o campo nao respondeu como
# esperado - na pratica cada chamada HTTP sai de um IP diferente). Isso
# significa que autorizar_facta e simular_uuid do MESMO cpf podem sair de
# IPs diferentes entre si - ainda nao confirmado se a Facta aceita isso
# sem problema; ver campo "usou_proxy"/"caiu_para_direto" no dashboard
# para acompanhar se isso gera mais `sem_retorno` do que o esperado.
# ------------------------------------------------------------------

def get_proxy_config() -> dict:
    """
    Le config de proxy do banco. Se as chaves ainda nao foram configuradas
    la (banco recem criado), cai para as env vars FACTA_* como fallback -
    isso evita que o proxy simplesmente "desligue" na primeira migracao
    de quem ja tinha env vars configuradas antes dessa mudanca.
    """
    import database

    cfg = database.get_proxy_config()
    if not cfg["proxy_host"] and not cfg["proxy_user"]:
        # Nada configurado no banco ainda - tenta env vars como fallback.
        cfg = {
            "usar_proxy": os.getenv("FACTA_USAR_PROXY", "false").lower() in ("1", "true", "yes"),
            "proxy_host": os.getenv("FACTA_PROXY_HOST", ""),
            "proxy_port": os.getenv("FACTA_PROXY_PORT", ""),
            "proxy_user": os.getenv("FACTA_PROXY_USER", ""),
            "proxy_pass": os.getenv("FACTA_PROXY_PASS", ""),
        }
    return cfg


def _montar_proxy_url() -> Optional[str]:
    cfg = get_proxy_config()
    if not cfg["usar_proxy"] or not (cfg["proxy_host"] and cfg["proxy_port"] and cfg["proxy_user"] and cfg["proxy_pass"]):
        return None
    return f"http://{cfg['proxy_user']}:{cfg['proxy_pass']}@{cfg['proxy_host']}:{cfg['proxy_port']}"

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
#
# So faz sentido quando as chamadas saem todas do MESMO IP (sem proxy) -
# com proxy ligado, cada chamada pode sair de um IP diferente, entao esse
# espacamento fixo global so limitaria o throughput a toa sem necessidade
# (o rate limit da Facta e por IP, nao por conta/servico). Por isso o
# espacamento so e aplicado quando o proxy esta desligado.
_ultimo_disparo = 0.0
_lock_rate_limit = threading.Lock()
INTERVALO_MINIMO_SEGUNDOS = float(os.getenv("FACTA_INTERVALO_MINIMO_SEGUNDOS", "1.2"))


def _aguardar_rate_limit():
    if _montar_proxy_url():
        # Proxy ligado: cada chamada tende a sair de IP diferente, entao o
        # rate limit por IP da Facta nao se acumula do mesmo jeito - nao
        # aplica o espacamento fixo global.
        return
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
STATUS_BLOQUEIO_IP = {403}  # bloqueio real (ex: Cloudflare "Just a moment")
STATUS_RATE_LIMIT = {429}  # throttle esperado sob volume, nao e bloqueio
PALAVRAS_BLOQUEIO_IP = ("blocked", "bloqueado", "forbidden", "just a moment")
PALAVRAS_RATE_LIMIT = ("rate limit", "too many requests", "excessivo")


@dataclass
class TentativaResultado:
    ok: bool
    tipo_erro: Optional[str] = None  # "bloqueio_ip" | "rate_limit" | "erro_rede" | "erro_proxy" | "sem_uuid" | "sem_retorno" | None
    detalhe: Optional[str] = None
    status_code: Optional[int] = None


def _classificar_erro(e: Exception) -> TentativaResultado:
    status_code = None
    detalhe = str(e)

    # Falha na conexao com o proxy em si (gateway caiu, timeout ao trocar de
    # IP, etc) - diferente de erro de rede generico ou de bloqueio da Facta.
    # Importante distinguir para o dashboard mostrar se o gargalo e o
    # provedor de proxy ou a propria Facta.
    if isinstance(e, (httpx.ProxyError, httpx.ConnectTimeout, httpx.ConnectError)) and _montar_proxy_url():
        return TentativaResultado(ok=False, tipo_erro="erro_proxy", detalhe=detalhe, status_code=None)

    if isinstance(e, httpx.HTTPStatusError):
        status_code = e.response.status_code
        detalhe = f"HTTP {status_code}: {e.response.text[:200]}"
        corpo_lower = e.response.text.lower()

        if status_code in STATUS_BLOQUEIO_IP or any(p in corpo_lower for p in PALAVRAS_BLOQUEIO_IP):
            return TentativaResultado(ok=False, tipo_erro="bloqueio_ip", detalhe=detalhe, status_code=status_code)

        if status_code in STATUS_RATE_LIMIT or any(p in corpo_lower for p in PALAVRAS_RATE_LIMIT):
            return TentativaResultado(ok=False, tipo_erro="rate_limit", detalhe=detalhe, status_code=status_code)

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
        resp = client.post(URL_AUTORIZAR, headers=headers, data=body, timeout=None)
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
        resp = client.post(URL_SIMULAR, headers=headers, data=body, timeout=None)
        resp.raise_for_status()
        if not resp.text.strip():
            # Facta as vezes responde 200 com corpo vazio - nao e erro de
            # rede, e uma resposta valida (porem sem dado). Classifica
            # separado para nao ficar reinsistindo como se fosse falha de
            # conexao.
            return None, TentativaResultado(
                ok=False, tipo_erro="sem_retorno", detalhe="HTTP 200 com corpo vazio", status_code=200
            )
        return resp.json(), None
    except Exception as e:
        erro = _classificar_erro(e)
        logger.warning(f"Erro em simular [{erro.tipo_erro}]: {erro.detalhe}")
        return None, erro


def _novo_client(usar_proxy: bool) -> httpx.Client:
    proxy_url = _montar_proxy_url() if usar_proxy else None
    return httpx.Client(proxy=proxy_url)


def simular_clt(cpf: str, tentativas: int = 5) -> dict:
    """
    Executa o fluxo completo (sincrono, roda em thread via asyncio.to_thread):
    gera dados aleatorios uma unica vez, tenta autorizar + simular ate
    `tentativas` vezes. Retorna dict com status/resultado e detalhe de erro
    classificado (util para o log/dashboard identificar bloqueio de IP x
    falha de rede pontual x CPF sem uuid).

    Cada tentativa abre uma conexao nova (client novo) em vez de reaproveitar
    uma conexao unica durante todo o loop - isso e necessario para que a
    rotacao de IP configurada no proxy (por tempo) tenha efeito entre
    tentativas; mantendo a mesma conexao viva, o proxy manteria o mesmo IP
    de saida indefinidamente independente da config de rotacao.
    """
    dados = gerar_dados(cpf)
    logger.info(f"Dados gerados: nome={dados.nome_completo} email={dados.email} tel={dados.telefone_formatado}")

    ultimo_erro: Optional[TentativaResultado] = None
    chamadas_realizadas = 0
    sem_retorno_consecutivos = 0
    usou_proxy_em_alguma_chamada = False
    caiu_para_direto_em_alguma_chamada = False
    MAX_SEM_RETORNO = 2  # depois disso, para de tentar e marca como "sem_retorno"

    def _chamar_com_fallback(fn, *args):
        """
        Executa fn(client, *args) tentando primeiro via proxy (se ligado).
        Se a falha for especificamente do proxy (erro_proxy), tenta de novo
        uma vez em conexao direta (sem proxy) antes de desistir dessa
        chamada - evita que uma instabilidade pontual do provedor de proxy
        derrube a tentativa inteira.
        """
        nonlocal usou_proxy_em_alguma_chamada, caiu_para_direto_em_alguma_chamada

        if _montar_proxy_url():
            usou_proxy_em_alguma_chamada = True
            with _novo_client(usar_proxy=True) as client:
                resp, erro = fn(client, *args)
            if erro and erro.tipo_erro == "erro_proxy":
                logger.warning(f"Falha no proxy ({erro.detalhe}) - tentando direto sem proxy como fallback")
                caiu_para_direto_em_alguma_chamada = True
                with _novo_client(usar_proxy=False) as client:
                    resp, erro = fn(client, *args)
            return resp, erro

        with _novo_client(usar_proxy=False) as client:
            return fn(client, *args)

    for i in range(1, tentativas + 1):
        logger.info(f"--- Tentativa {i} de {tentativas} ---")

        aut_resp, erro = _chamar_com_fallback(autorizar_facta, cpf, dados)
        chamadas_realizadas += 1
        if aut_resp is None:
            ultimo_erro = erro
            if erro and erro.tipo_erro == "bloqueio_ip":
                # Bloqueio real (ex: Cloudflare) - nao adianta insistir,
                # aborta as tentativas restantes e reporta pra ficar visivel.
                break
            continue

        uuid = aut_resp.get("uuid")
        if not uuid:
            ultimo_erro = TentativaResultado(ok=False, tipo_erro="sem_uuid", detalhe=str(aut_resp)[:200])
            logger.info(f"Resposta sem uuid: {aut_resp}")
            continue

        logger.info(f"UUID obtido: {uuid}")

        sim_resp, erro = _chamar_com_fallback(simular_uuid, uuid)
        chamadas_realizadas += 1
        if sim_resp is not None:
            return {
                "status": "ok",
                "resultado": sim_resp,
                "uuid": uuid,
                "tentativa": i,
                "chamadas_realizadas": chamadas_realizadas,
                "usou_proxy": usou_proxy_em_alguma_chamada,
                "caiu_para_direto": caiu_para_direto_em_alguma_chamada,
                "dados_gerados": {
                    "nome": dados.nome_completo,
                    "email": dados.email,
                    "telefone": dados.telefone_formatado,
                    "ip": dados.ip,
                },
            }
        ultimo_erro = erro
        if erro and erro.tipo_erro == "bloqueio_ip":
            break
        if erro and erro.tipo_erro == "sem_retorno":
            sem_retorno_consecutivos += 1
            if sem_retorno_consecutivos >= MAX_SEM_RETORNO:
                logger.info(
                    f"Facta respondeu vazio {sem_retorno_consecutivos}x seguidas - "
                    f"parando de tentar (nao insiste mais para este CPF)"
                )
                break
        else:
            sem_retorno_consecutivos = 0

    return {
        "status": "falha",
        "chamadas_realizadas": chamadas_realizadas,
        "mensagem": f"Nenhuma tentativa retornou resultado apos {tentativas} tentativas.",
        "tipo_erro": ultimo_erro.tipo_erro if ultimo_erro else "desconhecido",
        "detalhe_erro": ultimo_erro.detalhe if ultimo_erro else None,
        "usou_proxy": usou_proxy_em_alguma_chamada,
        "caiu_para_direto": caiu_para_direto_em_alguma_chamada,
    }
