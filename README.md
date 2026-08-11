# Facta CLT - Autocontratacao + Fila + Dashboard

API que replica o fluxo de autocontratacao Facta CLT (autorizar + simular),
com fila de processamento com concorrencia configuravel, log de auditoria
por equipe e dashboard simples de acompanhamento.

## Rodando local

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Dashboard: `http://localhost:8000/`

## Fluxo

1. Criar equipe: `POST /admin/equipes {"nome": "equipe_a"}` -> retorna `api_key`
2. Enfileirar simulacao: `POST /simular_facta_clt` com header `X-Team-Key: <api_key>`
   ```json
   {"cpf": "08670660911", "token": "token_bearer_facta", "tentativas": 5}
   ```
   Retorna `job_id`.
3. Consultar resultado: `GET /job/{job_id}`
4. Ou aguardar direto: `POST /simular_facta_clt/aguardar?timeout=120`

## Concorrencia da fila

- `GET /admin/config` - ve concorrencia atual e tamanho da fila
- `POST /admin/config/concorrencia {"concorrencia": 5}` - ajusta ao vivo, sem restart

## Classificacao de erro

Cada falha registrada tem um `tipo_erro`:
- `bloqueio_suspeito` - status 403/429 ou corpo indicando rate-limit/bloqueio (serve de alerta, nao contorna)
- `erro_rede` - timeout/conexao pontual
- `sem_uuid` - autorizou mas resposta sem uuid

## Stats

- `GET /admin/stats/hoje?equipe=equipe_a` - contagem do dia
- `GET /admin/requisicoes?limite=50` - ultimas requisicoes
