# Contexto do projeto — Facta CLT Atualiza Base Offline

Histórico de decisões e descobertas da sessão de desenvolvimento (12/08/2026),
para referência futura sem precisar reconstruir o raciocínio do zero.

## Origem

Port do script `facta-clt-simular.ps1` (PowerShell) para uma API Python
(FastAPI), replicando o fluxo de autocontratação Facta CLT: gerar dados
fake de contato → `POST clt/analisa-cliente` (autorizar, pega `uuid`) →
`POST clt/simulando` (simular com o uuid).

## Descobertas importantes

### 1. Bloqueio de IP por Cloudflare (VPS antiga)
A VPS `179.198.103.79` (esteiradeleads.shop) tomava 403 "Just a moment..."
imediato do Cloudflare da Facta — reputação de IP de datacenter, não
relacionado ao nosso código. Confirmado testando o mesmo payload de VPSs
diferentes: `31.56.44.70` (automacaofintech.digital) não sofre esse
bloqueio. **Solução:** migrar para essa VPS.

### 2. Formato do celular importa
A Facta exige o campo `celular` no formato `(DD) NNNNN-NNNN` (com
parênteses, espaço e traço). Sem essa formatação, retorna erro de validação
("Celular inválido"). Isso já estava certo no `.ps1` original — foi
alterado por engano durante debug e revertido.

### 3. Rate limit real (429) é esperado sob volume
A Facta aplica throttle de verdade (`HTTP 429: Numero excessivo de
requisicoes`) quando o volume de chamadas é alto. Isso é diferente de
bloqueio de IP — é esperado, não é erro grave. O sistema separa os dois:
- `bloqueio_ip` (403, Cloudflare) → aborta tentativas, é grave
- `rate_limit` (429, throttle) → continua tentando normalmente, é esperado

### 4. Resposta vazia do `/simulando`
A Facta às vezes responde `200 OK` com corpo vazio no `clt/simulando`.
Não é erro de rede, é uma resposta válida sem dado. Classificado como
`sem_retorno` — o sistema para de tentar após 2 ocorrências consecutivas
(em vez de esgotar as 5 tentativas à toa).

### 5. Timeout do httpx vs PowerShell
O `.ps1` original (`Invoke-RestMethod`) não tem timeout curto fixo. Nosso
Python (`httpx`) tinha `timeout=30`, que causava "read operation timed out"
recorrente sob rede mais lenta (ex: conexão residencial). Removido
(`timeout=None`) para casar com o comportamento do script original.

### 6. Rate limiter global
Implementado um espaçamento mínimo entre chamadas de saída para a Facta
(`FACTA_INTERVALO_MINIMO_SEGUNDOS`, padrão `1.2s`, configurável via env
var). É serializado globalmente — não importa quantos workers concorrentes
existam, a saída real para a Facta respeita esse intervalo. Isso limita o
throughput teórico a `3600/intervalo` chamadas/hora.

### 7. Fila com prioridade
Cada equipe tem um campo `prioridade` (0-10, editável no painel). Jobs de
prioridade maior furam a fila na frente de jobs já esperando de prioridade
menor — implementado com `asyncio.PriorityQueue`. Não aumenta a capacidade
total do sistema, só reordena quem processa primeiro.

### 8. Cuidado ao limpar o banco com o serviço rodando
Rodar `DELETE FROM requisicoes` enquanto o serviço está no ar, com jobs já
enfileirados em memória, cria "zumbis": os workers continuam processando
esses jobs (fazendo chamadas reais), mas o `finalizar_requisicao` não acha
mais a linha no banco (foi apagada) e o update não afeta nada — silenciosa
mente. Isso trava o dashboard mostrando "0 processado" mesmo com atividade
real acontecendo, e os jobs novos ficam presos atrás dos zumbis na fila.
**Sempre reiniciar o serviço logo após limpar o banco** para manter memória
e banco sincronizados.

### 9. Cloudflare barra IP de datacenter E o cliente Python (out/2026)
O Cloudflare da Facta passou a devolver 403 "Just a moment" em dois casos
independentes: (a) qualquer IP de datacenter (VPS) — nem curl passa direto;
(b) o handshake TLS do `httpx`, mesmo saindo por proxy residencial. Por isso
o cliente HTTP agora e `curl_cffi` com `impersonate` (default `chrome120`,
env `FACTA_IMPERSONATE`) e o proxy residencial e **obrigatorio**: com o
proxy desligado/incompleto tudo da `bloqueio_ip`. O fallback direto quando
o proxy da erro nao adianta mais. Na VPS (Docker) `curl_cffi` vem pelo
requirements.txt.

### 10. Fila persistida e webhook de retorno
- A fila agora vive no banco: no boot, `na_fila` e `processando` voltam para
  a fila (mesma prioridade/tentativas/ref). Restart/deploy nao perde mais job.
  Limpar o banco continua exigindo parar o servico antes (secao 8).
- `POST /simular_facta_clt` aceita `webhook_url` e `ref` opcionais. Responde
  na hora com `job_id`; quando o CPF termina, o servico faz POST do resultado
  na URL: `{job_id, ref, cpf, status ok|falha, uuid, tentativa, resultado,
  tipo_erro, detalhe_erro, resultado_completo}`. `ref` volta igual ao enviado.
- Entrega com retry (5 tentativas: 5s, 30s, 2min, 10min, 1h), timeout de 10s,
  sem assinatura. Outbox persistida na tabela `webhooks` (sobrevive a restart).
  Acompanhar/reenviar: `GET /admin/webhooks?status=falhou`,
  `POST /admin/webhooks/reenviar`.
- CPF duplicado na fila: todos os chamadores com webhook sao avisados; a
  Facta e consultada uma vez so.
- `webhook_url` interna (localhost, IP privado, `.internal`) e recusada (SSRF);
  `WEBHOOK_PERMITIR_REDE_PRIVADA=true` libera.
- `GET /job/{id}` e `/simular_facta_clt/aguardar` continuam funcionando.
  Job finalizado sai da memoria apos 10 min e passa a ser lido do banco
  (so `uuid`, sem o `resultado` completo).
- Reduzir a concorrencia nao cancela mais job em andamento: o worker termina
  o job atual e so entao sai (antes deixava zumbi em `processando`).

### 11. Saldo do proxy, estimativa de gasto e alerta
- `proxy_saldo.py` consulta a API do Proxy-Seller
  (`GET https://proxy-seller.com/personal/api/v2/{API_KEY}/resident/package`
  -> `traffic_limit/usage/left` em bytes) de hora em hora e guarda fotos na
  tabela `proxy_saldo`. A API key vai na URL: o logger do httpx fica em
  WARNING e os erros sao sanitizados para a chave nunca ir para o log.
  O IP da VPS precisa estar autorizado no painel do Proxy-Seller
  (senao a API responde "IP not allowed" - aparece no painel como erro).
- Painel: card "Saldo do proxy" (restante, consumido, consumo/dia, dias
  restantes, KB por CPF e, se houver preco/GB, custo ja consumido, por dia e
  por 1000 CPFs). Config via painel ou `POST /admin/proxy/saldo/config`
  (`api_key`, `alerta_mb`, `preco_gb`, `alerta_webhook_url`);
  `GET /admin/proxy/saldo?atualizar=true` consulta na hora.
- Alerta por POST no `alerta_webhook_url` (n8n) quando restar menos que
  `alerta_mb` (default 500) ou zerar: 1 aviso por mudanca de nivel, lembrete
  a cada 24h, rearma apos recarga. Payload: `{tipo:"proxy_saldo", nivel,
  traffic_left_mb, dias_restantes_estimado, mensagem}`.
- A estimativa de consumo inclui qualquer outro uso da mesma conta de proxy.
- Proxy: campos host/porta/usuario/senha vazios sao ignorados ao salvar (um
  salvamento em branco ja deixou o proxy ligado e vazio); o painel avisa em
  vermelho quando o proxy esta ligado mas incompleto.

## Ambientes ativos

- **VPS produção:** `https://factaclt.automacaofintech.digital`
  (VPS `31.56.44.70`, Docker Swarm, rede `traefik_public`)
- **Local (teste/comparação):** roda via `uvicorn` no PC do Rodrigo,
  exposto publicamente via Cloudflare Tunnel (`cloudflared tunnel --url
  http://localhost:8000`). URL muda a cada reinício do túnel.

## Comandos úteis

```bash
# Deploy na VPS
cd /opt/facta-clt-atualiza-base-offline
git pull <url-com-token> main
bash deploy.sh

# Limpar historico de requisicoes (mantendo equipes) - VPS, dentro do container
docker exec <container_id> python3 -c "
import sqlite3
c = sqlite3.connect('/app/data/facta_clt.db')
c.execute('DELETE FROM requisicoes')
c.commit()
print('Apagados:', c.total_changes)
"

# Diagnostico rapido de bloqueio (sem fila, 1 chamada direta)
curl -s https://factaclt.automacaofintech.digital/admin/diagnostico-facta | python3 -m json.tool

# Ver capacidade/throughput teorico vs real
curl -s https://factaclt.automacaofintech.digital/admin/capacidade | python3 -m json.tool
```

## Pendências / ideias futuras

- Perguntar para a Facta qual o limite real de chamadas/hora ou dia para
  a API de autocontratação, para dimensionar capacidade com certeza em
  vez de estimar por tentativa e erro.
- Considerar se vale a pena tornar o ambiente local um serviço persistente
  do Windows (NSSM ou Task Scheduler) em vez de terminal manual, caso
  continue em uso além de teste pontual.

## 12. Resultado do fluxo (n8n) + funil de metricas + regras de entrega do webhook

- **"Base atualizada"** (antes "Autorizado" no painel): `status=autorizado` no banco = a Facta respondeu com dado. NAO significa pre-aprovado. A aprovacao real e decidida no fluxo n8n.
- **`POST /resultado`** (sem Basic Auth; a "senha" e o `job_id`, que so quem recebeu o webhook conhece; `X-Team-Key` opcional, se enviada tem que ser da equipe dona do job). Body: `job_id`, `resultado` (PRE_APROVADO | NAO_APROVADO | FALHA_CONSULTA | APROVADO_SEM_SIMULACAO ...), `motivo`, `banco`, `margem`, `valor_liquido_max`, `detalhes`. Upsert por job (tabela `resultados_fluxo`); 404 se o job nao existe.
- **`GET /admin/funil?equipe=`**: entraram, base atualizada, com/sem resultado do fluxo, por resultado, pre-aprovados, taxa, margem media, liquido medio/maximo, motivos, por equipe (dia de Brasilia). Painel: secao "Funil de resultado (hoje)".
- **Entrega do webhook** (`queue_worker._enviar_webhook`): timeout de leitura `WEBHOOK_TIMEOUT_SEGUNDOS` (default 60). Leitura estourou -> considerado entregue (aviso em `ultimo_erro`), NAO reenvia (evita rodar o fluxo 2x). Retry so em erro de conexao e HTTP 408/425/429/502/503/504. Outros non-2xx (500, 404...) -> `falhou` na hora (reenvio manual em `/admin/webhooks/reenviar`). `webhooks.status_http` e `webhooks.resposta` guardam o retorno do destino.
- Fluxo pronto: `n8n/retorno_com_metricas.json` (token do painel esteira e placeholder; `$('')` corrigido; nos "Reportar Resultado - *" em cada termino; Respond no caminho de ofertas).
- Testes (fora do repo): test_resultado.py (15), test_webhook.py (37).

- **Pausa** (`POST /admin/config/pausa {"pausado": bool}`, botao no painel): em pausa `/simular_facta_clt` (e `/aguardar`) recusa CPFs novos com 503 e os workers nao comecam jobs novos; o que ja esta rodando termina. Estado salvo em `config.pausado`, respeitado no boot.
