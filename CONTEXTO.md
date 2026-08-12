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
