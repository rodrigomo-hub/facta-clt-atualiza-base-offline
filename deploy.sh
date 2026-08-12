#!/bin/bash
set -e

STACK_NAME="facta-clt-atualiza-base-offline"
IMAGE_NAME="facta-clt-atualiza-base-offline:latest"

echo "=== Build da imagem ==="
docker build -t "$IMAGE_NAME" .

# docker stack deploy NAO le .env automaticamente como o docker compose
# faz - precisa exportar as variaveis no shell antes, senao ${ADMIN_PASS}
# etc no docker-compose.yml ficam vazios silenciosamente.
if [ -f .env ]; then
    echo "=== Carregando .env ==="
    set -a
    source .env
    set +a
else
    echo "AVISO: .env nao encontrado - ADMIN_PASS e outras vars podem ficar vazias."
fi

echo "=== Deploy no swarm ==="
docker stack deploy -c docker-compose.yml "$STACK_NAME" --resolve-image never

echo "=== Forcando update do service (garante que pega a imagem nova) ==="
docker service update --image "$IMAGE_NAME" --force "${STACK_NAME}_facta-clt-atualiza-base-offline"

echo "=== Deploy concluido. Acompanhe com: docker service logs -f ${STACK_NAME}_facta-clt-atualiza-base-offline ==="
