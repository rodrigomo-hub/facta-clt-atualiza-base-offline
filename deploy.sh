#!/bin/bash
set -e

STACK_NAME="facta-clt-atualiza-base-offline"
IMAGE_NAME="facta-clt-atualiza-base-offline:latest"

echo "=== Build da imagem ==="
docker build -t "$IMAGE_NAME" .

echo "=== Deploy no swarm ==="
docker stack deploy -c docker-compose.yml "$STACK_NAME" --resolve-image never

echo "=== Forcando update do service (garante que pega a imagem nova) ==="
docker service update --image "$IMAGE_NAME" --force "${STACK_NAME}_facta-clt-atualiza-base-offline"

echo "=== Deploy concluido. Acompanhe com: docker service logs -f ${STACK_NAME}_facta-clt-atualiza-base-offline ==="
