FROM python:3.12-slim

LABEL org.opencontainers.image.title="FleetSwarm"
LABEL org.opencontainers.image.description="Unified dashboard for mixed Bitcoin ASIC fleets"
LABEL org.opencontainers.image.source="https://github.com/gbechtel-beck/fleetswarm"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Install only what we need; one layer keeps the image small
RUN pip install --no-cache-dir flask==3.0.3 requests==2.32.3

COPY app/ /app/
COPY templates/ /app/templates/

# Persistent volume for DB + config — user-mounted via docker-compose
VOLUME /data

EXPOSE 8888

ENV PYTHONUNBUFFERED=1 \
    POLL_INTERVAL=30 \
    DEFAULT_SUBNET=192.168.1.0/24

CMD ["python", "/app/server.py"]
