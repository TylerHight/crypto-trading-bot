FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY packages/exchange_adapters /app/packages/exchange_adapters
COPY packages/domain /app/packages/domain
COPY apps/collector /app/apps/collector

RUN python -m pip install --no-cache-dir /app/packages/domain \
    && python -m pip install --no-cache-dir /app/packages/exchange_adapters \
    && python -m pip install --no-cache-dir /app/apps/collector

USER 65532:65532

ENTRYPOINT ["crypto-collector"]
