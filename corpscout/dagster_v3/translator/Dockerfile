FROM python:3.12-slim

RUN pip install --no-cache-dir uv
WORKDIR /app
COPY corpscout/dagster_v3 /app/dagster_v3
WORKDIR /app/dagster_v3
RUN uv sync --no-dev
CMD ["uv", "run", "translator-worker"]
