FROM python:3.12-slim

ENV DAGSTER_HOME=/opt/dagster/home
WORKDIR /opt/dagster/app

COPY pyproject.toml ./
COPY dagster_corpscout ./dagster_corpscout
RUN pip install --no-cache-dir . dagster-webserver

COPY dagster.yaml workspace.yaml /opt/dagster/home/
