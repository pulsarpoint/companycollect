uv run --env-file crawler_ratsit/ansible/worker-environment \
  ratsit-inspect 9280007478 \
  --config crawler_ratsit/ansible/process-config.toml \
  --browser direct \
  --headless