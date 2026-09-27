# provider-recon Ansible deployment

Builds `provider-recon` for linux/amd64 and runs `provider-recon serve` as the
systemd unit `provider-recon.service` on `companycollect` (user
`provider-recon`). The API has no authentication for now, so it listens only on
companycollect's Tailscale address, `100.85.212.113:8095`. It also creates the ClickHouse named collection
`provider_recon`, which the S3-engine table `provider_recon_documents_s3`
reads through.

Installed:
- `/opt/companycollect/corpscout/provider_recon/bin/provider-recon`
- `/opt/companycollect/corpscout/provider_recon/definitions/*.yaml`, with
  deleted definitions removed
- `/etc/corpscout-provider-recon/provider-recon.env` (root, 0600)
- `/etc/systemd/system/provider-recon.service`

## Deploy

```bash
cd services/provider_recon/ansible
export CORPSCOUT_S3_ACCESS_KEY="$(sed -n 's/^CORPSCOUT_S3_ACCESS_KEY=//p' ../../backoffice/.env)"
export CORPSCOUT_S3_SECRET_KEY="$(sed -n 's/^CORPSCOUT_S3_SECRET_KEY=//p' ../../backoffice/.env)"
export LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The named collection is created only when it does not exist. To rotate its
credentials, use `ALTER NAMED COLLECTION provider_recon SET …` by hand.

## Check

```bash
curl -s http://companycollect.taileb086.ts.net:8095/healthz
curl -s -X POST http://companycollect.taileb086.ts.net:8095/v1/collect
curl -s http://companycollect.taileb086.ts.net:8095/v1/runs/<run_id>
journalctl -u provider-recon -n 50
```
