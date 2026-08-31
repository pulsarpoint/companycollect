# Webtech Ansible deployment

This playbook deploys the Webtech scanner to `graovic@192.168.88.149` and
manages its existing user-level `webtech.service` unit. It validates the local
Python and JavaScript source, synchronizes the project, reconciles the Linux
virtual environment with the committed `uv.lock`, and verifies `/healthz`.

The target owns `/opt/companycollect/corpscout/webtech/.env`. Ansible requires
that file to exist as `graovic` with mode `0600` and never copies it from the
control machine. Synchronization also preserves `.venv`, `output`, and
`.cloakbrowser-profile`.

The playbook queries `/healthz` before changing files. It refuses to deploy
while a scan is active, because both a service restart and an extension source
change could alter in-flight browser work. When no source or unit files change,
the playbook leaves the running service untouched.

## Deploy

The macOS control environment needs a supported UTF-8 locale:

```bash
cd corpscout/services/webtech/ansible
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The controller needs Ansible, `uv`, `rsync`, and Node.js. The target needs
Python 3, `uv`, `rsync`, systemd user services, the existing CloakBrowser
runtime, and the manually provisioned `.env`.

## Operations

```bash
ssh 192.168.88.149 'systemctl --user status webtech.service --no-pager'
ssh 192.168.88.149 'journalctl --user -u webtech.service -n 200 -f'
ssh 192.168.88.149 'curl -fsS http://127.0.0.1:8088/healthz'
```
