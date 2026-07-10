# cc-dns-worker Ansible Deployment Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reproduce the existing `cc-dns-worker` production deployment (currently on `100.118.244.87`) as idempotent Ansible, and stand it up on `hetzner01`.

**Architecture:** One Ansible playbook (`site.yml`) with three roles — `os_tuning` (sysctl, nofile limits, DNS NOTRACK firewall), `unbound` (install + tuned recursive resolver at `127.0.0.1:53`), `cc_dns_worker` (cross-compiled binary + `.env` + systemd orchestrator service). Tunables (unbound threads/caches, run flags) are host-scaled via facts/vars; the ClickHouse password is `ansible-vault`-encrypted. Faithful to the source deployment, with one correctness fix (raise `net.core.rmem_max`/`wmem_max` so unbound's `so-rcvbuf: 8m` isn't clamped).

**Tech Stack:** Ansible (control machine), Ubuntu 26.04 target, unbound, iptables-persistent, systemd, a static Go binary (CGO-free).

## Source deployment (reverse-engineered from 100.118.244.87)

- **Service `cc-dns-scan.service`** (runs as root): `WorkingDirectory` + binary + `.env` under `/opt/companycollect/corpscout/commoncrawl/cc-dns-worker/`; `After=network-online.target unbound.service`; `EnvironmentFile=…/.env`; `LimitNOFILE=1048576`; `Restart=always`, `RestartSec=10`; ExecStart:
  `bin/cc-dns-worker run --dir <workdir> --resolvers 127.0.0.1:53 --discovery-qps 3000 --workers 2000 --query-timeout 2s --per-server-qps 30 --per-server-inflight 20 --hyperscaler-qps 500 --discovery-inflight 3000 --load-interval 30m`
- **Unbound 1.19.2** `scanner.conf`: `interface 127.0.0.1` `port 53`, access-control localhost-only, `module-config: "iterator"` (no validator), `num-threads: 8`, `so-reuseport`, `so-rcvbuf/sndbuf: 8m`, `msg-cache-size 4g`, `rrset-cache-size 8g`, `cache-min-ttl 60`, `cache-max-ttl 86400`, `prefetch`+`prefetch-key`, `outgoing-range 4096`, `num-queries-per-thread 2048`, `infra-cache-numhosts 1000000`, `qname-minimisation`, `extended-statistics`; systemd drop-in `LimitNOFILE=131072`.
- **sysctl `99-cc-dns.conf`**: `fs.file-max=2097152`, `net.ipv4.ip_local_port_range=1024 65535`, `net.netfilter.nf_conntrack_max=1048576`, `nf_conntrack_udp_timeout=30`, `nf_conntrack_udp_timeout_stream=60`. (**Note:** `net.core.rmem_max`/`wmem_max` were left at the 208 KB default, silently clamping unbound's 8m buffers — fixed here.)
- **limits `/etc/security/limits.d`**: `* soft/hard nofile 1048576`, `root soft/hard nofile 1048576`.
- **Firewall**: iptables `raw` NOTRACK for DNS, persisted by `iptables-persistent` in `/etc/iptables/rules.v4`:
  `PREROUTING -p udp/tcp --sport 53 -j NOTRACK`; `OUTPUT -p udp/tcp --dport 53 -j NOTRACK`.
- **ClickHouse** (`.env`, worker reads `CLICKHOUSE_HOST/NATIVE_PORT/USER/PASSWORD/DATABASE`): `HOST=companycollect` (Tailscale), `NATIVE_PORT=9002`, `USER=default`, `DATABASE=corpscout`, `SECURE=false`, password = secret.
- **Binary**: prebuilt static Go ELF (~21 MB), no Go/git on the box.
- **Target `hetzner01`**: Ubuntu 26.04, 12 vCPU / 62 GB, already on Tailscale (`companycollect` resolves), unbound absent.

## Global Constraints

- **Idempotent** — re-running the playbook is a no-op on a converged host. Handlers restart services only on config change.
- **Deploy path** (unchanged from source): `/opt/companycollect/corpscout/commoncrawl/cc-dns-worker/`. Service name `cc-dns-scan.service`. Runs as **root** (matches source; the NOTRACK/limits/bind-53 need privilege).
- **Secrets:** `CLICKHOUSE_PASSWORD` lives only in `group_vars/cc_dns/vault.yml` (ansible-vault). Never commit a plaintext password. The `.env` is templated `0600 root:root`.
- **Runtime state stays on the box:** the multi-GB SQLite cycle DBs, `orchestrator-state.json`, and logs are created by the worker at runtime — Ansible creates the directory but never ships or deletes state.
- **Binary is built CGO-free** (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64`) on the control machine (modernc sqlite + clickhouse-go are pure Go) and copied — matches the source (no Go toolchain on the target).
- **New-feature flags ON by default** (`--axfr --host-enrich`) — these are the new version's defaults. The ExecStart flags are the var `cc_dns_run_flags`; dropping those two words falls back to prod parity if the enrichment load ever needs backing out.
- Files/paths live under `corpscout/commoncrawl/cc-dns-worker/deploy/ansible/`. Git root is `/Users/graovic/pulsarpoint/ppoint/companycollect`.
- Prereq (out of Ansible scope, already true): target on Tailscale with `companycollect` reachable; ClickHouse migrations `000105–000110` already applied.

## File Structure (`deploy/ansible/`)

```
ansible.cfg
inventory.ini                         # [cc_dns] hetzner01 …
site.yml                              # play: hosts cc_dns, roles os_tuning/unbound/cc_dns_worker
group_vars/cc_dns/vars.yml            # non-secret vars (paths, tuning, run flags, CH conn)
group_vars/cc_dns/vault.yml           # ansible-vault: vault_clickhouse_password
roles/
  os_tuning/tasks/main.yml
  os_tuning/templates/99-cc-dns.conf.j2
  os_tuning/templates/99-cc-dns-limits.conf.j2
  unbound/tasks/main.yml
  unbound/templates/scanner.conf.j2
  unbound/templates/unbound-limits.conf.j2   # systemd drop-in
  cc_dns_worker/tasks/main.yml
  cc_dns_worker/templates/env.j2
  cc_dns_worker/templates/cc-dns-scan.service.j2
README.md
```

---

## Task 1: Scaffold — inventory, config, vars, vault, playbook skeleton

**Files:** `deploy/ansible/{ansible.cfg,inventory.ini,site.yml,README.md}`, `group_vars/cc_dns/{vars.yml,vault.yml}`

**Interfaces:** Produces the vars every role reads (`deploy_dir`, `service_name`, `cc_dns_run_flags`, unbound tuning vars, `clickhouse_*`, `vault_clickhouse_password`).

- [ ] **Step 1: `ansible.cfg`**

```ini
[defaults]
inventory = inventory.ini
host_key_checking = False
retry_files_enabled = False
stdout_callback = default
result_format = yaml
# Vault password read from a file (generated in Step 4) so runs are non-interactive.
vault_password_file = ~/.config/ansible/cc-dns-scan
[ssh_connection]
pipelining = True
```

- [ ] **Step 2: `inventory.ini`**

```ini
[cc_dns]
hetzner01 ansible_host=hetzner01 ansible_user=root
```

(`hetzner01` resolves via Tailscale/ssh-config; override `ansible_host` with the IP `100.106.174.123` if name resolution isn't available to the control machine.)

- [ ] **Step 3: `group_vars/cc_dns/vars.yml`**

```yaml
# --- deploy layout (matches the source host) ---
deploy_root: /opt/companycollect
deploy_dir: "{{ deploy_root }}/corpscout/commoncrawl/cc-dns-worker"
service_name: cc-dns-scan

# --- worker run flags (new-version defaults: AXFR + CT/registry enrichment ON) ---
# --axfr / --host-enrich are the new version's features; drop them to fall back to prod parity.
cc_dns_run_flags: >-
  run --dir {{ deploy_dir }}
  --resolvers 127.0.0.1:53
  --discovery-qps 3000 --workers 2000 --query-timeout 2s
  --per-server-qps 30 --per-server-inflight 20 --hyperscaler-qps 500
  --discovery-inflight 3000 --load-interval 30m
  --axfr --host-enrich
worker_limit_nofile: 1048576

# --- unbound tuning (num-threads scales to the target's CPUs) ---
unbound_num_threads: "{{ ansible_processor_vcpus }}"
unbound_msg_cache_size: 4g
unbound_rrset_cache_size: 8g
unbound_outgoing_range: 4096
unbound_so_rcvbuf: 8m
unbound_so_sndbuf: 8m
unbound_limit_nofile: 131072

# --- OS tuning ---
nf_conntrack_max: 1048576
# rmem/wmem raised so unbound's so-rcvbuf/sndbuf (8m) are not clamped (source bug)
net_core_rmem_max: 16777216
net_core_wmem_max: 16777216
sys_nofile: 1048576

# --- ClickHouse connection (non-secret; password from vault) ---
clickhouse_host: companycollect
clickhouse_native_port: 9002
clickhouse_user: default
clickhouse_database: corpscout
clickhouse_secure: "false"
clickhouse_password: "{{ vault_clickhouse_password }}"
```

- [ ] **Step 4: Generate the vault password file, then encrypt the CH secret**

The ansible-vault password is generated **once** and stored (mode `0600`, outside the repo) at
`~/.config/ansible/cc-dns-scan`; `ansible.cfg`'s `vault_password_file` reads it automatically, so no
run ever needs `--ask-vault-pass`:

```bash
mkdir -p ~/.config/ansible
openssl rand -base64 48 > ~/.config/ansible/cc-dns-scan   # generate the vault password
chmod 600 ~/.config/ansible/cc-dns-scan

cd deploy/ansible
printf 'vault_clickhouse_password: "REPLACE_WITH_REAL_CH_PASSWORD"\n' > group_vars/cc_dns/vault.yml
ansible-vault encrypt group_vars/cc_dns/vault.yml         # uses vault_password_file from ansible.cfg
```

The encrypted `vault.yml` is safe to commit; the password file lives in `~/.config/ansible/` (never in
the repo). To rotate: re-`openssl rand` into the file and `ansible-vault rekey group_vars/cc_dns/vault.yml`.

- [ ] **Step 5: `site.yml`**

```yaml
---
- name: Deploy cc-dns-worker
  hosts: cc_dns
  become: true
  gather_facts: true
  roles:
    - os_tuning
    - unbound
    - cc_dns_worker
```

- [ ] **Step 6: `README.md`**

Document: prereqs (target on Tailscale reaching `companycollect`; CH migrations applied; control machine has Ansible + Go + this repo), the one-time vault setup (Step 4 — generate `~/.config/ansible/cc-dns-scan`, encrypt the CH password into `vault.yml`), and the run command:
`ansible-playbook site.yml` (non-interactive — the vault password is read from `~/.config/ansible/cc-dns-scan` via `ansible.cfg`; add `--check --diff` for a dry run).

- [ ] **Step 7: Commit**

```bash
git add corpscout/commoncrawl/cc-dns-worker/deploy/ansible
git commit -m "feat(deploy): ansible scaffold — inventory, vars, vault, site.yml"
```

---

## Task 2: `os_tuning` role — sysctl, nofile limits, DNS NOTRACK

**Files:** `roles/os_tuning/tasks/main.yml`, `templates/99-cc-dns.conf.j2`, `templates/99-cc-dns-limits.conf.j2`

**Interfaces:** Consumes `nf_conntrack_max`, `net_core_rmem_max/wmem_max`, `sys_nofile`.

- [ ] **Step 1: `templates/99-cc-dns.conf.j2`**

```jinja
fs.file-max = 2097152
net.ipv4.ip_local_port_range = 1024 65535
net.netfilter.nf_conntrack_max = {{ nf_conntrack_max }}
net.netfilter.nf_conntrack_udp_timeout = 30
net.netfilter.nf_conntrack_udp_timeout_stream = 60
# raised so unbound so-rcvbuf/sndbuf ({{ unbound_so_rcvbuf }}) are not clamped
net.core.rmem_max = {{ net_core_rmem_max }}
net.core.wmem_max = {{ net_core_wmem_max }}
```

- [ ] **Step 2: `templates/99-cc-dns-limits.conf.j2`**

```jinja
*     soft nofile {{ sys_nofile }}
*     hard nofile {{ sys_nofile }}
root  soft nofile {{ sys_nofile }}
root  hard nofile {{ sys_nofile }}
```

- [ ] **Step 3: `tasks/main.yml`**

```yaml
---
- name: Ensure nf_conntrack module is loaded (for the conntrack sysctls)
  community.general.modprobe:
    name: nf_conntrack
    state: present
    persistent: present

- name: Install DNS sysctl tuning
  ansible.builtin.template:
    src: 99-cc-dns.conf.j2
    dest: /etc/sysctl.d/99-cc-dns.conf
    owner: root
    group: root
    mode: "0644"
  notify: Reload sysctl

- name: Install nofile limits
  ansible.builtin.template:
    src: 99-cc-dns-limits.conf.j2
    dest: /etc/security/limits.d/99-cc-dns.conf
    owner: root
    group: root
    mode: "0644"

- name: Install iptables-persistent (persists the raw NOTRACK rules)
  ansible.builtin.apt:
    name: [iptables-persistent, netfilter-persistent]
    state: present
    update_cache: true

# NOTE: split into two tasks — Ansible cannot template a module PARAMETER NAME
# (a single loop with "{{ item.dir }}_port" fails), so source_port / destination_port are fixed.
- name: DNS NOTRACK — replies (PREROUTING, source port 53)
  ansible.builtin.iptables:
    table: raw
    chain: PREROUTING
    protocol: "{{ item }}"
    source_port: "53"
    jump: NOTRACK
    comment: "cc-dns NOTRACK reply {{ item }}"
    state: present
  loop: [udp, tcp]
  notify: Persist iptables

- name: DNS NOTRACK — queries (OUTPUT, destination port 53)
  ansible.builtin.iptables:
    table: raw
    chain: OUTPUT
    protocol: "{{ item }}"
    destination_port: "53"
    jump: NOTRACK
    comment: "cc-dns NOTRACK query {{ item }}"
    state: present
  loop: [udp, tcp]
  notify: Persist iptables
```

Note (Ubuntu 26.04): installing `iptables-persistent` removes `ufw` (packaging conflict on 26.04). This is safe when `ufw status` is `inactive` (the box's firewall is Tailscale + the cloud provider's) — confirm `ufw` is inactive before applying; if it's actively enforcing rules, persist the NOTRACK ruleset via native nftables (`/etc/nftables.conf`) instead of installing iptables-persistent.

Handlers (`roles/os_tuning/handlers/main.yml`):

```yaml
---
- name: Reload sysctl
  ansible.builtin.command: sysctl --system
  changed_when: true

- name: Persist iptables
  ansible.builtin.command: netfilter-persistent save
  changed_when: true
```

- [ ] **Step 4: Apply + verify**

Run: `ansible-playbook site.yml --tags os_tuning` (add `--tags` via `tags: os_tuning` on the role in site.yml, or run the whole play). Then verify on the target:

```bash
ssh root@hetzner01 'sysctl net.core.rmem_max net.netfilter.nf_conntrack_max fs.file-max; iptables -t raw -S | grep NOTRACK; grep -c nofile /etc/security/limits.d/99-cc-dns.conf'
```
Expected: `rmem_max = 16777216`, `nf_conntrack_max = 1048576`, four `NOTRACK` rules, nofile lines present.

- [ ] **Step 5: Commit**

```bash
git add corpscout/commoncrawl/cc-dns-worker/deploy/ansible/roles/os_tuning
git commit -m "feat(deploy): os_tuning role — sysctl, nofile, DNS NOTRACK"
```

---

## Task 3: `unbound` role — install + tuned recursive resolver

**Files:** `roles/unbound/tasks/main.yml`, `templates/scanner.conf.j2`, `templates/unbound-limits.conf.j2`, `handlers/main.yml`

**Interfaces:** Consumes the `unbound_*` vars. Produces a resolver answering at `127.0.0.1:53` (what the worker's `--resolvers` points at).

- [ ] **Step 1: `templates/scanner.conf.j2`**

```jinja
server:
    interface: 127.0.0.1
    port: 53
    access-control: 127.0.0.0/8 allow
    access-control: 0.0.0.0/0 refuse
    module-config: "iterator"
    num-threads: {{ unbound_num_threads }}
    so-reuseport: yes
    so-rcvbuf: {{ unbound_so_rcvbuf }}
    so-sndbuf: {{ unbound_so_sndbuf }}
    msg-cache-size: {{ unbound_msg_cache_size }}
    rrset-cache-size: {{ unbound_rrset_cache_size }}
    cache-min-ttl: 60
    cache-max-ttl: 86400
    prefetch: yes
    prefetch-key: yes
    outgoing-range: {{ unbound_outgoing_range }}
    num-queries-per-thread: 2048
    infra-cache-numhosts: 1000000
    qname-minimisation: yes
    extended-statistics: yes
remote-control:
    control-enable: yes
```

- [ ] **Step 2: `templates/unbound-limits.conf.j2`**

```jinja
[Service]
LimitNOFILE={{ unbound_limit_nofile }}
```

- [ ] **Step 3: `tasks/main.yml`**

```yaml
---
- name: Install unbound
  ansible.builtin.apt:
    name: [unbound, dns-root-data]
    state: present
    update_cache: true

- name: Install scanner.conf (tuned recursive resolver)
  ansible.builtin.template:
    src: scanner.conf.j2
    dest: /etc/unbound/unbound.conf.d/scanner.conf
    owner: root
    group: root
    mode: "0644"
    validate: "unbound-checkconf %s"
  notify: Restart unbound

- name: Systemd drop-in — raise unbound LimitNOFILE
  ansible.builtin.template:
    src: unbound-limits.conf.j2
    dest: /etc/systemd/system/unbound.service.d/limits.conf
    owner: root
    group: root
    mode: "0644"
  notify:
    - Reload systemd
    - Restart unbound

- name: Enable + start unbound
  ansible.builtin.systemd_service:
    name: unbound
    enabled: true
    state: started
```

Handlers:

```yaml
---
- name: Reload systemd
  ansible.builtin.systemd_service:
    daemon_reload: true
- name: Restart unbound
  ansible.builtin.systemd_service:
    name: unbound
    state: restarted
```

Note: the `validate: unbound-checkconf %s` guarantees a broken template never lands live. The `unbound.service.d` directory is created by the package; if a task fails because it's absent, add a `file: path=…/unbound.service.d state=directory` step before the drop-in.

- [ ] **Step 4: Apply + verify unbound resolves**

Run the play, then:

```bash
ssh root@hetzner01 'systemctl is-active unbound; unbound-checkconf; dig +short +time=2 @127.0.0.1 example.com A; unbound-control stats_noreset | grep -E "num.threads|total.num.queries" | head'
```
Expected: `active`, checkconf OK, an A record returned, thread count = target vcpus.

- [ ] **Step 5: Commit**

```bash
git add corpscout/commoncrawl/cc-dns-worker/deploy/ansible/roles/unbound
git commit -m "feat(deploy): unbound role — tuned iterator resolver on 127.0.0.1:53"
```

---

## Task 4: `cc_dns_worker` role — binary, .env, systemd service

**Files:** `roles/cc_dns_worker/tasks/main.yml`, `templates/env.j2`, `templates/cc-dns-scan.service.j2`, `handlers/main.yml`

**Interfaces:** Consumes `deploy_dir`, `service_name`, `cc_dns_run_flags`, `worker_limit_nofile`, `clickhouse_*`. Produces the running orchestrator service.

- [ ] **Step 1: `templates/env.j2`** (worker reads only the CLICKHOUSE_* set)

```jinja
CLICKHOUSE_HOST={{ clickhouse_host }}
CLICKHOUSE_NATIVE_PORT={{ clickhouse_native_port }}
CLICKHOUSE_USER={{ clickhouse_user }}
CLICKHOUSE_PASSWORD={{ clickhouse_password }}
CLICKHOUSE_DATABASE={{ clickhouse_database }}
CLICKHOUSE_SECURE={{ clickhouse_secure }}
```

- [ ] **Step 2: `templates/cc-dns-scan.service.j2`**

```jinja
[Unit]
Description=cc-dns-worker continuous orchestrator (scan -> load -> repeat)
After=network-online.target unbound.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={{ deploy_dir }}
ExecStart={{ deploy_dir }}/bin/cc-dns-worker {{ cc_dns_run_flags }}
EnvironmentFile={{ deploy_dir }}/.env
LimitNOFILE={{ worker_limit_nofile }}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 3: `tasks/main.yml`**

```yaml
---
- name: Build the worker binary (CGO-free, linux/amd64) on the control machine
  ansible.builtin.command:
    cmd: go build -o bin/cc-dns-worker ./cmd/cc-dns-worker
    chdir: "{{ playbook_dir }}/../.."   # -> corpscout/commoncrawl/cc-dns-worker
  environment:
    CGO_ENABLED: "0"
    GOOS: linux
    GOARCH: amd64
  delegate_to: localhost
  become: false
  changed_when: true

- name: Create deploy dir + bin/
  ansible.builtin.file:
    path: "{{ deploy_dir }}/bin"
    state: directory
    owner: root
    group: root
    mode: "0755"

- name: Copy the worker binary
  ansible.builtin.copy:
    src: "{{ playbook_dir }}/../../bin/cc-dns-worker"
    dest: "{{ deploy_dir }}/bin/cc-dns-worker"
    owner: root
    group: root
    mode: "0755"
  notify: Restart cc-dns-scan

- name: Template the .env (secret)
  ansible.builtin.template:
    src: env.j2
    dest: "{{ deploy_dir }}/.env"
    owner: root
    group: root
    mode: "0600"
  notify: Restart cc-dns-scan

- name: Install the systemd unit
  ansible.builtin.template:
    src: cc-dns-scan.service.j2
    dest: "/etc/systemd/system/{{ service_name }}.service"
    owner: root
    group: root
    mode: "0644"
  notify:
    - Reload systemd
    - Restart cc-dns-scan

- name: Enable + start the orchestrator
  ansible.builtin.systemd_service:
    name: "{{ service_name }}"
    enabled: true
    state: started
```

Handlers:

```yaml
---
- name: Reload systemd
  ansible.builtin.systemd_service:
    daemon_reload: true
- name: Restart cc-dns-scan
  ansible.builtin.systemd_service:
    name: "{{ service_name }}"
    state: restarted
```

Note: the build/copy paths assume `deploy/ansible/` sits two levels under the module root (`…/cc-dns-worker/deploy/ansible`), so `{{ playbook_dir }}/../..` is the module and `bin/cc-dns-worker` is the just-built binary. Verify this relative path when scaffolding; adjust if the ansible dir moves.

- [ ] **Step 4: Apply + verify the service runs and connects**

Run the play, then:

```bash
ssh root@hetzner01 'systemctl is-active cc-dns-scan; systemctl status cc-dns-scan --no-pager | head -6; journalctl -u cc-dns-scan -n 25 --no-pager'
```
Expected: `active`, and the log shows the orchestrator starting a cycle, seeding from ClickHouse (proves CH connectivity via `companycollect`), and no auth/connection errors. If it errors on CH, re-check the vault password and that the target reaches `companycollect:9002`.

- [ ] **Step 5: Commit**

```bash
git add corpscout/commoncrawl/cc-dns-worker/deploy/ansible/roles/cc_dns_worker
git commit -m "feat(deploy): cc_dns_worker role — binary, .env, systemd orchestrator"
```

---

## Task 5: End-to-end verify + idempotency on hetzner01

**Files:** none (operational).

- [ ] **Step 1: Full converge from clean**

Run: `ansible-playbook site.yml`
Expected: completes with changed tasks; no failures.

- [ ] **Step 2: Idempotency (the acceptance gate)**

Run it again: `ansible-playbook site.yml`
Expected: `changed=0` for every host (a converged host is a no-op; handlers don't fire).

- [ ] **Step 3: End-to-end health**

```bash
ssh root@hetzner01 'set -e
  systemctl is-active unbound cc-dns-scan
  dig +short @127.0.0.1 cloudflare.com A
  iptables -t raw -S | grep -c NOTRACK        # expect 4
  sysctl -n net.core.rmem_max                 # expect 16777216
  ls -la /opt/companycollect/corpscout/commoncrawl/cc-dns-worker/
  journalctl -u cc-dns-scan -n 15 --no-pager | grep -iE "cycle|seeded|resolved|load" | tail'
```
Expected: both services active, unbound resolves, 4 NOTRACK rules, raised rmem, a `scan-<cycle>.db` being created, the orchestrator logging progress.

- [ ] **Step 4: Confirm the new features are active + watch first-cycle load**

The service runs with `--axfr --host-enrich` (the new defaults). On the first cycle, watch that the host-load phase runs and the enrichment load is sane:

```bash
ssh root@hetzner01 'journalctl -u cc-dns-scan --no-pager | grep -iE "AXFR probing ENABLED|host-load|registered .* hostnames" | tail'
```
Expected: an `AXFR probing ENABLED` line and, after the seed, a `host-load complete (N discovered hostnames)` line. If the added query/CH-lookup load is ever a problem, dropping `--axfr --host-enrich` from `cc_dns_run_flags` and re-running the play backs out to prod parity. No commit here — operational note.

---

## Self-Review

**Coverage vs. source deployment:** systemd service (Task 4), unbound + tuning + FD drop-in (Task 3), sysctl + limits + NOTRACK persistence (Task 2), binary build/copy + `.env` (Task 4), CH connectivity via Tailscale `companycollect` (env + verify). Every component enumerated in "Source deployment" maps to a task. ✓

**Placeholder scan:** No TBD/TODO; every template/task has literal content. The `REPLACE_WITH_REAL_CH_PASSWORD` in Task 1 Step 4 is an explicit operator action (put the real secret in the vault), not a code placeholder. ✓

**Consistency / fidelity + fixes:** Deploy path, service name, unit fields, unbound directives, sysctl keys, NOTRACK rules, and `.env` keys are copied from the reverse-engineered source. Deliberate deltas, each noted: (a) `net.core.rmem_max/wmem_max` raised so unbound's 8m buffers apply (source clamp bug); (b) `unbound num-threads` scales to target vCPUs (12 vs source's fixed 8); (c) new-feature flags default off with a documented toggle; (d) CH password moved into ansible-vault instead of a plaintext `.env`. Idempotency + a re-run `changed=0` gate is the acceptance test (Task 5 Step 2). ✓
