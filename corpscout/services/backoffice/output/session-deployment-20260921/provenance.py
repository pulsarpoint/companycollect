from pathlib import Path
import subprocess,json,hashlib
root=Path('..')
result={}
for folder,package in [('browser_service','browser_service'),('company_research','company_research')]:
 script=f'''import json,hashlib\nfrom pathlib import Path\nimport {package}\nroot=Path({package}.__file__).parent\nprint(json.dumps({{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*.py')}}))'''
 remote=json.loads(subprocess.check_output(['ssh','graovic@192.168.88.132','sudo',f'/opt/companycollect/corpscout/{folder}/current/.venv/bin/python','-'],text=True,input=script))
 local={str(p.relative_to(root/folder/'src'/package)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (root/folder/'src'/package).rglob('*.py')}
 if remote!=local:raise SystemExit(f'Deployed source differs for {folder}: {set(remote.items())^set(local.items())}')
 result[folder]={'modules':len(local),'sourceMatches':True}
print(json.dumps(result))
Path('output/session-deployment-20260921/provenance.json').write_text(json.dumps(result,indent=2))
