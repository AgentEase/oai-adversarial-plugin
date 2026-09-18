"""Show the probe engine state after the v1.5.0 deployment."""

import json
import subprocess
import urllib.request

environment = dict(
    item.split('=', 1)
    for item in json.loads(subprocess.run(
        ['docker', 'inspect', 'cpa-usage-keeper'], check=True,
        stdout=subprocess.PIPE, universal_newlines=True).stdout)[0]['Config']['Env']
    if '=' in item
)
key = environment['CPA_MANAGEMENT_KEY']

request = urllib.request.Request('http://127.0.0.1:8317/v0/management/timezone-override/requests',
                                 headers={'Authorization': 'Bearer ' + key})
with urllib.request.urlopen(request, timeout=8) as response:
    data = json.loads(response.read())

print('plugin version:', data.get('version'))
probe = (data.get('turn_state_override') or {}).get('probe') or {}
print('probe enabled:', probe.get('enabled'), '| running:', probe.get('running'),
      '| total:', probe.get('probes_total'), '| ok:', probe.get('probes_ok'))
print('last_error:', (probe.get('last_error') or '')[:160])
print('last_activity:', probe.get('last_activity') or '')
print()
print('== per-model values ==')
for value in probe.get('values', []):
    print('  {model:<14} len={length:<5} source={source:<8} expires={expires}'.format(
        model=value.get('model', '?'), length=value.get('value_length', 0),
        source=value.get('source') or '-', expires=value.get('expires_at') or '-'))
print()
print('== recent probe history ==')
for record in probe.get('history', [])[:12]:
    status = 'OK ' if record.get('success') else 'FAIL'
    detail = (record.get('error') or '')[:90]
    print('  {time} {status} {model:<14} {proxy:<48} {ms:>6}ms {detail}'.format(
        time=(record.get('time') or '')[:19], status=status, model=record.get('model', '?'),
        proxy=(record.get('proxy') or '')[:48], ms=record.get('duration_ms', 0), detail=detail))
print()
print('== rewrite override ==')
override = data.get('turn_state_override') or {}
print('  enabled:', override.get('enabled'), '| force:', override.get('force'),
      '| models:', override.get('models'), '| value_length:', override.get('value_length'))
