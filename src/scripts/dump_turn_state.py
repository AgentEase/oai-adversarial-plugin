"""Dump the X-Codex-Turn-State observations from the plugin's records endpoint."""

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
    payload = json.loads(response.read())

print('version:', payload.get('version'), '| total:', payload.get('total'),
      '| mismatches:', payload.get('mismatches'))
for record in payload.get('records', []):
    print('----')
    print('time:', record.get('time'))
    print('request_id:', record.get('request_id'))
    print('model:', record.get('model'),
          '| upstream:', record.get('upstream_model') or '(none)',
          '| mismatch:', record.get('model_mismatch'))
    print('turn_state_length:', record.get('turn_state_length'))
    print('turn_state_source:', record.get('turn_state_source') or '(none)')
    print('turn_state_value:', record.get('turn_state_value') or '(none)')
    if record.get('turn_state_truncated'):
        print('turn_state_truncated: True')
