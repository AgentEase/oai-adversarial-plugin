"""Fetch audit records to locate and print the newest full 292-byte turn-state value."""

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

api = urllib.request.Request('http://127.0.0.1:8317/v0/management/timezone-override/requests',
                             headers={'Authorization': 'Bearer ' + key})
with urllib.request.urlopen(api, timeout=8) as response:
    payload = json.loads(response.read())

records = payload.get('records', [])
print('plugin version:', payload.get('version'), '| total:', payload.get('total'),
      '| records kept:', len(records))
print()
for index, record in enumerate(records):
    print('== record', index, '| time:', record.get('time'),
          '| model:', record.get('model'), '| upstream:', record.get('upstream_model'),
          '| mismatch:', record.get('model_mismatch'),
          '| length:', record.get('turn_state_length'),
          '| source:', record.get('turn_state_source'))
    for name, value in sorted(record.items()):
        if 'turn' in name.lower():
            print('   ', name, '=>', value)
    print()
