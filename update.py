import os
import re
import json
from pathlib import Path
import requests
import shutil

update_url = 'https://raw.githubusercontent.com/nwen-cu/rpi-hub/main/update.py'
main_url = 'https://raw.githubusercontent.com/nwen-cu/rpi-hub/main/main.py'
config_url = 'https://raw.githubusercontent.com/nwen-cu/rpi-hub/main/config.json'

update_dir = Path('~/update').expanduser()
config_file = Path('~/config.json').expanduser()
main_file = Path('~/main.py').expanduser()
update_file = Path('~/update.py').expanduser()

print('Checking update of this script')
r = requests.get(update_url)

require_update = False
with open(update_file, 'r') as fp:
    if r.text != fp.read():
        require_update = True

if require_update:
    print('Updating this script')
    with open(update_file, 'w') as fp:
        fp.write(r.text)
    print('Updated, please re-run this script')
    exit()


if not update_dir.exists():
    print('Creating update workspace')
    os.mkdir(Path('~/update').expanduser())

# Retrieve key and endpoint from config file
# Update from v3.x
if not config_file.exists():
    print('Migrating from old version(~v3.x)')
    with open(main_file, 'r') as fp:
        fs = fp.read()
    print('Retrieving config from script file')
    m = re.search("endpoint.*=.*'(.*)'", fs)
    endpoint = m.groups(1)[0]
    m = re.search("key.*=.*'(.*)'", fs)
    key = m.groups(1)[0]
# Update from v4.x
else:
    print('Retrieving config from config file')
    with open(config_file, 'r') as fp:
        config = json.load(fp)
    
    endpoint = config['endpoint']
    key = config['key']

# Download main.py
print('Downloading new script file')
r = requests.get(main_url)

with open(update_dir / 'main.py', 'w') as fp:
    fp.write(r.text)

# Download config.json
print('Downloading new config file')
r = requests.get(config_url)

# Update key and endpoint in config.json
print('Writing config file')
config = json.loads(r.text)
config['endpoint'] = endpoint
config['key'] = key

with open(update_dir / 'config.json', 'w') as fp:
    json.dump(config, fp)

# Backup old main.py and config.json
print('Backuping old version')
shutil.move(main_file, update_dir / 'main.py.bak')
if config_file.exists():
    shutil.move(config_file, update_dir / 'config.json.bak')

print('Applying update')
shutil.move(update_dir / 'main.py', main_file)
shutil.move(update_dir / 'config.json', config_file)

print('Done')

