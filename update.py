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

if not update_dir.exists():
    os.mkdir('/update')

# Retrieve key and endpoint from config file
# Update from v3.x
if not config_file.exists():
    with open(main_file, 'r') as fp:
        fs = fp.read()

    m = re.search("endpoint.*=.*'(.*)'", fs)
    endpoint = m.groups(1)[0]
    m = re.search("key.*=.*'(.*)'", fs)
    key = m.groups(1)[0]
# Update from v4.x
else:
    with open(config_file, 'r') as fp:
        config = json.load(fp)
    
    endpoint = config['endpoint']
    key = config['key']

# Download main.py
r = requests.get(main_url)

with open(update_dir / 'main.py', 'w') as fp:
    fp.write(r.text)

# Download config.json
r = requests.get(config_url)

# Update key and endpoint in config.json
config = json.loads(r.text)
config['endpoint'] = endpoint
config['key'] = key

with open(update_dir / 'config.json', 'w') as fp:
    json.dump(config, fp)

# Backup old main.py and config.json
shutil.move(main_file, update_dir / 'main.py.bak')
shutil.move(config_file, update_dir / 'config.json.bak')

shutil.move(update_dir / 'main.py', main_file)
shutil.move(update_dir / 'config.json', config_file)


