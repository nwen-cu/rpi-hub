import os
import sys
import re
import json
from pathlib import Path
import requests

update_url = 'https://raw.githubusercontent.com/nwen-cu/rpi-hub/main/update.py'
main_url = 'https://raw.githubusercontent.com/nwen-cu/rpi-hub/main/main.py'
config_url = 'https://raw.githubusercontent.com/nwen-cu/rpi-hub/main/config.json'

update_dir = Path('~/update').expanduser()
config_path = Path('~/config.json').expanduser()
main_path = Path('~/main.py').expanduser()

if not update_dir.exists():
    os.mkdir('/update')

# Retrieve key and endpoint from config file
# Update from v3.x
if not config_path.exists():
    with open(main_path, 'r') as fp:
        fs = fp.read()

    m = re.search("endpoint.*=.*'(.*)'", fs)
    endpoint = m.groups(1)[0]
    m = re.search("key.*=.*'(.*)'", fs)
    key = m.groups(1)[0]
# Update from v4.x
else:
    with open(config_path, 'r') as fp:
        config = json.load(fp)
    
    endpoint = config['endpoint']
    key = config['key']

# Download config.json
# requests.get()


# with urllib.request.urlopen(main_url) as fp:

# Check if v3.x
# Get key and endpoint from old main.py
# Download config.json
# Update key and endpoint in config.json
# Backup old main.py
# Download main.py
