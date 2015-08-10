from swh.storage import store
from swh.protocols import serial
from swh.http import client

r = client.put('http://localhost:5000', store.Type.origin, {'url': 'https://github.com/swh/good.git', 'type': git})
