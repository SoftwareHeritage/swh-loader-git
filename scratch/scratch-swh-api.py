from swh.storage import store
from swh.protocols import serial
from swh.client import http

r = http.put('http://localhost:5000', store.Type.origin, {'url': 'https://github.com/swh/good.git', 'type': git})
