#!/usr/bin/env python3

import requests

response = requests.get('http://localhost:5000/commits/1')

print(response)
