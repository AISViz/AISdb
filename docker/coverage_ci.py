#!/usr/bin/python3

import requests

coverage_html = requests.get(
    'https://aisdb.meridian.cs.dal.ca/coverage/index.html')

coverage = '0%'
for line in coverage_html.content.decode().split('\n'):
    if "%" in line:
        coverage = line.rsplit()[-1].split('>', 1)[1].split('<', 1)[0]

print(f'COVERAGE: {coverage}')
