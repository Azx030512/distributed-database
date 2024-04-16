import requests

urls = [
    'http://127.0.0.1:5000/api/rounte/create_table',
    'http://127.0.0.1:5000/api/rounte/read_table',
    'http://127.0.0.1:5000/api/rounte/write_table',
    'http://127.0.0.1:5000/api/rounte/drop_table',
]
table_name = input('table name:')
print(requests.post(urls[3], json={'table_name': table_name, 'estimated_size': 200}).text)
