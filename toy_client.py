import requests

url = 'http://127.0.0.1:5000/api/rounte/create_table'
new_table_name = input('new table name:')
requests.post(url, json={'table_name':new_table_name, 'estimated_size':200})