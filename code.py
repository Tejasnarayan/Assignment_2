import pandas as pd
import json

with open('https://jsonplaceholder.typicode.com/users','r') as f:
    data = json.loads(f.read())

df = pd.json_normalize(data, record_path= ["address"])



