import urllib.request, json 
import pandas as pd
import csv

with urllib.request.urlopen("https://jsonplaceholder.typicode.com/posts") as url:
    data = json.loads(url.read().decode())
    df = pd.DataFrame(data)

df.to_csv('posts.csv', encoding='utf-8', index=False)

