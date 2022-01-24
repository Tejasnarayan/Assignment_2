import urllib.request, json 
import pandas as pd

with urllib.request.urlopen("https://jsonplaceholder.typicode.com/users") as url:
    data = json.loads(url.read().decode())
    df = pd.DataFrame(data)
print(df)