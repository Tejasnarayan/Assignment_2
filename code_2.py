import pandas as pd
import json

json_string = 'https://jsonplaceholder.typicode.com/users'

a_json = json.loads(json_string)

dataframe = pd.DataFrame.from_dict(a_json, orient="index")
print (dataframe)