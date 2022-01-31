import urllib.request, json 
import pandas as pd
import requests
from pandas.io.json import json_normalize
import psycopg2

url = "https://jsonplaceholder.typicode.com/users" 
resp = requests.get(url=url)
df1 = json_normalize(resp.json()) #flattening a json file
df1.rename(columns = {'address.street':'street','address.suite':'suite','address.city':'city',\
	'address.zipcode':'zipcode','address.geo.lat':'latitude','address.geo.lng':'longitude',\
	'company.name':'compname','company.catchPhrase':'compcatchPhrase','company.bs':'compbs'}, inplace=True)
df1.info(verbose=True)	

with urllib.request.urlopen("https://jsonplaceholder.typicode.com/posts") as url:
    data = json.loads(url.read().decode())
    df2 = pd.DataFrame(data)
#df2.info(verbose=True)

with urllib.request.urlopen("https://jsonplaceholder.typicode.com/comments") as url:
    data = json.loads(url.read().decode())
    df3 = pd.DataFrame(data)
#df3.info(verbose=True)

with urllib.request.urlopen("https://jsonplaceholder.typicode.com/todos") as url:
    data = json.loads(url.read().decode())
    df4 = pd.DataFrame(data)
#df4.info(verbose=True)

#converting json file into csv file
df1.to_csv('users.csv', encoding='utf-8', index=False)
df2.to_csv('posts.csv', encoding='utf-8', index=False)
df3.to_csv('comments.csv', encoding='utf-8', index=False)
df4.to_csv('todos.csv', encoding='utf-8', index=False)

#database credentials
conn = psycopg2.connect(database="demodb",user='postgres', password='123',
						host='localhost', port='5432')

conn.autocommit = True
cursor = conn.cursor()

#SQl query to create tables in db
sql1_1 = '''CREATE TABLE USERS(id int NOT NULL, name varchar(50), username varchar(50),\
email varchar(100), phone varchar(100), website varchar(50), street varchar(100), suite varchar(100),\
	city varchar(100), zipcode varchar(10), latitude varchar(20), longitude varchar(20), compname varchar(100),\
		compcatchPhrase varchar(100), compbs varchar(100));'''
cursor.execute(sql1_1)

sql1_2 = '''CREATE TABLE POSTS(userId int NOT NULL, id varchar(20),\
	title varchar(100), body varchar(500));'''
cursor.execute(sql1_2)

sql1_3 = '''CREATE TABLE COMMENTS(postId int NOT NULL, id varchar(20),\
	name varchar(100), email varchar(50), body varchar(500));'''
cursor.execute(sql1_3)

sql1_4 = '''CREATE TABLE TODOS(userId int NOT NULL, id varchar(20),\
	title varchar(200), completed varchar(10));'''
cursor.execute(sql1_4)

#copying data to the tables from csv files
sql2_1 = '''COPY users(id, name, username, email, phone, website, street, suite, city, zipcode, latitude, longitude, compname, compcatchPhrase, compbs)
FROM 'E:/Projects/Assignment_2/users.csv'
DELIMITER ','
CSV HEADER;'''
cursor.execute(sql2_1)

sql2_2 = '''COPY posts(userId, id, title, body)
FROM 'E:/Projects/Assignment_2/posts.csv'
DELIMITER ','
CSV HEADER;'''
cursor.execute(sql2_2)

sql2_3 = '''COPY comments(postId, id, name, email, body)
FROM 'E:/Projects/Assignment_2/comments.csv'
DELIMITER ','
CSV HEADER;'''
cursor.execute(sql2_3)

sql2_4 = '''COPY todos(userId, id, title, completed)
FROM 'E:/Projects/Assignment_2/todos.csv'
DELIMITER ','
CSV HEADER;'''
cursor.execute(sql2_4)

#displaying
sql3_1 = '''select * from users;'''
sql3_2 = '''select * from posts;'''
sql3_3 = '''select * from comments;'''
sql3_4 = '''select * from todos;'''

cursor.execute(sql3_1)
cursor.execute(sql3_2)
cursor.execute(sql3_3)
cursor.execute(sql3_4)

for i in cursor.fetchall():
	print(i)
	
conn.commit()
conn.close()

