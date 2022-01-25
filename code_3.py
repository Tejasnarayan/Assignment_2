import urllib.request, json 
import pandas as pd
import psycopg2

with urllib.request.urlopen("https://jsonplaceholder.typicode.com/posts") as url:
    data = json.loads(url.read().decode())
    df = pd.DataFrame(data)

#df.to_csv('posts.csv', encoding='utf-8', index=False)


conn = psycopg2.connect(database="demodb",
						user='postgres', password='123',
						host='localhost', port='5432')

conn.autocommit = True
cursor = conn.cursor()

sql = '''CREATE TABLE DETAILS(user_id int NOT NULL, post_id varchar(20),\
post_title varchar(100), post_body varchar(500));'''


cursor.execute(sql)

sql2 = '''COPY details(user_id,post_id, post_title,post_body)
FROM 'E:/Projects/Assignment_2/posts.csv'
DELIMITER ','
CSV HEADER;'''

cursor.execute(sql2)

sql3 = '''select * from details;'''
cursor.execute(sql3)
for i in cursor.fetchall():
	print(i)

conn.commit()
conn.close()
