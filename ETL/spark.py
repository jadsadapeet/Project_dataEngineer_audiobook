import os
import pymysql.cursors
import pandas as pd
import requests

class Config :
    MYSQL_HOST = "34.136.184.58"
    MYSQL_PORT = 3306
    MYSQL_USER = "r2de2"
    MYSQL_PASSWORD="I_Love_Data_Engineer"
    MYSQL_DB= "r2de2"
    MYSQL_CHARSET= "utf8mb4"

# Connect to  the databese
connection=pymysql.connect(host=Config.MYSQL_HOST,
                            port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db = Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)
#print(connection)

#LIST TABLE
#list all table ด้วย SQL COMMAND (show tables;)
"""cursor = connection.cursor()
cursor.execute(("show tables;"))
tables = cursor.fetchall()
cursor.close()
print(tables)
"""

with connection.cursor() as cursor :
    sql = "select * from audible_data ; "
    cursor.execute(sql)
    result = cursor.fetchall()    #fetchall  ดึงข้อมูล

# pandas
audible_data = pd.DataFrame(result)
audible_data = audible_data.set_index("Book_ID")


with connection.cursor() as cursor :
    sql = "select * from audible_transaction ;"
    cursor.execute(sql)
    result = cursor.fetchall()
audible_transaction = pd.DataFrame(result)

transaction = audible_transaction.merge(audible_data, how ="left",left_on="book_id",right_on="Book_ID") #join

# requests API
url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
r = requests.get(url)
result_conversion_rate = r.json()
conversion_rate = pd.DataFrame(result_conversion_rate)
conversion_rate = conversion_rate.reset_index().rename(columns={"index":"date"})

# แปลง usd เป็น THB

transaction["date"] = transaction["timestamp"]
transaction["date"] = pd.to_datetime(transaction["date"]).dt.date
conversion_rate["date"] = pd.to_datetime(conversion_rate["date"]).dt.date

final_df = transaction.merge(conversion_rate,how="left",left_on="date",right_on="date")
final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""),axis=1)
final_df["Price"] = final_df["Price"].astype(float)

final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
"""
def convert_rate(price,rate):
    return price*rate
final_df["THBprice"] = final_df.apply(lambda x:x[convert_rate(final_df["Price"],final_df["THBprice"])],axis=1)
"""

final_df = final_df.drop("date",axis=1)
final_df.to_csv("output.csv",index=False)


