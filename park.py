import pandas as pd
import pandasql as ps
import os
# step1
# import the dataset
df = pd.read_csv('./data.csv', encoding = 'unicode_escape')

# step2
# split datetime column in two individual column date and time
df[["date","time"]] = df.InvoiceDate.str.split(' ',n=1,expand=True)
df[['date']] = df.date.str.replace('/','-')

#query string for data

# qu = "Select distinct(date) from df"
# qu_by_date = "Select * from df where date = '12/1/2010'"
all_distinct_date = "select distinct(date) from df limit 10"
# date = '12-1-2010';
# all_ochurance_in_a_day = 'select * from df where date = "'+date+'"'

date = ps.sqldf(all_distinct_date, locals())
# print(date_all)

# make directory
parent_dir = "/home/sh-root/Desktop/practice/parque/ecommerce-data/"
for index,item in date.iterrows():
    directory = item['date']
    path = os.path.join(parent_dir, directory) 
    # os.mkdir(path)
    date_name = directory
    all_ochurance_in_a_day = 'select * from df where date = "'+date_name+'" limit 10'
    date_all = ps.sqldf(all_ochurance_in_a_day, globals())
    print(date_all)
    # put_file = path+'/'+date+".csv"
    # print(put_file)
    # date_all.to_csv(put_file,index = False,header = True)

print("ok")
# print(df.head())  
# df.to_csv(r'./modify.csv', index = False,header=True)
