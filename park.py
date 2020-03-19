import pandas as pd
import pandasql as ps
import fastparquet
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
all_distinct_date = "select distinct(date) from df"
# date = '12-1-2010';
# all_ochurance_in_a_day = 'select * from df where date = "'+date+'"'

date = ps.sqldf(all_distinct_date, locals())
# print(date_all)

# make directory
parent_dir = "/home/sh-root/Desktop/practice/parque/result/"
for index,item in date.iterrows():
    directory = item['date']
    path = os.path.join(parent_dir, directory) 
    os.mkdir(path)
    date_name = directory
    all_ochurance_in_a_day = 'select * from df where date = "'+date_name+'"'
    date_all = ps.sqldf(all_ochurance_in_a_day, globals())

    all_distinct_hour = 'select distinct(time) from date_all'

    distinct_time_of_a_day = ps.sqldf(all_distinct_hour, globals())

    for index,val in distinct_time_of_a_day.iterrows():
        inside_dir = val['time']
        inside_path = path+'/'
        time_path = os.path.join(inside_path,inside_dir) 
        os.mkdir(time_path)
        all_occurance_in_a_hour = "select * from date_all where time = '"+inside_dir+"'"
        all_data_of_an_hour = ps.sqldf(all_occurance_in_a_hour, globals())
        
        put_per = time_path+'/'+inside_dir+'.parquet.gzip'
        all_data_of_an_hour.to_parquet(put_per,compression='gzip') 
        # print(all_data_of_an_hour)
        # break
    #for csv
    # put_file = path+'/'+date_name+".csv"
    #for perquite
    # print(put_file)
    # date_all.to_csv(put_file,index = False,header = True)
    # put_per = time_path+'/'+inside_dir+'.parquet.gzip'

    # date_all.to_parquet(put_per,compression='gzip') 

print("ok")
# print(df.head())  
# df.to_csv(r'./modify.csv', index = False,header=True)
