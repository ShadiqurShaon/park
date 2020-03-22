import pandas as pd
import pandasql as ps
import fastparquet
import os

# import the dataset
df = pd.read_csv('./data.csv', encoding = 'unicode_escape')

# split invoicedate column into seperate date and time
df[["date","time"]] = df.InvoiceDate.str.split(' ',n=1,expand=True)

#configure the date formate
df[['date']] = df.date.str.replace('/','-')

# Get all distinct date from the data set
all_distinct_date = "select distinct(date) from df"
all_distinct_date_df = ps.sqldf(all_distinct_date, locals())

# Make parennt directory
os.mkdir('result')
parent_dir = 'result/'

# Make all Directory by Date
total = len(all_distinct_date_df.index)
for index,item in all_distinct_date_df.iterrows():
    date_dir_name  = item['date']
    #join path with parent
    path = os.path.join(parent_dir, date_dir_name)
    os.mkdir(path)

    #Get all transection on a unique date
    all_ochurance_in_a_day = 'select * from df where date = "'+date_dir_name+'"'
    all_ochurance_in_a_day_df = ps.sqldf(all_ochurance_in_a_day, globals())

    # Get all distinct houre of transection in a unique date
    all_distinct_hour_in_a_day = 'select distinct(time) from all_ochurance_in_a_day_df'
    all_distinct_hour_in_a_day_df = ps.sqldf(all_distinct_hour_in_a_day, globals())
    
    #Make Dir with houre
    for index,val in all_distinct_hour_in_a_day_df.iterrows():
        hour_dir_name = val['time']
        inside_date_folder = path+'/'
        
        # Time folder path
        time_dir_path = os.path.join(inside_date_folder,hour_dir_name)
        os.mkdir(time_dir_path)

        # All transection in an hour
        all_occurance_in_a_hour = "select * from all_ochurance_in_a_day_df where time = '"+hour_dir_name+"'"
        all_occurance_in_a_hour_df = ps.sqldf(all_occurance_in_a_hour, globals())

        # Make perquite file nad put inside the hour folder
        put_per = time_dir_path+'/'+hour_dir_name+'.parquet.gzip'
        all_occurance_in_a_hour_df.to_parquet(put_per,compression='gzip')
    total = total-1
    print('Number of case left ----->>>',total) 