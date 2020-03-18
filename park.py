import pandas as pd
# import the dataset
df = pd.read_csv('./data.csv', encoding = 'unicode_escape')
# print(df.head())
# split a column in two
df[["country1","country2"]] = df.Country.str.split(' ',n=1,expand=True)
print(df.head())  
df.to_csv(r'./modify.csv', index = False,header=True)
