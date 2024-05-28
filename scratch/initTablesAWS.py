import pandas as pd
import numpy as np
import awswrangler as wr

df = pd.read_csv(filepath_or_buffer='region_data.csv', sep=',')
df = df.filter(['state-county', 'state'])
cols = ['cases-today', 'deaths-today', 'cases-week1', 'deaths-week1', 'cases-week2', 'deaths-week2', 'cases-week3', 'deaths-week3', 'cases-rate', 'deaths-rate']

for i in cols:
    df[i] = 0

wr.dynamodb.put_df(df=df, table_name='covidmonthly')