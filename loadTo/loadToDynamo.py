import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import json
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import awswrangler as wr
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='python_log.log', encoding='utf-8', level=logging.DEBUG)

state = "california"
lastDays = "1"
covidBucketName = "sunshine-covidapibucket-dev"
covidPrefix = 'covid/' + state + '/'
# fluBucketName = ""
# fluPrefix = ""
today = datetime.today().strftime('%Y-%m-%d')
yesterday = datetime.today() - timedelta(days = 1)
yesterday = yesterday.strftime('%Y-%m-%d')
datedb = today
key = covidPrefix + datedb + "_last" + lastDays
tableName = 'covidtable'
tableNameMonthly = 'covidmonthly'

# def putDynamo():
def connectAWS():
    try:
        s3_client = boto3.client('s3')
        db_client = boto3.client('dynamodb')
    except ClientError as err:
        logger.error("::Couldn't connect to AWS S3 and/or DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

    return (s3_client, db_client)

def loadDynamo(state, date, s3_client, fromBucket, toTable):
    return

def main():
    # connect S3 and dynamoDB, check dynamoDB table exists or create
    s3_client, db_client = connectAWS()
    
    # read S3 objects put today
    covidResponse = s3_client.get_object(Bucket=covidBucketName, Key=key)
    covidBytes = covidResponse['Body'].read()
    # print(covidBytes)
    # fluResponse = s3_client.get_object(Bucket=fluBucketName, Prefix=fluPrefix, Key=key)
    # fluBytes = fluResponse['Body'].read()

    # convert to dataframe, format, re-index
    data = json.loads(covidBytes)
    df = pd.json_normalize(data)
    df = df.drop(df[df['county'] == 'out of ca'].index)
    df = df.drop(df[df['county'] == 'unassigned'].index)
    df['state-county'] = df['province'] + "-" + df['county']
    df['date'] = datedb
    # df.set_index('date', inplace=True)
    datedrop = df.columns[2].split('timeline.cases.', 1)[1]
    df.rename(columns={'province': 'state', 'timeline.cases.'+datedrop : 'cases', 'timeline.deaths.'+datedrop : 'deaths'}, inplace=True)
    col = df.pop('date')
    df.insert(0, col.name, col)
    col = df.pop('state-county')
    df.insert(1, col.name, col)
    
    # dfcsv = df.filter(['state-county', 'state'])
    # dfcsv.to_csv(path_or_buf='region_data.csv', sep=',')

    # put dataframe to DynamoDB using awswrangler
    wr.dynamodb.put_df(df=df, table_name=tableName)




    # read DynamoDB for today, 1-week ago, 2-week ago, 3-week ago
    # update covidmonthly table for each county
    cols = ['state-county', 'state', 'cases-today', 'deaths-today',
            'cases-week1', 'deaths-week1',
            'cases-week2', 'deaths-week2',
            'cases-week3', 'deaths-week3']

    today = datetime.today().strftime('%Y-%m-%d')
    week1 = (datetime.today() - timedelta(days = 7)).strftime('%Y-%m-%d')
    week2 = (datetime.today() - timedelta(days = 14)).strftime('%Y-%m-%d')
    week3 = (datetime.today() - timedelta(days = 21)).strftime('%Y-%m-%d')
    pastWeeks = [today, week1, week2, week3]
    # print(pastWeeks)

    dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
    table = dynamodb.Table(tableName)

    data = []
    dataList0 = []
    dataList1 = []
    dataList2 = []
    dataList3 = []
    for t in range(4):

        response = table.query(
            TableName='covidtable',
            KeyConditionExpression=Key('date').eq(pastWeeks[t])
        )

        for ln in response['Items']:
            if t == 0:
                dataTemp = [
                    {cols[0]: ln['state-county'],
                    cols[1]: ln['state'], 
                    cols[2]: ln['cases'], 
                    cols[3]: ln['deaths']
                    }]
                dataList0.append(dataTemp)

            else:
                dataTemp = [ 
                    {cols[2+2*t]: ln['cases'], 
                    cols[3+2*t]: ln['deaths']}
                    ]
                if t == 1:
                    dataList1.append(dataTemp)
                elif t == 2:
                    dataList2.append(dataTemp)
                elif t == 3:
                    dataList3.append(dataTemp)
 
    for i in range(len(dataList0)):
        dataTemp = dataList0[i][0]
        dicts = [dataList1, dataList2, dataList3]
        for d in dicts:
            for dd in d[i]:
                dataTemp.update(dd)
        data.append(dataTemp)

    ds = pd.DataFrame(data, columns=cols)
    # ds.to_csv("test_ds.csv")
    ds['monthly-case-rate'] = 0
    ds['monthly-death-rate'] = 0

    print(ds) 
    print("\n")
    for index, row in ds.iterrows():
        if row['cases-week3'] != 0:
            ds.at[index, 'monthly-case-rate'] = (row['cases-today'] - row['cases-week3']) / row['cases-week3']
        if row['deaths-week3'] != 0:
            ds.at[index, 'monthly-death-rate'] = (row['deaths-today'] - row['deaths-week3']) / row['deaths-week3']
    
    print(ds)
    wr.dynamodb.put_df(df=ds, table_name=tableNameMonthly)

if __name__ == "__main__":
    main()