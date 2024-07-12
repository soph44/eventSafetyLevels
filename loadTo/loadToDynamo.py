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

def checkDynamoTable(tableName, keySchema, attrDef, db_client, db_resource):
        print(":: checkDynamoTable {}".format(tableName))
        response = db_client.list_tables()

        if tableName in response['TableNames']:
            tableFound = True
            print("Table {} Exists".format(tableName))
        else:
            tableFound = False
            # Create the DynamoDB table called followers
            table = db_resource.create_table(
                TableName = tableName,
                KeySchema = keySchema,
                AttributeDefinitions = attrDef,
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                    }
            )

            # Wait until the table exists.
            table.meta.client.get_waiter('table_exists').wait(TableName='tableName')
            print("Table {} Does Not Exists. Created...".format(tableName))

        return tableFound

def connectAWS():
    try:
        s3_client = boto3.client('s3')
        db_client = boto3.client('dynamodb')
        db_resource = boto3.resource('dynamodb', region_name="us-east-2")
    except ClientError as err:
        logger.error("::Couldn't connect to AWS S3 and/or DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

    return (s3_client, db_client, db_resource)

def putDynamoCovid(state, s3_client):
    print("::putDynamoCovid for {}".format(state))
    logger.info("::putDynamoCovid for {}".format(state))
    # read S3 objects put today
    tableName = 'covidtable'
    covidBucket = "sunshine-covidapibucket-dev"
    today = datetime.today().strftime('%Y-%m-%d')
    lastDays = "1"
    covidPrefix = 'covid/' + state + '/'
    key = covidPrefix + today + "_last" + lastDays
    covidResponse = s3_client.get_object(Bucket=covidBucket, Key=key)
    covidBytes = covidResponse['Body'].read()
    # print(covidBytes)
    # print("------")

    # convert to dataframe, format, re-index
    data = json.loads(covidBytes)
    df = pd.json_normalize(data)
    # print(df)
    df = df.drop(df[df['county'].str.contains('out of')].index)
    df = df.drop(df[df['county'] == 'unassigned'].index)
    df['state-county'] = df['province'] + "-" + df['county']
    df['date'] = today
    # df.set_index('date', inplace=True)
    datedrop = df.columns[2].split('timeline.cases.', 1)[1]
    df.rename(columns={'province': 'state', 'timeline.cases.'+datedrop : 'cases', 'timeline.deaths.'+datedrop : 'deaths'}, inplace=True)
    col = df.pop('date')
    df.insert(0, col.name, col)
    col = df.pop('state-county')
    df.insert(1, col.name, col)
    # print(df)

    # dfcsv = df.filter(['state-county', 'state'])
    # dfcsv.to_csv(path_or_buf='region_data.csv', sep=',')

    # put dataframe to DynamoDB using awswrangler
    wr.dynamodb.put_df(df=df, table_name=tableName)

def updateDynamoCovidRates(db_resource):
    # read DynamoDB for today, 1-week ago, 2-week ago, 3-week ago
    # update covidmonthly table for each county
    tableName = 'covidtable'
    tableNameMonthly = 'covidmonthly'
    table = db_resource.Table(tableName)

    cols = ['state-county', 'state', 'cases-today', 'deaths-today',
            'cases-week1', 'deaths-week1',
            'cases-week2', 'deaths-week2',
            'cases-week3', 'deaths-week3']

    today = datetime.today().strftime('%Y-%m-%d')
    week1 = today
    week2 = today
    week3 = today
    # week1 = (datetime.today() - timedelta(days = 7)).strftime('%Y-%m-%d')
    # week2 = (datetime.today() - timedelta(days = 14)).strftime('%Y-%m-%d')
    # week3 = (datetime.today() - timedelta(days = 21)).strftime('%Y-%m-%d')
    pastWeeks = [today, week1, week2, week3]
    print(pastWeeks)

    data = []
    dataList0 = []
    dataList1 = []
    dataList2 = []
    dataList3 = []
    for t in range(4):

        response = table.query(
            TableName=tableName,
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
            print(d)
            for dd in d[i]:
                print(dd)
                dataTemp.update(dd)
        data.append(dataTemp)
        print(dataTemp)

    ds = pd.DataFrame(data, columns=cols)
    # ds.to_csv("test_ds.csv")
    ds['monthly-case-rate'] = 0
    ds['monthly-death-rate'] = 0

    # print(ds) 
    # print("\n")
    for index, row in ds.iterrows():
        if row['cases-week3'] != 0:
            ds.at[index, 'monthly-case-rate'] = (row['cases-today'] - row['cases-week3']) / row['cases-week3']
        if row['deaths-week3'] != 0:
            ds.at[index, 'monthly-death-rate'] = (row['deaths-today'] - row['deaths-week3']) / row['deaths-week3']
    
    # ds.to_csv("test_ds_rates.csv")
    # print(ds)
    wr.dynamodb.put_df(df=ds, table_name=tableNameMonthly)

    return

def putDynamoFlu(s3_client):
    fromBucket = "sunshine-fluapibucket-dev"
    toTable = 'flutable'
    print("::putDynamoFlu")
    logger.info("::putDynamoFlu")

    # read S3 objects put today
    today = datetime.today().strftime('%Y-%m-%d')

    schema = {'release_date': 'object', 'region': 'object', 'num_ili': 'int64', 'num_patients': 'int64', 'ili': 'object'}
    dfAllRegion = pd.DataFrame(columns=schema.keys()).astype(schema)

    for i in range(1, 11):
        print("::putDynamoFlu for Region{}".format(i))
        logger.info("::putDynamoFlu for Region{}".format(i))
        key = "flu/hhs" + str(i) + "/" + today
        fluResponse = s3_client.get_object(Bucket=fromBucket, Key=key)
        fluBytes = fluResponse['Body'].read()
        
        # convert to dataframe, format, re-index
        data = json.loads(fluBytes)
        df = pd.json_normalize(data['epidata'])
        df['ili'] = df['ili'].round(2).astype(str)
        colsKeep = ['release_date', 'region', 'num_ili', 'num_patients', 'ili']
        for col in df:
            if col not in colsKeep:
                df.pop(col)
        dfAllRegion = dfAllRegion._append(df)
        dfAllRegion['date'] = today

    print(dfAllRegion)
    # put dataframe to DynamoDB using awswrangler
    wr.dynamodb.put_df(df=dfAllRegion, table_name=toTable)

    return

def updateDynamoFluRates(db_resource):
    # read DynamoDB for today, 1-week ago, 2-week ago, 3-week ago
    # update covidmonthly table for each county
    tableName = 'flutable'
    tableNameMonthly = 'flumonthly'
    table = db_resource.Table(tableName)

    cols = ['region',
            'today-num_ili',
            'today-num_patients',
            'week1-num_ili',
            'week1-num_patients',
            'week2-num_ili',
            'week2-num_patients',
            'week3-num_ili',
            'week3-num_patients',
            ]

    today = datetime.today().strftime('%Y-%m-%d')
    week1 = today
    week2 = today
    week3 = today
    # week1 = (datetime.today() - timedelta(days = 7)).strftime('%Y-%m-%d')
    # week2 = (datetime.today() - timedelta(days = 14)).strftime('%Y-%m-%d')
    # week3 = (datetime.today() - timedelta(days = 21)).strftime('%Y-%m-%d')
    pastWeeks = [today, week1, week2, week3]
    # print(pastWeeks)

    data = []
    dataList0 = []
    dataList1 = []
    dataList2 = []
    dataList3 = []
    for t in range(4):

        response = table.query(
            TableName=tableName,
            KeyConditionExpression=Key('date').eq(pastWeeks[t])
        )

        for ln in response['Items']:
            print(ln)
            if t == 0:
                dataTemp = [
                    {cols[0]: ln['region'],
                    cols[1]: ln['num_ili'], 
                    cols[2]: ln['num_patients']
                    }]
                dataList0.append(dataTemp)

            else:
                dataTemp = [ 
                    {cols[2+(2*t-1)]: ln['num_ili'], 
                    cols[3+(2*t-1)]: ln['num_patients']}
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
            print(d)
            for dd in d[i]:
                print(dd)
                dataTemp.update(dd)
        data.append(dataTemp)
        print(dataTemp)

    ds = pd.DataFrame(data, columns=cols)
    ds['monthly-case-rate'] = 0

    print(ds) 
    print("\n")
    for index, row in ds.iterrows():
        if row['week3-num_ili'] != 0:
            ds.at[index, 'monthly-case-rate'] = (row['today-num_ili'] - row['week3-num_ili']) / row['week3-num_ili']
    
    wr.dynamodb.put_df(df=ds, table_name=tableNameMonthly)
    return

def main():
    # connect S3 and dynamoDB, check dynamoDB table exists or create
    s3_client, db_client, db_resource = connectAWS()
    state = "alaska"
    # putDynamoCovid(state, s3_client)
    # updateDynamoCovidRates(state, db_resource)
    keySchema1 = [
            {
                'AttributeName': 'date',
                'KeyType': 'HASH'  # Partition Key
            },
            {
                'AttributeName': 'region',
                'KeyType': 'RANGE'  # Sort Key
            }
        ]
    attrDef1 = [
            {
                'AttributeName': 'date',
                'AttributeType': 'S'  # string data type
            },
            {
                'AttributeName': 'region',
                'AttributeType': 'S'  # string data type
            }
        ]
    # checkDynamoTable("flutable", keySchema1, attrDef1, db_client, db_resource)
    # putDynamoFlu(s3_client)

    keySchema2 = [
            {
                'AttributeName': 'region',
                'KeyType': 'HASH'  # Partition Key
            }
        ]
    attrDef2 = [
            {
                'AttributeName': 'region',
                'AttributeType': 'S'  # string data type
            }
        ]
    # checkDynamoTable("flumonthly", keySchema2, attrDef2, db_client, db_resource)
    updateDynamoFluRates(db_resource)
    
    

if __name__ == "__main__":
    main()