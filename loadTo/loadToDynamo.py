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
    #Check if the table name already exists in DynamoDB. If not, create the table.
    logging.info("::checkDynamoTable - Checking/Creating DynamoDB table {}".format(tableName))

    try:
        response = db_client.list_tables()
        status = response['ResponseMetadata']["HTTPStatusCode"]
        logging.info(f"::checkDynamoTable DynamoDB list_tables status: {status}")

        if tableName in response['TableNames']:
            #if found, return True
            tableFound = True
            logging.info("::Table {} Exists".format(tableName))
        else:
            tableFound = False
            #else create table with input schema
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
            logging.info("::checkDynamoTable Table {} Does Not Exists. Created...".format(tableName))

    except ClientError as err:
        logger.error("::checkDynamoTable Error occured with Boto3 or DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

    return tableFound

def connectAWS(region):
    #Creates connection to AWS and creates S3 and DynamoDB resources
    logger.info("::connectAWS - Creating AWS connection and resources")
    try:
        s3_client = boto3.client('s3')
        db_client = boto3.client('dynamodb')
        db_resource = boto3.resource('dynamodb', region_name=region)
    except ClientError as err:
        logger.error("::Couldn't connect to AWS S3 and/or DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

    #return S3 client, DynamoDB Client and Resource
    return (s3_client, db_client, db_resource)

def putDynamoCovid(state, s3_client, tableName, bucketName):
    # Reads from S3 bucket (from today) and transforms/puts data into DynamoDB Covid table
    logger.info("::putDynamoCovid for {}".format(state))

    # read S3 objects put today
    today = datetime.today().strftime('%Y-%m-%d')
    lastDays = "1"
    covidPrefix = 'covid/' + state + '/'
    key = covidPrefix + today + "_last" + lastDays

    try:
        response = s3_client.get_object(Bucket=bucketName, Key=key)
        bytes = response['Body'].read()
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        logging.info(f"::DynamoDB get_object status: {status}")

        # convert to dataframe, format, re-index, and clean
        data = json.loads(bytes)
        df = pd.json_normalize(data)
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

        # put dataframe to DynamoDB using awswrangler
        wr.dynamodb.put_df(df=df, table_name=tableName)
    except ClientError as err:
        logger.error("::Error occured with Boto3 or S3...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

def updateDynamoCovidRates(db_resource, tableName, tableNameMonthly):
    # read DynamoDB for today, 1-week ago, 2-week ago, 3-week ago
    # update covidmonthly table for each county with covid rate data
    logger.info(f"::updateDynamoCovidRates Table1:{tableName}, Table2:{tableNameMonthly}")
    try:
        table = db_resource.Table(tableName)

        cols = ['state-county', 'state', 'cases-today', 'deaths-today',
                'cases-week1', 'deaths-week1',
                'cases-week2', 'deaths-week2',
                'cases-week3', 'deaths-week3']

        today = datetime.today().strftime('%Y-%m-%d')
        week1 = (datetime.today() - timedelta(days = 7)).strftime('%Y-%m-%d')
        week2 = (datetime.today() - timedelta(days = 14)).strftime('%Y-%m-%d')
        week3 = (datetime.today() - timedelta(days = 21)).strftime('%Y-%m-%d')
        pastWeeks = [today, week1, week2, week3]

        data = []
        dataList0 = []
        dataList1 = []
        dataList2 = []
        dataList3 = []
        for t in range(4):
            #Query tables with each date key. Includes all states/countries
            response = table.query(
                TableName=tableName,
                KeyConditionExpression=Key('date').eq(pastWeeks[t])
            )
            if response['Count'] == 0:
                response = table.query(
                TableName=tableName,
                KeyConditionExpression=Key('date').eq(today)
            )

            #append to new array to be converted to dataframe then added to DB table
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
        ds['monthly-case-rate'] = 0
        ds['monthly-death-rate'] = 0

        # perform calculations to create case/death rates for each country
        for index, row in ds.iterrows():
            if row['cases-week3'] != 0:
                ds.at[index, 'monthly-case-rate'] = (row['cases-today'] - row['cases-week3']) / row['cases-week3']
            if row['deaths-week3'] != 0:
                ds.at[index, 'monthly-death-rate'] = (row['deaths-today'] - row['deaths-week3']) / row['deaths-week3']
        
        #Put data into Monthly Covid Rate table
        wr.dynamodb.put_df(df=ds, table_name=tableNameMonthly)

    except ClientError as err:
        logger.error("::Error occured with Boto3 or DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

    return

def putDynamoFlu(s3_client, fromBucket, toTable):
    #Read S3 objects for each US region and put into DynamoDB Table
    logger.info(f"::putDynamoFlu from S3 bucket: {fromBucket} to AWS table: {toTable}")

    # read S3 objects put today
    today = datetime.today().strftime('%Y-%m-%d')
    schema = {'release_date': 'object', 'region': 'object', 'num_ili': 'int64', 'num_patients': 'int64', 'ili': 'object'}
    dfAllRegion = pd.DataFrame(columns=schema.keys()).astype(schema)

    for i in range(1, 11):
        logger.info("::putDynamoFlu for Region{}".format(i))
        try:    
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
        except ClientError as err:
            logger.error("::Error occured with Boto3, S3, or DynamoDB...\n",
                        err.response["Error"]["Code"], \
                        err.response["Error"]["Message"]
                        )
            raise

    # put dataframe to DynamoDB using awswrangler
    wr.dynamodb.put_df(df=dfAllRegion, table_name=toTable)

    return

def updateDynamoFluRates(db_resource, tableName, tableNameMonthly):
    # read DynamoDB for today, 1-week ago, 2-week ago, 3-week ago
    # update covidmonthly table for each county
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
    week1 = (datetime.today() - timedelta(days = 7)).strftime('%Y-%m-%d')
    week2 = (datetime.today() - timedelta(days = 14)).strftime('%Y-%m-%d')
    week3 = (datetime.today() - timedelta(days = 21)).strftime('%Y-%m-%d')
    pastWeeks = [today, week1, week2, week3]

    data = []
    dataList0 = []
    dataList1 = []
    dataList2 = []
    dataList3 = []

    try:
        for t in range(4):
            #Query tables with each date key. Includes all states/countries
            response = table.query(
                TableName=tableName,
                KeyConditionExpression=Key('date').eq(pastWeeks[t])
            )
            if response['Count'] == 0:
                    response = table.query(
                    TableName=tableName,
                    KeyConditionExpression=Key('date').eq(today)
                )
                    
            #append to new array to be converted to dataframe then added to DB table
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

        # perform calculations to determine case rates
        for index, row in ds.iterrows():
            if row['week3-num_ili'] != 0:
                ds.at[index, 'monthly-case-rate'] = (row['today-num_ili'] - row['week3-num_ili']) / row['week3-num_ili']
        
        # put new dataframe into flu rate DB table
        wr.dynamodb.put_df(df=ds, table_name=tableNameMonthly)

    except ClientError as err:
        logger.error("::Error occured with Boto3 or DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

    return

def main():
    # connect S3 and dynamoDB, check dynamoDB table exists or create
    s3_client, db_client, db_resource = connectAWS(region="us-east-2")
    # check necessary tables exist or initiate creation if not
    # f = open("./loadTo/tableSchema.json")
    # data = json.load(f)
    # checkDynamoTable("testtable", data['fluKey'], data['fluAttr'], db_client, db_resource)
    # checkDynamoTable("flumonthly", keySchema2, attrDef2, db_client, db_resource)
    # checkDynamoTable("covidtable", keySchema1, attrDef1, db_client, db_resource)
    # checkDynamoTable("covidmonthly", keySchema2, attrDef2, db_client, db_resource)
    testState = "alaska"
    putDynamoCovid(state=testState, s3_client=s3_client, tableName = "covidTable", bucketName = "sunshine-covidapibucket-dev")
    updateDynamoCovidRates(db_resource=db_resource, tableName = 'covidtable', tableNameMonthly = 'covidmonthly')
    putDynamoFlu(s3_client = s3_client, fromBucket = "sunshine-fluapibucket-dev", toTable = 'flutable')
    updateDynamoFluRates(db_resource, tableName = 'flutable', tableNameMonthly = 'flumonthly')

if __name__ == "__main__":
    main()