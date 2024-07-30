from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import boto3
import logging
import json
from decimal import Decimal

'''
Provides functions for dashApp to access AWS DynamoDB data on Covid and Flu infection rates
'''

#Helper function for json dump 
def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        return int(obj)
    raise TypeError("Type not serializable")

#Connect to AWS DynamoDB
def connectDynamo(logger, region):
    try:
        db_resource = boto3.resource('dynamodb', region_name=region)
    except ClientError as err:
        logger.error("::connectDynamo Couldn't connect to AWS DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise
    logging.info(f"::connectDynamo successful")
    
    return db_resource

#Get dataframe of covid monthly rates
def get_df_covid(county, state, db_resource, logger, tableName):
    county = county.lower()
    state = state.lower()
    stateCounty = state + '-' + county
    try:
        table = db_resource.Table(tableName)
        response = table.get_item(Key={"state-county": stateCounty, "state": state})
        if not 'Item' in response:
            logger.error('::Item not found in AWS Database::')
            raise
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        keep = {"monthly-death-rate", "deaths-today", "monthly-case-rate", "cases-today"}
        rename ={"cases-today":"daily-covid-cases", "deaths-today":"daily-covid-deaths", "monthly-case-rate":"monthly-covid-case-rate", "monthly-death-rate":"monthly-covid-death-rate"}
        data = {rename.get(key,key) : val for key ,val in response["Item"].items() if key in keep}
        for key in data:
            data[key] = decimal_serializer(data[key])
    except ClientError as err:
        logger.error(
            "::get_df Couldn't retrieve items from DynamoDB -- ",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise
    else:
        logging.info(f"::get_def_covid for {county}-{state} Covid monthly Successful")
        return data, status

#Get dataframe of flu monthly rates 
def get_df_flu(state, db_resource, logger, tableName, hhsRegion):
    state = state.lower()
    region = hhsRegion[state]
    key = "hhs" + str(region)

    try:
        table = db_resource.Table(tableName)
        response = table.get_item(Key={"region": key})
        if not 'Item' in response:
            logger.error('::Item not found in AWS Database::')
            raise
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        keep = {"today-case-rate", "week3-case-rate"}
        data = {key : val for key ,val in response["Item"].items() if key in keep}
        for key in data:
            data[key] = decimal_serializer(data[key])
    except ClientError as err:
        logger.error(
            "::get_df_flu Couldn't retrieve items from DynamoDB -- ",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise
    else:
        logging.info(f"::get_def for {state}, HHS {region} Flu monthly Successful")
        return data, status

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    db_resource = connectDynamo(logger, "us-east-2")
    county = "Alameda"
    state = "California"

    # diseaseData = get_df_flu(state, db_resource, logger, "flumonthly", 9)
    covidDict = get_df_covid(county=county, 
                                state=state, 
                                db_resource=db_resource, 
                                logger=logger, 
                                tableName='covidmonthly')[0]
    
