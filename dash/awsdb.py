from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import boto3
import logging
import json
from decimal import Decimal

def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Type not serializable")

def connectDynamo(logger):
    try:
        db_resource = boto3.resource('dynamodb', region_name="us-east-2")
    except ClientError as err:
        logger.error("::Couldn't connect to AWS DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

    return db_resource

def get_df(county, state, db_resource, logger):
    county = county.lower()
    state = state.lower()
    tableNameMonthly = 'covidmonthly'
    stateCounty = state + '-' + county
    try:
        table = db_resource.Table(tableNameMonthly)
        response = table.get_item(Key={"state-county": stateCounty, "state": state})
        if not 'Item' in response:
            logger.error('::Item not found in AWS Database::')
            raise
        keep = {"monthly-death-rate", "deaths-today", "monthly-case-rate", "cases-today"}
        rename ={"cases-today":"daily-covid-cases", "deaths-today":"daily-covid-deaths", "monthly-case-rate":"monthly-covid-case-rate", "monthly-death-rate":"monthly-covid-death-rate"}
        diseaseData = {rename.get(key,key) : val for key ,val in response["Item"].items() if key in keep}
        # dataJson = json.dumps(data, indent=4, default=decimal_serializer)
    except ClientError as err:
        logger.error(
            "::Couldn't retrieve items from DynamoDB -- ",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise
    else:
        return diseaseData

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    db_resource = connectDynamo(logger)
    county = "Alameda"
    state = "California"
    diseaseData = get_df(county, state, db_resource, logger)
    print(diseaseData)