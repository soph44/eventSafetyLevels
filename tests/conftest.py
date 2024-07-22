import pytest
import boto3
from dashApp import api

@pytest.fixture
def ex_eventbrite_id():
    # first key is existing eventID (Rattleship 2024), second key is not an eventID
    return "893792827407", "893792827409"

@pytest.fixture
def ex_eventbrite_data():
    eventData, venueData = api.get_eventbrite(api.authenticate_eventbrite, ex_eventbrite_key()[1])
    return eventData, venueData

@pytest.fixture
def bucket_name_covid():
    return "sunshine-covidapibucket-dev"

@pytest.fixture
def table_name_covid():
    return "covidtable", "covidmonthly"

@pytest.fixture
def ex_test_state():
    return "alaska"

@pytest.fixture
def aws_resource():
    region = "us-east-2"
    s3_client = boto3.client('s3')
    db_client = boto3.client('dynamodb')
    db_resource = boto3.resource('dynamodb', region_name=region)
    return s3_client, db_client, db_resource