import pytest
from loadTo import loadToDynamo
from dashApp import api
from dashApp import awsdb
import json

def test_eventbrite_api(ex_eventbrite_id):
    ebKey = api.authenticate_eventbrite()
    assert api.get_eventbrite(ebKey, ex_eventbrite_id[0])[2] == "200"

    return

def test_map_api(ex_eventbrite_id):
    ebKey = api.authenticate_eventbrite()
    venueData = api.get_eventbrite(ebKey, ex_eventbrite_id[0])[1]
    mapKey = api.authenticate_map()
    assert api.get_map(mapKey, venueData) == 200

    return

def test_put_dyanmo_state(ex_test_state, table_name_covid, bucket_name_covid, aws_resource):
    s3_client = aws_resource[0]
    assert loadToDynamo.putDynamoCovid(state = ex_test_state, 
                                        s3_client = s3_client,
                                        tableName = table_name_covid[0],
                                        bucketName = bucket_name_covid) == 200

    return
