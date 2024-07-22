import json
from eventbrite import Eventbrite
import requests
import pandas as pd
import logging

'''
Provides functions for making API calls to EventBrite and Mapbox
Functions:
    authenticate_eventbrite
    get_eventbrite
    authenticate_map
    get_map
'''

logger = logging.getLogger(__name__)
logging.basicConfig(filename='python_log.log', encoding='utf-8', level=logging.DEBUG)

def authenticate_eventbrite():
    data = json.load(open('./keys/apikey.json'))
    key = data.get('eventbrite')
    
    return key

def get_eventbrite(key, eventID):
    info = ["name", "description", "start", "end", "venue_id", "logo"]
    info_base = ["text", "text", "local", "local"]
    info_logo = ["url"]

    eb = Eventbrite(key)
    event = eb.get_event(eventID)
    print(event)
    if 'status_code' in event:
        status = event['status_code']
        logging.info("::get_eventbrite Error - ", status, event['error_description'])
        raise #TODO: create custom exceptions for API error codes
    else:
        status = "200"

    eventData = {}
    for i in range(len(info)):
        if (i < 4):
            item = {info[i]:event[info[i]][info_base[i]]}
            eventData.update(item)
        if (i == 4):
            item = {info[i]:event[info[i]]}
            eventData.update(item)
        if (i == 5):
            item = {info[i]:event[info[i]][info_logo[0]]}
            eventData.update(item)
    # eventJson = json.dumps(eventData, indent=4)

    #Venue endpoint not available in Python SDK
    venueID = eventData['venue_id']
    venueUrl = "https://www.eventbriteapi.com/v3/venues/"
    venueUrl += venueID
    header = {"Authorization": "Bearer " + key}
    venueResponse = requests.get(venueUrl, headers=header).json()

    if 'status_code' in venueResponse:
        logging.info("::get_eventbrite Error - ", event['status_code'], event['error_description'])
        raise #TODO: create custom exceptions for API error codes

    venueAddress = venueResponse['address']['localized_address_display']
    venueZipcode = venueResponse['address']['postal_code']

    df = pd.read_csv("./refTables/geo_data.csv")
    data = df[df['zipcode'] == venueZipcode]
    venueState = data['state'].item()
    venueCounty = data['county'].item()
    
    venueData = {
        "address": venueAddress,
        "state": venueState,
        "zipcode": venueZipcode,
        "county": venueCounty
        }
    # venueJson = json.dumps(venueData, indent=4)

    return eventData, venueData, status

def authenticate_map():
    data = json.load(open('./keys/apikey.json'))
    key = data.get('googlemap')
    return key

def get_map(key, venueData):

    mapUrl = "https://maps.googleapis.com/maps/api/staticmap?"
    location = ""
    for d in venueData:
        if d != "county":
            location += str(venueData[d]) + ", "
        location = location[:-2]
    zoom = "10"
    size = "400x400"
    parameter = f"center={location}&format=png&zoom={zoom}&size={size}&key={key}"
    mapUrl += parameter
    mapResponse = requests.get(mapUrl)
    if mapResponse.status_code != 200:
        logging.info("::get_map Error - ", mapResponse.status_code)
    else:
        f = open("testmap.png", "wb")
        f.write(mapResponse.content)
    
    return mapResponse.status_code

if __name__ == "__main__":
    ebkey = authenticate_eventbrite()
    eventID = "893792827407"
    eventData, venueData = get_eventbrite(ebkey, eventID)
    mapkey = authenticate_map()
    get_map(mapkey, venueData)