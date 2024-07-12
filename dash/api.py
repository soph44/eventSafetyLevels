import json
from eventbrite import Eventbrite
import requests
import pandas as pd

'''
Provides functions for making API calls to EventBrite and Mapbox
Functions:
    authenticate_eventbrite
    get_eventbrite
    authenticate_map
    get_map
'''
def authenticate_eventbrite():
    #Note, Ticketmaster calls are made with api key directly in URL, no authentification needed
    data = json.load(open('./keys/apikey.json'))
    key = data.get('eventbrite')

    return key

def get_eventbrite(key, eventID):
    info = ["name", "description", "start", "end", "venue_id", "logo"]
    info_base = ["text", "text", "local", "local"]
    info_logo = ["url"]
    eb = Eventbrite(key)
    event = eb.get_event(eventID)

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
    venueAddress = venueResponse['address']['localized_address_display']
    venueZipcode = venueResponse['address']['postal_code']

    df = pd.read_csv("./refTables/geo_data.csv")
    data = df[df['zipcode'] == venueZipcode]
    venueState = data['state'].item()
    venueCounty = data['county'].item()
    
    venueData = {
        "address": venueAddress,
        "zipcode": venueZipcode,
        "state": venueState,
        "county": venueCounty
        }
    # venueJson = json.dumps(venueData, indent=4)

    return eventData, venueData

def authenticate_map():

    return

def get_map():

    return

if __name__ == "__main__":
    key = authenticate_eventbrite()
    eventID = "893792827407"
    eventData, venueData = get_eventbrite(key, eventID)
    print(eventData)
    
