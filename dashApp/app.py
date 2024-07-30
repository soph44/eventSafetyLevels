import os
from dash import Dash, Input, Output, State, html, dcc, callback
import dash_daq as daq
from dash.exceptions import PreventUpdate
import pandas as pd
import logging
import api as a
import awsdb as db
import json

'''
File: app.py
Author: SonnyP
Description: Flask/Dash dashboard application for integrated Covid & Flu risk at event locations.
'''

logger = logging.getLogger(__name__)
logging.basicConfig(filename='python_log.log', encoding='utf-8', level=logging.DEBUG)
db_resource = db.connectDynamo(logger=logger, region="us-east-2")

asst_path = os.path.join(os.getcwd(), "assets")
app = Dash(
    __name__,
    # meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    # assets_folder=asst_path,
)
app.title = "Event Attendance Safety Levels"
server = app.server
app.config["suppress_callback_exceptions"] = True

#Load tablenames from awstables.json for functions to access correct AWS DynamoDB Tables
awsNames = json.load(open(asst_path + '/awstables.json'))
covidbucket = awsNames.get('covidbucket')
covidtable = awsNames.get('covidtable')
covidmonthly = awsNames.get('covidmonthlytable')
flubucket = awsNames.get('flubucket')
flutable = awsNames.get('flutable')
flumonthly = awsNames.get('flumonthlytable')

#Load list of states and correlating regions for functions to access correct AWS DynamoDB Tables
statesCsv = pd.read_csv(asst_path + '/statesPartial.csv', usecols=['State', 'Region'])
hhsDict = dict(zip(statesCsv.State, statesCsv.Region))
statesList = statesCsv['State'].tolist()

# ===== html object builds =====
def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H1(app.title),
                    html.H2("Integrated Display of Covid & Influenze in the U.S. by Event"),
                ],
                style={"textAlign":"center"},
            ),
            html.Div(
                id="banner-logo",
                children=[
                    html.A(
                        html.Img(id="logo", src=""),
                        href="",
                    ),
                ],
                style={"textAlign":"center"},
            ),
        ],
    )

def build_faq_tab():
    return html.Div(
        id="faq",
        className="panelLabel",
        children=[
            html.H1("FAQ"),
            html.P("This brower dashboard was created as a data engineering expercise.\n\
                   This application utilizes an ETL pipeline using Java to request Covid and Flu \n\
                   information from separate api-exposed databases and puts those responses into AWS S3 buckets.\n\
                   This is then processed by a Python transformation/loading script into AWS DynamoDB tables.\n\
                   This dashboard pulls relavent data from the DynamoDB tables based on the EventID or Location of interest.\n\
                   An additional map and event details are also dynamically pulled from EventBrite and Google Map APIs."),
            html.Br(),
            html.H5("Author: SonnyP"),
        ],
    )

def build_tabs():
    return html.Div(
        id="tabs",
        className="tabs",
        children=[
            dcc.Tabs(
                id="app-tabs",
                value="tab3",
                className="custom-tabs",
                children=[
                    dcc.Tab(
                        id="by-event-tab",
                        label="Information by Event",
                        value="tab1",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                    ),
                    dcc.Tab(
                        id="by-state-tab",
                        label="Information by State",
                        value="tab2",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                    ),
                    dcc.Tab(
                        id="faq-tab",
                        label="Additional Information",
                        value="tab3",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                    ),
                ],
            )
        ],
    )

def build_eventid_input():
    return html.Div(
        className="inputEvent",
        children=[
        dcc.Input(
            id="event-id",
            className="input",
            type="number",
            placeholder="Type in Event Brite EventID number here...",
            debounce=True,
            persistence=True,
            style={"width":"25vw", "margin":"2px"},

        ),
        html.Div(
            html.Button('Submit', id="submit-event-button", n_clicks=0),
            style={"margin":"5px"},
        ),
        ],
    )

def build_state_input():
    return html.Div(
        className="inputEvent",
        children=[
        dcc.Dropdown(
            options=statesList,
            id="state-id",
            className="input",
            placeholder="Select State from Dropdown...",
            persistence=True,
            style={"width":"25vw", "margin":"2px"},
        ),
        dcc.Input(
            id="county-id",
            className="input",
            type="text",
            placeholder="Type in County here...",
            debounce=True,
            persistence=True,
            style={"width":"25vw", "margin":"2px"},
        ),
        html.Div(
            html.Button('Submit', id="submit-state-button", n_clicks=0),
            style={"margin":"5px"},
        ),
        html.Div(id="validate-state")
        ]
    )

def build_covid_panel():
    return html.Div([
        html.Div(
            id="covid-event-info-cases",
            className="panelCovid",
            children=[
                html.Div(
                id="led-covid",
                className="panelLED",
                children=[
                    html.H4("Current Cases"),
                    daq.LEDDisplay(
                        id="led-covid-cases",
                        value="000000",
                        color="#FFFF00",
                        backgroundColor="#000000",
                        size=50,
                        ),
                    ],
                ),
                html.Br(),
                html.Div(
                    id="bar-covid",
                    children=[
                        html.H4("Monthly Case Rate Change"),
                        daq.GraduatedBar(
                            id="bar-covid-cases",
                            max=300,
                            min=-300,
                            value=50,
                            step=1,
                            showCurrentValue=True,  # default size 200 pixel
                        ),
                    ],
                    style={"display":"inline-block", "align-items":"center"},
                ),

            ],
        ),
        html.Div(
            id="covid-event-info-deaths",
            className="panelCovid",
            style={"justifyContent": "center"},
            children=[
                html.Div(
                    id="led-2",
                    className="panelLED",
                    children=[
                        html.H4("Current Deaths"),
                        daq.LEDDisplay(
                            id="led-covid-deaths",
                            value="000000",
                            color="#FF0000",
                            backgroundColor="#000000",
                            size=50,
                            ),
                    ],
                ),
                html.Br(),
                html.Div(
                    id="bar-2",
                    children=[
                        html.H4("Monthly Death Rate Change"),
                        daq.GraduatedBar(
                            id="bar-covid-deaths",
                            max=300,
                            min=-300,
                            value=50,
                            step=1,
                            showCurrentValue=True,
                        ),
                    ],
                    style={"display":"inline-block", "align-items":"center"},
                ),
            ],
        ),
    ],
    style={"display": "flex", "align-items":"center"}
    )

def build_flu_panel():
    return html.Div([
        html.Div(
            id="flu-event-info-cases",
            className="panelFlu",
            children=[
                html.Div(
                id="led-flu-1",
                className="panelLED",
                children=[
                    html.H4("Current Flu Patient Cases"),
                    daq.LEDDisplay(
                        id="led-flu-cases-today",
                        value="000000",
                        color="#FFFF00",
                        backgroundColor="#000000",
                        size=50,
                        ),
                    ],
                ),
            ],
            ),
        html.Div(
            id="flu-event-info-cases2",
            className="panelFlu",
            children=[
                html.Div(
                    id="led-flu-2",
                    className="panelLED",
                    children=[
                        html.H4("Previous Month Flu Patient Cases"),
                        daq.LEDDisplay(
                            id="led-flu-cases-week3",
                            value="000000",
                            color="#0000FF",
                            backgroundColor="#000000",
                            size=50,
                            ),
                    ],
                ),
            ],
        ),
    ],
    style={"display": "flex", "align-items":"center"}
    )

def build_map_panel():
    return html.Div(
        id="map-panel",
        className="panelMap",
        children=[
            html.Div(
                id="map-data",
                children=[
                    html.Img(src="assets/default-image.png",
                             id="map-img",)
                ],
            ),
        ],
    )

def build_event_info_panel():
    return html.Div(
        id="event-info-panel",
        className="panelInfo",
        children=[
            html.H4("Event Name:"),
            html.Div(
                id="event-name"),
            html.Br(),
            html.H4("Event Description:"),
            html.Div(
                id="event-description"),
            html.Br(),
            html.H4("Start Time:"),
            html.Div(
                id="start-time"),
            html.Br(),
            html.H4("Venue ID:"),
            html.Div(
                id="venue-id"),
            html.Br(),
            html.H4("Artwork:"),
            html.Img(src="",
                    id="event-img",)
        ]
    )

# TODO: Add persistance so data does not refresh on tab switches
# def init_covid_df():
#     data = {"daily-covid-cases":0, "daily-covid-deaths":0, "monthly-covid-case-rate":0, "monthly-covid-death-rate":0}
#     dataJson = json.dumps(data, indent=4)
#     return dataJson

# def init_flu_df():
#     data = {"today-case-rate":0, "week3-case-rate":0}
#     dataJson = json.dumps(data, indent=4)
#     return dataJson

# def init_event():
#     data = {"name":"NULL", "description":"NULL", "start":"", "venue_id":"", "logo":""}
#     dataJson = json.dumps(data, indent=4)
#     return dataJson

# def init_map():
    # image_path = "assest/default-image.png"
    # return html.Img(src=image_path)

# ===== call app layout =====
app.layout = html.Div(
    id="big-app-container",
    children=[
        build_banner(),
        html.Div(
            id="app-container",
            children=[
                build_tabs(),
                # Main app
                html.Div(id="app-content"),
            ],
        ),
        # dcc.Store(id="covid-data", data=init_covid_df()),
        # dcc.Store(id="flu-data", data=init_flu_df()),
        # dcc.Store(id="event-data", data=init_event()),
        # dcc.Store(id="map-data", data=init_map()),

    ],
)

#=====callback to build tabs on tab select=====
@app.callback(
    [Output("app-content", "children")],
    [Input("app-tabs", "value")],
)
def render_tab_content(tab_switch):
    if tab_switch == "tab1":
        return(
            html.Div(
                id="by-event-container",
                children=[
                    html.Div(
                        id="input-panel",
                        children=[
                            html.H3("Event ID Input",
                                    className="panelLabel"),
                            build_eventid_input(),
                        ],
                    ),
                    html.Div(
                        id="covid-flu-panel",
                        children=[
                            html.H3("Covid & Flu Data",
                                    className="panelLabel"),
                            build_covid_panel(),
                            build_flu_panel()
                        ],
                    ),
                    html.Div(
                        id="map-panel",
                        children=[
                            html.H3("Local Area Map",
                                    className="panelLabel"),
                            build_map_panel(),
                        ],
                    ),
                    html.Div(
                        id="event-info-panel",
                        children=[
                            html.H3("Event Information",
                                    className="panelLabel"),
                            build_event_info_panel(),
                        ],
                    ),
                ],
            ),
        )
    elif tab_switch == "tab2":
        return(
            html.Div(
                id="by-state-container",
                children=[
                    html.Div(
                        id="input-panel",
                        children=[
                            html.H3("Event State/County Input",
                                    className="panelLabel"),
                            build_state_input(),
                        ],
                    ),
                    html.Div(
                        id="covid-flu-panel",
                        children=[
                            html.H3("Covid & Flu Data",
                                    className="panelLabel"),
                            build_covid_panel(),
                            build_flu_panel(),
                        ],
                    ),
                    html.Div(
                        id="map-panel",
                        children=[
                            html.H3("Local Area Map",
                                    className="panelLabel"),
                            build_map_panel(),
                        ],
                    ),
                ],
            ),
        )
    elif tab_switch == "tab3":
        return(
            html.Div(
                id="faq-container",
                children=[
                    build_faq_tab(),
                ],
            ),
        )

# ====== Callbacks to update stored data via click, EventID =====
@app.callback(
    output=[
            # Output("covid-data", "data"),
            # Output("flu-data", "data"), 
            # Output("event-data", "data"), 
            # Output("map-img", "src"),
            Output("submit-event-button", "n_clicks"),
            Output("led-covid-cases", "value", allow_duplicate=True),
            Output("led-covid-deaths", "value", allow_duplicate=True),
            Output("bar-covid-cases", "value", allow_duplicate=True),
            Output("bar-covid-deaths", "value", allow_duplicate=True),
            Output("led-flu-cases-today", "value", allow_duplicate=True),
            Output("led-flu-cases-week3", "value", allow_duplicate=True),
            Output("map-img", "src", allow_duplicate=True),
            Output("event-name", "children"),
            Output("event-description", "children"),
            Output("start-time", "children"),
            Output("venue-id", "children"),
            Output("event-img", "src"),
            ],
    inputs=[Input("submit-event-button", "n_clicks")],
    state=[
        State("event-id", "value")
    ],
    prevent_initial_call=True
)
def update_data_to_event(n_clicks, eventID):
    if (n_clicks == 1):
        # Update Covid Data
        keyEventBrite = a.authenticate_eventbrite()
        event, venue = a.get_eventbrite(keyEventBrite, eventID)[0:2]

        county = venue['county']
        state = venue['state']
        covidDict = db.get_df_covid(county=county, 
                                state=state, 
                                db_resource=db_resource, 
                                logger=logger, 
                                tableName=covidmonthly)[0]

        covidDict['monthly-covid-case-rate'] *= 100
        covidDict['monthly-covid-death-rate'] *= 100
        covidCases=covidDict['daily-covid-cases']
        covidDeaths=covidDict['daily-covid-deaths']
        covidCaseRate=covidDict['monthly-covid-case-rate']
        covidDeathRate=covidDict['monthly-covid-death-rate']
        logging.info("::update_data_to_event: covidDict = ", covidDict)
        
        #Update Flu Data
        fluDict = db.get_df_flu(state=state,
                                  db_resource=db_resource,
                                  logger=logger,
                                  tableName=flumonthly,
                                  hhsRegion=hhsDict)[0]
        fluCasesToday = fluDict['today-case-rate']
        fluCasesPast = fluDict['week3-case-rate']
        logging.info("::update_data_to_event: fluDict = ", fluDict)
   
        #Update Map Data
        keyMap = a.authenticate_map()
        img = a.get_map(keyMap, venue)[0]
        f = open("./dashApp/assets/staticmap.png", "wb")
        f.write(img)
        f.close
        imgUrl = "/assets/staticmap.png"

        #Update Event Data
        eventName = event['name']
        eventDesc = event['description']
        eventTime = event['start']
        eventVenue = event['venue_id']
        eventArtUrl = event['logo']

    else:
        raise PreventUpdate
    return None, \
        covidCases, covidDeaths, covidCaseRate, covidDeathRate, \
        fluCasesToday, fluCasesPast, imgUrl, \
        eventName, eventDesc, eventTime, eventVenue, eventArtUrl

# ====== Callbacks to update stored data via click, State/County =====
@app.callback(
    output=[
            # Output("covid-data", "data"),
            # Output("flu-data", "data"), 
            # Output("event-data", "data"), 
            # Output("map-img", "src"),
            Output("submit-state-button", "n_clicks"),
            Output("led-covid-cases", "value"),
            Output("led-covid-deaths", "value"),
            Output("bar-covid-cases", "value"),
            Output("bar-covid-deaths", "value"),
            Output("led-flu-cases-today", "value"),
            Output("led-flu-cases-week3", "value"),
            Output("map-img", "src"),
            ],
    inputs=[Input("submit-state-button", "n_clicks")],
    state=[
        State("state-id", "value"),
        State("county-id", "value")
    ],
    prevent_initial_call=True
)
def update_data_to_event(n_clicks, stateID, countyID):
    if (n_clicks == 1):
        # Update Covid Data
        county = countyID
        state = stateID
        covidDict = db.get_df_covid(county=county, 
                                state=state, 
                                db_resource=db_resource, 
                                logger=logger, 
                                tableName=covidmonthly)[0]

        covidDict['monthly-covid-case-rate'] *= 100
        covidDict['monthly-covid-death-rate'] *= 100
        covidCases=covidDict['daily-covid-cases']
        covidDeaths=covidDict['daily-covid-deaths']
        covidCaseRate=covidDict['monthly-covid-case-rate']
        covidDeathRate=covidDict['monthly-covid-death-rate']
        logging.info("::update_data_to_event: covidDict = ", covidDict)
        
        #Update Flu Data
        fluDict = db.get_df_flu(state=state,
                                  db_resource=db_resource,
                                  logger=logger,
                                  tableName=flumonthly,
                                  hhsRegion=hhsDict)[0]
        fluCasesToday = fluDict['today-case-rate']
        fluCasesPast = fluDict['week3-case-rate']
        logging.info("::update_data_to_event: fluDict = ", fluDict)
   
        #Update Map Data
        keyMap = a.authenticate_map()
        img = a.get_map(keyMap, str(county + " " + state))[0]
        f = open("./dashApp/assets/staticmap.png", "wb")
        f.write(img)
        f.close
        imgUrl = "/assets/staticmap.png"

    else:
        raise PreventUpdate
    return None, \
        covidCases, covidDeaths, covidCaseRate, covidDeathRate, \
        fluCasesToday, fluCasesPast, imgUrl

if __name__ == '__main__':
    app.run(debug=True)