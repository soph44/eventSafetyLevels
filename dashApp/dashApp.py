import dashApp
from dashApp import dash_table, Input, Output, State, html, dcc, callback
import dash_daq as daq
from dash.exceptions import PreventUpdate
import pandas as pd
import numpy as np
import os
import logging
import api as a
import awsdb as db
import json

logger = logging.getLogger(__name__)
logging.basicConfig(filename='python_log.log', encoding='utf-8', level=logging.DEBUG)

db_resource = db.connectDynamo(logger)

asst_path = os.path.join(os.getcwd(), "assets")
app = dashApp.Dash(
    __name__,
    # meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    # assets_folder=asst_path,
)
app.title = "Event Attendance Safety Levels"
server = app.server
app.config["suppress_callback_exceptions"] = True


# ===== html object builds =====

def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5(app.title),
                    html.H6("Integrated Display of Covid & Influenze in the U.S. by Event"),
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

def build_tabs():
    return html.Div(
        id="tabs",
        className="tabs",
        children=[
            dcc.Tabs(
                id="app-tabs",
                value="tab2",
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
        children=[
        html.Div(
            id="banner-event-id",
            children=["Event ID Input"],
        ),
        dcc.Input(
            id="event-id",
            className="input",
            type="text",
            placeholder="Type in Event Brite EventID here...",
            debounce=True,
            
        ),
        html.Div(
            html.Button('Submit', id="submit-event-button", n_clicks=0),
        ),
        ],
        className="inputEvent",
    )

def build_state_input():
    return html.Div(
        children=[
        dcc.Input(
            id="state-id",
            className="input",
            type="text",
            placeholder="Type in State here...",
            debounce=True
        ),
        dcc.Input(
            id="county-id",
            className="input",
            type="text",
            placeholder="Type in County here...",
            debounce=True
        ),
        html.Div(
            html.Button('Submit', id="submit-state-button", n_clicks=0)
        ),
        html.Div(id="validate-state")
        ]
    )

def build_covid_panel():
    return html.Div([
        html.Div(
            id="event-info-cases",
            className="panelCovid",
            style={"justifyContent": "center"},
            children=[
                html.Div(
                id="led-1",
                children=[
                    html.P("Current Cases"),
                    daq.LEDDisplay(
                        id="led-covid-cases",
                        value="000000",
                        color="#FFFF00",
                        backgroundColor="#000000",
                        size=50,
                        ),
                    ],
                    style={"display":"inline-block"},
                ),
                html.Div(
                    id="bar-1",
                    children=[
                        html.P("Monthly Case Rate Change"),
                        daq.GraduatedBar(
                            id="bar-covid-cases",
                            max=300,
                            min=-300,
                            value=125,
                            step=1,
                            showCurrentValue=True,  # default size 200 pixel
                        ),
                    ],
                    style={"display":"inline-block"},
                ),

            ],
        ),
        html.Div(
            id="event-info-deaths",
            className="panelCovid",
            style={"justifyContent": "center"},
            children=[
                html.Div(
                    id="led-2",
                    children=[
                        html.P("Current Deaths"),
                        daq.LEDDisplay(
                            id="led-covid-deaths",
                            value="000000",
                            color="#FF0000",
                            backgroundColor="#000000",
                            size=50,
                            ),
                    ],
                    style={"display":"inline-block"},
                ),
                html.Div(
                    id="bar-2",
                    children=[
                        html.P("Monthly Death Rate Change"),
                        daq.GraduatedBar(
                            id="bar-covid-deaths",
                            max=300,
                            min=-300,
                            value=125,
                            step=1,
                            showCurrentValue=True,
                        ),
                    ],
                    style={"display":"inline-block"},
                ),
            ],
        ),
    ],
    style={"display": "flex", "align-items":"center"}
    )

def build_faq_tab():
    return html.Div(
        id="faq",
        children=[
            html.H1("INFO 1"),
            html.H2("INFO 2"),
            html.H3("INFO 3"),
        ]
    )

def init_df():
    data = {"daily-covid-cases":0, "daily-covid-deaths":0, "monthly-covid-case-rate":0, "monthly-covid-death-rate":0}
    dataJson = json.dumps(data, indent=4)
    return dataJson

def init_event():
    data = {"name":"NULL", "description":"NULL", "start":"", "end":"", "venue_id":"", "logo":""}
    dataJson = json.dumps(data, indent=4)
    return dataJson

# Validate the state/county entered is valid before attempting pull from database
def validate_entry():
    return

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
        dcc.Store(id="covid-flu-data", data=init_df()),
        dcc.Store(id="event-data", data=init_event())
    ],
)

#=====callback to build tabs on tab select=====
@app.callback(
    [Output("app-content", "children")],
    [Input("app-tabs", "value")],
)
def render_tab_content(tab_switch):
    img_src=app.get_asset_url("test_map.jpeg")
    if tab_switch == "tab1":
        return(
            html.Div(
                id="by-event-container",
                children=[
                    html.Div(
                        id="input-panel",
                        children=[
                            build_eventid_input(),
                        ],
                    ),
                    html.Div(
                        id="covid-flu-panel",
                        children=[
                            build_covid_panel(),
                            html.Div("TBD FLU INFO")
                        ],
                    ),
                    html.Div(
                        id="map-panel",
                        children=[
                            html.Div("TBD MAP PANEL"),
                            html.Img(src=img_src, style={'height':'10%', 'width':'10%'}),
                        ],
                    ),
                    html.Div(
                        id="event-info-panel",
                        children=[
                            html.Div("TBD Event Info PANEL"),
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
                            html.Div(
                                id="banner-state-id",
                                children=["Event State Input"]
                            ),
                            build_state_input(),
                        ],
                    ),
                    html.Div(
                        id="covid-flu-panel",
                        children=[
                            build_covid_panel(),
                            html.Div("TBD FLU INFO")
                        ],
                    ),
                    html.Div(
                        id="map-panel",
                        children=[
                            html.Div("TBD MAP PANEL"),
                            html.Img(src=img_src, style={'height':'10%', 'width':'10%'}),
                        ],
                    ),
                    html.Div(
                        id="event-info-panel",
                        children=[
                            html.Div("TBD Event Info PANEL"),
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
            Output("covid-flu-data", "data"), 
            Output("event-data", "data"), 
            Output("submit-event-button", "n_clicks"),
            Output("led-covid-cases", "value"),
            Output("led-covid-deaths", "value"),
            Output("bar-covid-cases", "value"),
            Output("bar-covid-deaths", "value")
            ],
    inputs=[Input("submit-event-button", "n_clicks")],
    state=[
        State("event-id", "value")
    ]
)
def update_data_to_event(n_clicks, eventID):
    if (n_clicks == 1):
        key = a.authenticate_eventbrite()
        event, venue = a.get_eventbrite(key, eventID)

        county = venue['county']
        state = venue['state']
        diseaseDict = db.get_df(county, state, db_resource, logger)
        covidCases=diseaseDict['daily-covid-cases']
        covidDeaths=diseaseDict['daily-covid-deaths']
        covidCaseRate=diseaseDict['monthly-covid-case-rate']*100
        covidDeathRate=diseaseDict['monthly-covid-death-rate']*100
        print("diseaseDict = ", diseaseDict)
    else:
        raise PreventUpdate
    return diseaseDict, event, None, covidCases, covidDeaths, covidCaseRate, covidDeathRate

# ====== Callbacks to update stored data via click, State =====
# @app.callback(
#     output=[Output("covid-flu-data", "data"), Output("submit-state-button", "n_clicks")],
#     inputs=[Input("submit-state-button", "n_clicks")],
#     state=[
#         State("county-id", "value"), State("state-id", "value")
#     ]
# )
# def update_data_to_state(n_clicks, county, state):
#     if (n_clicks == 1):
#         diseaseJson = db.get_df(county, state, db_resource, logger)
#     else:
#         raise PreventUpdate
#     return diseaseJson, None

if __name__ == '__main__':
    app.run(debug=True)