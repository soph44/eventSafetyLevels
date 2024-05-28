import dash
from dash import dash_table, Input, Output, State, html, dcc, callback
import dash_daq as daq
import pandas as pd
import numpy as np
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='python_log.log', encoding='utf-8', level=logging.DEBUG)

asst_path = os.path.join(os.getcwd(), "assets")
app = dash.Dash(
    __name__,
    # meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    assets_folder=asst_path
)
app.title = "Event Attendance Safety Levels"
server = app.server
app.config["suppress_callback_exceptions"] = True

def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5(app.title),
                    html.H6("Integrated Display of Covid & Influenze in the U.S."),
                ],
            ),
            html.Div(
                id="banner-logo",
                children=[
                    html.A(
                        html.Img(id="logo", src=""),
                        href="",
                    ),
                ],
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
        dcc.Input(
            id="eventid",
            className="input",
            type="text",
            placeholder="Type in EventID here...",
            debounce=True
        ),
        dcc.Dropdown(
            id="ticket-api",
            options=["Ticket Master", "Live Nation"],
            
        ),
        html.Div(
            html.Button('Submit', id="submit-event-button", n_clicks=0)
        )
        ]
    )

def build_state_input():
    return html.Div(
        children=[
        dcc.Input(
            id="stateid",
            className="input",
            type="text",
            placeholder="Type in State here...",
            debounce=True
        ),
        html.Div(
            html.Button('Submit', id="submit-state-button", n_clicks=0)
        ),
        html.Div(id="validate-state")
        ]
    )

def build_covid_panel():
    return html.Div(
        id="event-info",
        className="row",
        children=[
            html.Div(
                id="card-1",
                children=[
                    html.P("Current Cases"),
                    daq.LEDDisplay(
                        id="operator-led",
                        value="1704",
                        color="#92e0d3",
                        backgroundColor="#1e2130",
                        size=50,
                    ),
                ],
            ),
            html.Div(
                id="card-1",
                children=[
                    html.P("Current Cases"),
                    daq.LEDDisplay(
                        id="operator-led",
                        value="1704",
                        color="#92e0d3",
                        backgroundColor="#1e2130",
                        size=50,
                    ),
                ],
            ),
            html.Div(
                id="gauge-1",
                children=[
                    html.P("Monthly Case Rate Change"),
                    daq.Gauge(
                        id="progress-gauge",
                        max=300,
                        min=0,
                        showCurrentValue=True,  # default size 200 pixel
                        units="%"
                    ),
                ],
            ),
            html.Div(
                id="gauge-2",
                children=[
                    html.P("Monthly Death Rate Change"),
                    daq.Gauge(
                        id="progress-gauge",
                        max=300,
                        min=0,
                        showCurrentValue=True,  # default size 200 pixel
                        units="%"
                    ),
                ],
            ),
        ],
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
# def build_flu_panel():

# def build_event_map_panel():
#     return mp

def init_df():
    cols = ["covid-cases-today", "covid-deaths-today", "monthly-covid-case-rate", "monthly-covid-death-rate"]
    zero_data = np.zeroes(len(cols))
    empty_df = pd.DataFrame()
    df = pd.DataFrame(zero_data, columns=cols)
    return df

def get_df(dbClient, state, county, date, tableName):
    try:
        table = dbClient.Table(tableName)
        response = table.get_item(Key={"state-county": county})
    except ClientError as err:
        logger.error(
            "::Couldn't retrieve items from DynamoDB -- ",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise
    else:
        return response["Item"]
    return df

def connectDynamo():
    try:
        db_client = boto3.client('dynamodb', region_name="us-east-2")
    except ClientError as err:
        logger.error("::Couldn't connect to AWS DynamoDB...\n",
                    err.response["Error"]["Code"], \
                    err.response["Error"]["Message"]
                    )
        raise

    return db_client

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
        dcc.Store(id="covid-flu-data", data=get_df)
    ],
)

#===callback to build tabs===
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
                            html.Div(
                                id="banner-event-id",
                                children=["Event ID Input"]
                            ),
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
    output=Output("covid-flu-data", "data"),
    inputs=[Input("submit-event-button", "n_clicks")],
    state=[
        State("metric-select-dropdown", "value"),
        State("value-setter-store", "data"),
    ],
)
def set_value_setter_store(set_btn, param, data):
    usl = daq.NumericInput(
        id="ud_usl_input", className="setting-input", size=200, max=9999999
    )
    lsl = daq.NumericInput(
        id="ud_lsl_input", className="setting-input", size=200, max=9999999
    )
    ucl = daq.NumericInput(
        id="ud_ucl_input", className="setting-input", size=200, max=9999999
    )
    lcl = daq.NumericInput(
        id="ud_lcl_input", className="setting-input", size=200, max=9999999
    )
    if set_btn is None:
        return data
    else:
        data[param]["usl"] = usl
        data[param]["lsl"] = lsl
        data[param]["ucl"] = ucl
        data[param]["lcl"] = lcl

        # Recalculate ooc in case of param updates
        data[param]["ooc"] = populate_ooc(df[param], ucl, lcl)
        return data


@app.callback(
    output=Output("value-setter-view-output", "children"),
    inputs=[
        Input("value-setter-view-btn", "n_clicks"),
        Input("metric-select-dropdown", "value"),
        Input("value-setter-store", "data"),
    ],
)
def show_current_specs(n_clicks, dd_select, store_data):
    if n_clicks > 0:
        curr_col_data = store_data[dd_select]
        new_df_dict = {
            "Specs": [
                "Upper Specification Limit",
                "Lower Specification Limit",
                "Upper Control Limit",
                "Lower Control Limit",
            ],
            "Current Setup": [
                curr_col_data["usl"],
                curr_col_data["lsl"],
                curr_col_data["ucl"],
                curr_col_data["lcl"],
            ],
        }
        new_df = pd.DataFrame.from_dict(new_df_dict)
        return dash_table.DataTable(
            style_header={"fontWeight": "bold", "color": "inherit"},
            style_as_list_view=True,
            fill_width=True,
            style_cell_conditional=[
                {"if": {"column_id": "Specs"}, "textAlign": "left"}
            ],
            style_cell={
                "backgroundColor": "#1e2130",
                "fontFamily": "Open Sans",
                "padding": "0 2rem",
                "color": "darkgray",
                "border": "none",
            },
            css=[
                {"selector": "tr:hover td", "rule": "color: #91dfd2 !important;"},
                {"selector": "td", "rule": "border: none !important;"},
                {
                    "selector": ".dash-cell.focused",
                    "rule": "background-color: #1e2130 !important;",
                },
                {"selector": "table", "rule": "--accent: #1e2130;"},
                {"selector": "tr", "rule": "background-color: transparent"},
            ],
            data=new_df.to_dict(orient="records"),
            columns=[{"id": c, "name": c} for c in ["Specs", "Current Setup"]],
        )


# decorator for list of output
def create_callback(param):
    def callback(interval, stored_data):
        count, ooc_n, ooc_g_value, indicator = update_count(
            interval, param, stored_data
        )
        spark_line_data = update_sparkline(interval, param)
        return count, spark_line_data, ooc_n, ooc_g_value, indicator

    return callback


for param in params[1:]:
    update_param_row_function = create_callback(param)
    app.callback(
        output=[
            Output(param + suffix_count, "children"),
            Output(param + suffix_sparkline_graph, "extendData"),
            Output(param + suffix_ooc_n, "children"),
            Output(param + suffix_ooc_g, "value"),
            Output(param + suffix_indicator, "color"),
        ],
        inputs=[Input("interval-component", "n_intervals")],
        state=[State("value-setter-store", "data")],
    )(update_param_row_function)







if __name__ == '__main__':
    app.run(debug=True)