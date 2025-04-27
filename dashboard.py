import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import json
import requests
from PIL import Image
from io import BytesIO
import os
from dotenv import load_dotenv
load_dotenv()

import sqlite3
from dash.dependencies import MATCH, ALL, State, Output, Input
# from airflow.hooks.base import BaseHook  # For redis connection
import redis
from datetime import datetime
import dateutil.parser  # Handy for parsing ISO strings with offset
import openai
from datetime import datetime, timezone
# --- PydanticAI Agent to Extract Bus Service Number ---
from pydantic import BaseModel
from pydantic_ai import Agent
from supabase import create_client
from ultralytics import YOLO
from collections import Counter

import dateutil.parser  # Handy for parsing ISO strings with offsets


# Set your API key and credentials
API_KEY= os.environ.get("OPENAI_KEY")
SUPABASE_URL= os.environ.get("SUPABASE_URL")
SUPABASE_KEY= os.environ.get("SUPABASE_KEY")

conn = sqlite3.connect('bus.db')

# Load data from SQLite
bus_stops_df = pd.read_sql_query("SELECT * FROM BusStop", conn)
bus_services_df = pd.read_sql_query("SELECT * FROM BusService", conn)
bus_routes_df = pd.read_sql_query("SELECT * FROM BusRoute", conn)

# Load data from Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
df = supabase.table("TrafficImages").select("*").execute()
df = pd.DataFrame(df.data)

# Load YOLO model
model = YOLO("yolov8n.pt")

# Connect to Redis and get upcoming bus arrivals
# redis_conn = BaseHook.get_connection("redis_default")
# r = redis.Redis(host=redis_conn.host, port=redis_conn.port, decode_responses=True)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
all_redis_items = r.lrange("bus_arrivals", 0, -1)
# Create dropdown options using bus stop description and code
bus_stops_list = [{'label': row['Description'], 'value': row['BusStopCode']} for _, row in bus_stops_df.iterrows()]

conn.close()

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Extend layout with a new store for selected bus stop and the chatbot section.
app.layout = dbc.Container(
    [
        # Title and Search Bar Section
        html.Div(
            [
                html.H1("Singapore Public Transport Dashboard", id='dashboard-title', className="text-center mb-4"),
                dcc.Tabs(id='tabs-example-1', value='tab-1', children=[
                    dcc.Tab(label='Bus Route', value='tab-1'),
                    dcc.Tab(label='Traffic Condition', value='tab-2'),
                    dcc.Tab(label='Monthly Taxi Fleet', value='tab-3'),
                ])
            ],
            className="title-search-container"
        ),

        # Tab 1
        html.Div(
            id='tab-1-content',
            children = [
                dcc.Dropdown(
                            id='bus-stop-search',
                            options=bus_stops_list,
                            placeholder="Search for a bus stop...",
                            searchable=True,
                            clearable=True,
                            style={'width': '100%', 'padding': '10px', 'font-size': '1.2rem'},
                            className='search-dropdown'
                        ),
                
                # Map and Bus Information Panel
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Graph(
                                id='map',
                                config={'scrollZoom': True}
                            ),
                            width=8
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.H4('Upcoming Buses', className="card-title"),
                                        html.Ul(id='bus-list', children=[])
                                    ]
                                ),
                            ),
                            width=4
                        ),
                    ],
                    className="mb-4",
                ),
                
                # Toggle button and collapsible slider/plot section (tap-in/out plots)
                dbc.Button(
                    "Toggle Tap-in/Out Data",
                    id="collapse-button",
                    color="primary",
                    className="mb-3"
                ),
                dbc.Collapse(
                    dbc.Row(
                        [
                            dbc.Col(
                                dcc.Slider(
                                    id='tap-in-out-slider',
                                    min=0,
                                    max=6,
                                    step=1,
                                    marks={
                                        0: 'Weekday Tap-in',
                                        1: 'Weekday Tap-out',
                                        2: 'Weekday Comparison',
                                        3: 'Tap-in (Wkday vs Wkend)',
                                        4: 'Tap-out (Wkday vs Wkend)',
                                        5: 'Heatmap',
                                        6: 'Stacked Bar (Wkday)'
                                    },
                                    value=0
                                ),
                                width=12
                            ),
                            dbc.Col(
                                dcc.Graph(
                                    id='tap-in-out-plots'
                                ),
                                width=12
                            )
                        ],
                        className="mt-4",
                    ),
                    id="slider-plot-section",
                    is_open=False
                ),
                # Stores to track selections
                dcc.Store(id='selected-bus', data=None),
                dcc.Store(id='selected-stop', data=None),
                dcc.Store(id='redis-data-store', data=None),

                dcc.Store(id='filtered-buses', data=bus_services_df.to_dict('records')),
                dcc.Interval(
                    id='refresh-interval',
                    interval=300000,  # 300,000 ms = 5 minutes
                    n_intervals=0
                ),

                # Chatbot section
                dbc.Row(
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4("Bus Info Assistant", className="card-title"),
                                    dcc.Textarea(
                                        id='chat-input',
                                        placeholder='Ask about a bus stop...',
                                        className='chat-input'
                                    ),
                                    html.Br(),
                                    html.Button("Send", id='chat-send', n_clicks=0, className="chat-send mt-2"),
                                    html.Div(id='chat-response', className='chat-response')
                                ]
                            )
                        ),
                        width=12
                    )
                )
            ]
        ),
        html.Div(
            id="tab-2-content",
            children = [
                # Map and Bus Information Panel
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Graph(
                                id='map_tf',
                                config={'scrollZoom': True}
                            ),
                            width=8
                        ),
                        dbc.Col(
                            children = [
                            dcc.Dropdown(
                                id='camera-selector',
                                options=[{'label': cam, 'value': cam} for cam in df['CameraID'].unique()],
                                placeholder="Select a Camera ID",
                                style={'marginBottom': '10px'}
                            ),
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.H4('Choosen Traffic Image', className="card-title"),
                                        html.Ul(id='traffic_image', children=[]),
                                        html.Ul(id='vehicle info', children=[])
                                    ]
                                ),
                            )],
                            width=4
                        ),
                    ],
                    className="mb-4",
                ),
            ]
        ),
        html.Div(
            id='tab-3-content',
            children=[
                html.H2(
                    "Monthly Taxi Fleet Forecasting",
                    className="text-center mb-4",
                    style={
                        'fontSize': '32px',
                        'fontWeight': 'bold',
                        'color': '#003366'  # dark blue
                    }
                ),

                html.Div([
                    html.H4(
                        "Model Performance (Train/Test Split)",
                        className="text-center",
                        style={
                            'fontSize': '24px',
                            'fontWeight': '600'
                        }
                    ),
                    html.Img(
                        src='/assets/taxi_forecast_model_performance.png',
                        style={'width': '80%', 'margin': 'auto', 'display': 'block', 'borderRadius': '10px'}
                    ),
                ], className="mb-5"),

                html.Div([
                    html.H4(
                        "Forecast Until 2030",
                        className="text-center",
                        style={
                            'fontSize': '24px',
                            'fontWeight': '600'
                        }
                    ),
                    html.Img(
                        src='/assets/taxi_forecast_until_2030.png',
                        style={'width': '80%', 'margin': 'auto', 'display': 'block', 'borderRadius': '10px'}
                    ),
                ])

            ],
            style={'display': 'none'}  # initially hidden
        ),
    ],
    fluid=True
)

@app.callback(
    Output('tab-1-content', 'style'),
    Output('tab-2-content', 'style'),
    Output('tab-3-content', 'style'),
    Input('tabs-example-1', 'value')
)

def switch_tabs(selected_tab):
    if selected_tab == 'tab-1':
        return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}
    elif selected_tab == 'tab-2':
        return {'display': 'none'}, {'display': 'block'}, {'display': 'none'}
    elif selected_tab == 'tab-3':
        return {'display': 'none'}, {'display': 'none'}, {'display': 'block'}

# Callback to update the selected-stop store.
@app.callback(
    Output('selected-stop', 'data'),
    [Input('map', 'clickData'),
     Input('bus-stop-search', 'value')],
    [State('selected-stop', 'data')]
)
def update_selected_stop(map_click, dropdown_value, current_selected_stop):
    ctx = dash.callback_context
    if not ctx.triggered:
        return current_selected_stop
    triggered_prop = ctx.triggered[0]['prop_id']
    # If dropdown changes, update the store with that value.
    if 'bus-stop-search.value' in triggered_prop:
        return dropdown_value
    # If map click triggered, extract the bus stop code.
    if 'map.clickData' in triggered_prop:
        try:
            clicked_code = map_click['points'][0]['customdata'][0]
        except Exception as e:
            print(f"Error in extracting bus stop code: {e}")
            return current_selected_stop
        # Toggle if the same stop is clicked twice.
        if current_selected_stop == clicked_code:
            return None
        else:
            return clicked_code
    return current_selected_stop
@app.callback(
    Output('redis-data-store', 'data'),
    Input('refresh-interval', 'n_intervals')
)
def update_redis_data(n_intervals):
    # redis_conn = BaseHook.get_connection("redis_default")
    # r = redis.Redis(
    #     host=redis_conn.host,
    #     port=redis_conn.port,
    #     decode_responses=True
    # )
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    items = r.lrange("bus_arrivals", 0, -1)
    return items

# Update the map callback to use the selected-stop store.
@app.callback(
    
    Output('map', 'figure'),
    [Input('selected-bus', 'data'),
     Input('selected-stop', 'data')],
    [State('filtered-buses', 'data')]
)
def update_map(selected_bus, selected_stop, filtered_buses_data):
    # Default: show all stops in gray.
    stops_df = bus_stops_df
    center = {'lat': 1.3521, 'lon': 103.8198}
    zoom = 11
    marker_color = 'gray'
    marker_size = 10

    # Priority: if a bus stop is selected, show only that stop.
    if selected_stop:
        row = bus_stops_df[bus_stops_df['BusStopCode'] == selected_stop]
        if not row.empty:
            stop = row.iloc[0]
            stops_df = pd.DataFrame([stop])
            center = {'lat': stop['Latitude'], 'lon': stop['Longitude']}
            zoom = 11
            marker_color = 'green'
            marker_size = 14
    if selected_bus:
        # If no bus stop is selected but a bus is selected, show stops covered by that bus.
        selected_bus = selected_bus.split("_")[0]
        stops_covered = bus_routes_df[bus_routes_df['ServiceNo'] == selected_bus]['BusStopCode'].unique()
        stops_df = bus_stops_df[bus_stops_df['BusStopCode'].isin(stops_covered)]
        if not stops_df.empty:
            center = {'lat': stops_df['Latitude'].mean(), 'lon': stops_df['Longitude'].mean()}
        zoom = 11
        marker_color = 'red'
        marker_size = 12

    customdata = list(zip(
        stops_df['BusStopCode'],
        stops_df['RoadName'],
        stops_df['Latitude'],
        stops_df['Longitude']
    ))
    
    figure = {
        'data': [go.Scattermapbox(
            lat=stops_df['Latitude'],
            lon=stops_df['Longitude'],
            mode='markers',
            marker={'color': marker_color, 'size': marker_size},
            text=stops_df['Description'],
            customdata=customdata,
            hovertemplate="<b>%{text}</b><br>Code: %{customdata[0]}<br>Road: %{customdata[1]}<br>Lat: %{customdata[2]}<br>Lon: %{customdata[3]}<extra></extra>"
        )],
        'layout': go.Layout(
            mapbox=dict(
                style="carto-positron",
                center=center,
                zoom=zoom
            ),
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            hovermode="closest"
        )
    }
    return figure

@app.callback(
    Output('map_tf', 'figure'),
    Input('camera-selector', 'value')
)
def update_traffic_map(selected_camera_id):
    marker_color = ['gray'] * len(df)
    marker_size = [10] * len(df)
    
    # If user selects a CameraID, highlight it in blue
    if selected_camera_id:
        match_idx = df.index[df['CameraID'] == selected_camera_id].tolist()
        if match_idx:
            marker_color[match_idx[0]] = 'blue'
            marker_size[match_idx[0]] = 14

    figure = {
        'data': [go.Scattermapbox(
            lat=df['Latitude'],
            lon=df['Longitude'],
            mode='markers',
            marker=dict(color=marker_color, size=marker_size),
            text=df['CameraID'],
            customdata=df['ImageLink'],
            hovertemplate="<b>Camera ID:</b> %{text}<br><b>Image:</b> <a href='%{customdata}'>View</a><extra></extra>"
        )],
        'layout': go.Layout(
            mapbox=dict(
                style="carto-positron",
                center={"lat": 1.3521, "lon": 103.8198},
                zoom=11
            ),
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            hovermode="closest"
        )
    }
    return figure

@app.callback(
    Output('camera-selector', 'value'),
    Input('map_tf', 'clickData'),
    prevent_initial_call=True
)
def update_camera_dropdown_on_click(clickData):
    if clickData and clickData['points']:
        clicked_camera_id = clickData['points'][0]['text']
        return clicked_camera_id
    return dash.no_update

@app.callback(
    [Output('traffic_image', 'children'),
    Output('vehicle info', 'children')],
    Input('camera-selector', 'value')
)
def display_traffic_image(selected_camera_id):
    if selected_camera_id is None:
        return html.P("Please select a camera to view the image."), html.P("")

    camera_row = df[df['CameraID'] == selected_camera_id]
    if camera_row.empty:
        return html.P("No image found for the selected camera."), html.P("")

    image_url = camera_row.iloc[0]['ImageLink']
    vehicle_classes = ['car', 'motorcycle', 'bus', 'truck', "bicycle"]
    response = requests.get(image_url)
    img = Image.open(BytesIO(response.content))
    result = model(img)
    boxes = result[0].boxes
    class_ids = boxes.cls.cpu().numpy().astype(int)
    names = model.names
    
    vehicle_counts = Counter()
    for cls_id in class_ids:
        class_name = names[cls_id]
        if class_name in vehicle_classes:
            vehicle_counts[class_name] += 1

    total_vehicle = sum(vehicle_counts.values()) if len(vehicle_counts) != 0 else 0
    vehicle_info = html.Div([
        html.P("Vehicle Info:"),
        html.Ul([
            html.Li(f"{key}: {value}") for key, value in vehicle_counts.items()
        ]),
        html.P(f"Total vehicle on the road: {total_vehicle}")
    ])
    return html.Img(src=image_url, style={'width': '100%', 'borderRadius': '10px'}), vehicle_info

@app.callback(
    Output('slider-plot-section', 'is_open'),
    [Input('collapse-button', 'n_clicks')],
    [State('slider-plot-section', 'is_open')]
)
def toggle_slider_plot(n_clicks, is_open):
    if n_clicks:
        return not is_open
    return is_open


@app.callback(
    Output('bus-list', 'children'),
    [Input('map', 'clickData'),
     Input('redis-data-store', 'data')]
)
def update_bus_list_on_click(clickData, redis_items):
    if not clickData:
        return [html.Li("Click on a bus stop marker to see upcoming bus services.")]
    
    try:
        # Extract the bus stop code from the marker's customdata.
        clicked_stop_code = clickData['points'][0]['customdata'][0]
        print(f"Clicked stop code: {clicked_stop_code}")
    except Exception as e:
        return [html.Li(f"Error extracting bus stop code: {e}")]
    
    # Use the updated Redis data from the store
    all_items = redis_items or []
    
    # Filter to only the items for the clicked stop
    upcoming_services = []
    for item in all_items:
        try:
            doc = json.loads(item)
            if doc.get("BusStopCode") == clicked_stop_code:
                upcoming_services.append(doc)
        except Exception as e:
            print("Error parsing Redis item:", e)
    
    if not upcoming_services:
        return [html.Li("No upcoming bus services found for this stop.")]
    
    def parse_eta_info(eta_str):
        if not eta_str:
            return ("N/A", "N/A")
        try:
            eta_dt = dateutil.parser.isoparse(eta_str)
            now_utc = datetime.now(timezone.utc)
            delta = eta_dt - now_utc
            delta_minutes = int(delta.total_seconds() // 60)
            formatted_eta = eta_dt.strftime("%H:%M")
            if delta_minutes < 0:
                difference_str = "Departed"
            elif delta_minutes < 1:
                difference_str = "Arr"
            else:
                difference_str = f"{delta_minutes} min"
            return (formatted_eta, difference_str)
        except Exception:
            return ("N/A", "N/A")
    
    def parse_eta_to_dt(doc):
        eta_str = doc.get("EstimatedArrival", "")
        if eta_str:
            try:
                return dateutil.parser.isoparse(eta_str)
            except Exception:
                return datetime.max.replace(tzinfo=timezone.utc)
        return datetime.max.replace(tzinfo=timezone.utc)

    # Sort and filter the services as before
    upcoming_services_sorted = sorted(upcoming_services, key=parse_eta_to_dt)
    now_utc = datetime.now(timezone.utc)
    upcoming_services_filtered = []
    for service in upcoming_services_sorted:
        eta_str = service.get("EstimatedArrival", "")
        if eta_str:
            try:
                eta_dt = dateutil.parser.isoparse(eta_str)
                if (eta_dt - now_utc).total_seconds() < -60:
                    continue
            except Exception:
                continue
        upcoming_services_filtered.append(service)

    bus_items = []
    occurrence_count = {}
    for doc in upcoming_services_filtered:
        service_no = doc.get("ServiceNo", "N/A")
        raw_eta = doc.get("EstimatedArrival", "")
        formatted_eta, difference_str = parse_eta_info(raw_eta)
        if formatted_eta == "N/A" and difference_str == "N/A":
            continue
        occurrence_count[service_no] = occurrence_count.get(service_no, 0) + 1
        unique_id = f"{service_no}_{occurrence_count[service_no]}"
        bus_item_text = f"Bus {service_no} : {formatted_eta} ({difference_str})"
        bus_items.append(
            html.Li(
                bus_item_text,
                id={'type': 'bus-item', 'index': unique_id},
                n_clicks=0,
                className="bus-item"
            )
        )

    return bus_items

@app.callback(
    Output('selected-bus', 'data'),
    [Input({'type': 'bus-item', 'index': ALL}, 'n_clicks'),
     Input('map', 'clickData'),
     Input('bus-stop-search', 'value')],
    [State('filtered-buses', 'data'),
     State('selected-bus', 'data')],
    prevent_initial_call=True
)
def handle_selections(clicks, map_click, search_value, filtered_data, current_selected):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update
    trigger = ctx.triggered[0]['prop_id']
    if any(x in trigger for x in ['map.clickData', 'bus-stop-search.value']):
        return None
    if all(click == 0 for click in clicks):
        return dash.no_update
    if 'bus-item' in trigger:
        try:
            bus_id = json.loads(trigger.split('.')[0])['index']
            return bus_id if current_selected != bus_id else None
        except Exception:
            return dash.no_update
    return dash.no_update

@app.callback(
    Output({'type': 'bus-item', 'index': MATCH}, 'style'),
    [Input('selected-bus', 'data')],
    [State({'type': 'bus-item', 'index': MATCH}, 'id')]
)
def update_bus_style(selected_bus, bus_id):
    if selected_bus == bus_id['index']:
        return {'fontWeight': 'bold', 'opacity': 1}
    return {'opacity': 0.5}

@app.callback(
    Output('tap-in-out-plots', 'figure'),
    [Input('map', 'clickData'),
     Input('tap-in-out-slider', 'value')]
)
def update_tap_in_out_plots(clickData, slider_value):
    if not clickData:
        return go.Figure(layout={'title': 'Click on a bus stop marker to view tap-in/out data'})
    try:
        bus_stop_code = clickData['points'][0]['customdata'][0]
        bus_stop_desc = clickData['points'][0]['text']
    except Exception as e:
        return go.Figure(layout={'title': f'Error extracting bus stop info: {e}'})
    
    conn = sqlite3.connect('/home/houss/airflow/bus.db')
    query = "SELECT * FROM PassengerVolume WHERE BusStopCode = ?"
    df = pd.read_sql_query(query, conn, params=(bus_stop_code,))
    conn.close()
    
    if df.empty:
        return go.Figure(layout={'title': f'No tap-in/out data available for bus stop {bus_stop_desc}'})
    
    weekday_data = df[df["DayType"].str.upper() == "WEEKDAY"].copy()
    weekend_data = df[df["DayType"].str.upper() == "WEEKENDS/HOLIDAY"].copy()
    
    weekday_data["TimePerHour"] = weekday_data["TimePerHour"].astype(int)
    weekend_data["TimePerHour"] = weekend_data["TimePerHour"].astype(int)
    weekday_data = weekday_data.sort_values(by="TimePerHour")
    weekend_data = weekend_data.sort_values(by="TimePerHour")
    
    weekday_data = weekday_data.rename(columns={
        "TimePerHour": "Hour",
        "TapInVolume": "Tap_in",
        "TapOutVolume": "Tap_out"
    })
    weekend_data = weekend_data.rename(columns={
        "TimePerHour": "Hour",
        "TapInVolume": "Tap_in",
        "TapOutVolume": "Tap_out"
    })
    
    all_hours = list(range(24))
    complete_weekday = pd.DataFrame({"Hour": all_hours})
    complete_weekend = pd.DataFrame({"Hour": all_hours})
    weekday_data = pd.merge(complete_weekday, weekday_data, on="Hour", how="left")
    weekend_data = pd.merge(complete_weekend, weekend_data, on="Hour", how="left")
    
    weekday_data["Tap_in"] = weekday_data["Tap_in"].fillna(0).astype(int)
    weekday_data["Tap_out"] = weekday_data["Tap_out"].fillna(0).astype(int)
    weekend_data["Tap_in"] = weekend_data["Tap_in"].fillna(0).astype(int)
    weekend_data["Tap_out"] = weekend_data["Tap_out"].fillna(0).astype(int)
    
    fig = go.Figure()
    if slider_value == 0:
        fig.add_trace(go.Scatter(x=weekday_data["Hour"], y=weekday_data["Tap_in"],
                                 mode="lines+markers", name="Weekday Tap-in",
                                 line=dict(color="blue")))
        fig.update_layout(title=f"Weekday Tap-in Volume at {bus_stop_desc}",
                          xaxis_title="Hour of Day", yaxis_title="Tap-in Volume")
    elif slider_value == 1:
        fig.add_trace(go.Scatter(x=weekday_data["Hour"], y=weekday_data["Tap_out"],
                                 mode="lines+markers", name="Weekday Tap-out",
                                 line=dict(color="green")))
        fig.update_layout(title=f"Weekday Tap-out Volume at {bus_stop_desc}",
                          xaxis_title="Hour of Day", yaxis_title="Tap-out Volume")
    elif slider_value == 2:
        fig.add_trace(go.Scatter(x=weekday_data["Hour"], y=weekday_data["Tap_in"],
                                 mode="lines+markers", name="Tap-in Volume",
                                 line=dict(color="blue")))
        fig.add_trace(go.Scatter(x=weekday_data["Hour"], y=weekday_data["Tap_out"],
                                 mode="lines+markers", name="Tap-out Volume",
                                 line=dict(color="green")))
        fig.update_layout(title=f"Weekday Tap-in vs Tap-out at {bus_stop_desc}",
                          xaxis_title="Hour of Day", yaxis_title="Volume")
    elif slider_value == 3:
        fig.add_trace(go.Scatter(x=weekday_data["Hour"], y=weekday_data["Tap_in"],
                                 mode="lines+markers", name="Weekday Tap-in",
                                 line=dict(color="blue")))
        fig.add_trace(go.Scatter(x=weekend_data["Hour"], y=weekend_data["Tap_in"],
                                 mode="lines+markers", name="Weekend Tap-in",
                                 line=dict(color="red")))
        fig.update_layout(title=f"Tap-in Volume: Weekdays vs Weekends at {bus_stop_desc}",
                          xaxis_title="Hour of Day", yaxis_title="Tap-in Volume")
    elif slider_value == 4:
        fig.add_trace(go.Scatter(x=weekday_data["Hour"], y=weekday_data["Tap_out"],
                                 mode="lines+markers", name="Weekday Tap-out",
                                 line=dict(color="green")))
        fig.add_trace(go.Scatter(x=weekend_data["Hour"], y=weekend_data["Tap_out"],
                                 mode="lines+markers", name="Weekend Tap-out",
                                 line=dict(color="orange")))
        fig.update_layout(title=f"Tap-out Volume: Weekdays vs Weekends at {bus_stop_desc}",
                          xaxis_title="Hour of Day", yaxis_title="Tap-out Volume")
    elif slider_value == 5:
        heatmap_data = pd.DataFrame({
            "Hour": all_hours,
            "Weekday_Tap_in": weekday_data["Tap_in"],
            "Weekend_Tap_in": weekend_data["Tap_in"],
            "Weekday_Tap_out": weekday_data["Tap_out"],
            "Weekend_Tap_out": weekend_data["Tap_out"],
        })
        z_data = heatmap_data[["Weekday_Tap_in", "Weekend_Tap_in", "Weekday_Tap_out", "Weekend_Tap_out"]].T.values
        fig = go.Figure(data=go.Heatmap(
            z=z_data,
            x=all_hours,
            y=["Weekday Tap_in", "Weekend Tap_in", "Weekday Tap_out", "Weekend Tap_out"],
            colorscale='RdBu',
            reversescale=True

        ))
        fig.update_layout(title=f"Heatmap of Tap-in and Tap-out at {bus_stop_desc}",
                          xaxis_title="Hour of Day", yaxis_title="Category")
    elif slider_value == 6:
        fig.add_trace(go.Bar(x=weekday_data["Hour"], y=weekday_data["Tap_in"],
                             name="Tap-in Volume", marker_color="blue"))
        fig.add_trace(go.Bar(x=weekday_data["Hour"], y=weekday_data["Tap_out"],
                             name="Tap-out Volume", marker_color="green"))
        fig.update_layout(barmode='stack', title=f"Stacked Bar: Weekday Tap-in/Out at {bus_stop_desc}",
                          xaxis_title="Hour of Day", yaxis_title="Volume")
    else:
        fig.update_layout(title="Select a plot using the slider")
    
    return fig


def retrieve_relevant_context(query: str) -> str:
    """
    Naively retrieve relevant context from SQL data (bus stops, services, routes)
    and Redis arrivals based on keywords in the user query.
    """
    query_lower = query.lower()
    retrieved = []

    # Retrieve bus stops matching query keywords
    for _, row in bus_stops_df.iterrows():
        if query_lower in row['Description'].lower() or query_lower in row['BusStopCode'].lower():
            retrieved.append("BusStop: " + json.dumps(row.to_dict()))

    # Retrieve bus services matching query keywords (e.g., service numbers)
    for _, row in bus_services_df.iterrows():
        if query_lower in str(row['ServiceNo']).lower():
            retrieved.append("BusService: " + json.dumps(row.to_dict()))

    # Retrieve bus routes matching query keywords
    for _, row in bus_routes_df.iterrows():
        if query_lower in str(row['ServiceNo']).lower():
            retrieved.append("BusRoute: " + json.dumps(row.to_dict()))

    # Retrieve upcoming arrivals from Redis matching query keywords
    for item in all_redis_items:
        try:
            doc = json.loads(item)
            if query_lower in doc.get("ServiceNo", "").lower() or query_lower in doc.get("BusStopCode", "").lower():
                retrieved.append("RedisArrival: " + json.dumps(doc))
        except Exception:
            continue

    # Join the retrieved data into one context string.
    return "\n".join(retrieved)


class BusQuery(BaseModel):
    bus_service: int

# Create an agent whose sole job is to extract the bus service number from text.
os.environ["OPENAI_API_KEY"] = API_KEY

bus_query_agent = Agent(
    "openai:gpt-4o",
    deps_type=None,
    result_type=BusQuery,
    system_prompt=(
        "Extract the bus service number from the input text. "
        "Only output a valid JSON object that matches the schema: "
        '{"bus_service": <number>}. Do not include any extra text.'
    ),
)

# --- Bus Info Callback ---
@app.callback(
    Output('chat-response', 'children'),
    Input('chat-send', 'n_clicks'),
    [State('chat-input', 'value'),
     State('redis-data-store', 'data')]
)
def generate_bus_info_response(n_clicks, user_query, redis_items):
    if n_clicks == 0 or not user_query:
        return ""
    
    # Use the PydanticAI agent to extract the bus service number
    try:
        extraction_result = bus_query_agent.run_sync(user_query)
        bus_id = str(extraction_result.data.bus_service)
    except Exception as e:
        return "Please provide a valid bus service number (e.g., 'bus info 196'). Error: " + str(e)
    
    # --- Retrieve Data from SQLite ---
    service_info_df = bus_services_df[bus_services_df['ServiceNo'] == bus_id]
    if service_info_df.empty:
        service_details = "No service details found in the BusServices table."
    else:
        service_details = service_info_df.to_dict('records')[0]
    
    route_df = bus_routes_df[bus_routes_df['ServiceNo'] == bus_id]
    num_stops = len(route_df)
    if num_stops > 0:
        merged = pd.merge(route_df, bus_stops_df, on='BusStopCode', how='left')
        stop_descriptions = merged[['BusStopCode', 'Description']].to_dict('records')
    else:
        stop_descriptions = "No route stops found for this service."
    
    # --- Retrieve Live Arrivals from Redis ---
    upcoming_arrivals = []
    now_utc = datetime.now(timezone.utc)
    for item in redis_items or []:
        try:
            doc = json.loads(item)
            if doc.get("ServiceNo") == bus_id:
                upcoming_arrivals.append(doc)
        except Exception:
            continue

    arrival_times = []
    for rec in upcoming_arrivals:
        eta_str = rec.get("EstimatedArrival", "")
        if eta_str:
            try:
                eta_dt = dateutil.parser.isoparse(eta_str)
                wait_minutes = int((eta_dt - now_utc).total_seconds() / 60)
                arrival_times.append(f"{eta_dt.strftime('%H:%M')} ({wait_minutes} min)")
            except Exception:
                continue

    # Placeholder for peak time information (if available)
    peak_info = "Peak times data not available."

    # --- Build Data Context for the LLM ---
    data_context = {
        "bus_id": bus_id,
        "service_details": service_details,
        "number_of_stops": num_stops,
        "stop_descriptions": stop_descriptions,
        "upcoming_arrivals": arrival_times,
        "peak_info": peak_info
    }
    
    prompt = f"""
You are a helpful public transport information assistant for Singapore.
A user requested detailed information for Bus Service {bus_id}.
Below is the data context extracted from our database and live Redis arrivals:

Service Details:
{json.dumps(service_details, indent=2)}

Route Information:
- Total number of stops: {num_stops}
- Stop details: {json.dumps(stop_descriptions, indent=2)}

Live Arrivals:
- Upcoming arrival times: {json.dumps(arrival_times, indent=2)}

Additional Information:
- Peak times: {peak_info}

Based solely on this data, generate a concise, professional summary that includes:
- A description of the bus service,
- The total number of stops on the route,
- The next upcoming arrival times,
- Any peak operating times or notable statistics from the data.

Do not include any details about how the data was retrieved.
    """
    try:
        from openai import OpenAI
        client = OpenAI(api_key =API_KEY
)
        response = client.responses.create(
            model="gpt-4o",
            input=[
                {"role": "developer", "content": "Generate a data-driven summary based solely on the provided context."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
        )
        chat_response = response.output_text
    except Exception as e:
        chat_response = f"Error generating response: {str(e)}"
    
    return chat_response

if __name__ == "__main__":
    app.run(debug=True)
