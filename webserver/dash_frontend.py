from dash import Dash, dcc, html, Input, Output, State, callback_context, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np # Import numpy for generating dummy data
import io # Import io for handling in-memory files
import json # Added for loading config
from flask import jsonify # Added jsonify import
import os # Import os to access environment variables
import zipfile # Import zipfile for creating ZIP archives

# Import the new get_download_data function
from utils import get_freq, get_download_data,get_surface_data # Import get_surface_data for surface data download
from utils import get_data_raw, get_data_sessions # Import for raw data download
from utils import load_config

# Load database connection string from config.json

config = load_config()
DBCONN = config["pg_conn"]

# Define depth_set
depth_set = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.5]
depth_marks = {str(depth): str(depth) for depth in depth_set}


def get_translator(lang_code='no'):
    """
    Loads the translation file for the given language code and returns a translation function t().
    Usage: t = get_translator('no') or t = get_translator('en')
    """
    fname = f'strings.{lang_code}.json'
    path = os.path.join(os.path.dirname(__file__), fname)
    with open(path, encoding='utf-8') as f:
        STRINGS = json.load(f)
    def t(*keys):
        d = STRINGS
        for k in keys:
            d = d[k]
        return d
    return t

t = get_translator("no")

resampling_intervals_dict = {
    'all': t('resampling_intervals', 'all'),
    '3H': t('resampling_intervals', '3H'),
    '6H': t('resampling_intervals', '6H'),
    '12H': t('resampling_intervals', '12H'),
    '1D': t('resampling_intervals', '1D'),
    '1W': t('resampling_intervals', '1W'),
    '1M': t('resampling_intervals', '1M')
}

depth_aggregation_dict = {
    "all_selected": t('depth_aggregation', 'all_selected'),
    "average": t('depth_aggregation', 'average')
}

download_formats_dict = {
    'xlsx': t('download_formats', 'xlsx'),
    'csv': t('download_formats', 'csv')
}
# --- End of inverted dictionaries ---

parameters_dict = {
    'oxygen': t('parameters', 'oxygen'),
    'temperature': t('parameters', 'temperature'),
    'turbidity': t('parameters', 'turbidity'),
    'salinity': t('parameters', 'salinity'),
    'fluorescence': t('parameters', 'fluorescence')
}

surface_parameters_dict = {
    'airtemp': t('surface_parameters', 'airtemp'),
    'windspeed': t('surface_parameters', 'windspeed'),
    'winddirection': t('surface_parameters', 'winddirection'),
    'airpressure': t('surface_parameters', 'airpressure'),
}

# Get the list of parameter values for preselection
all_parameter_values = list(parameters_dict.keys())
all_surface_parameter_values = list(surface_parameters_dict.keys())


# Determine the base pathnames considering SCRIPT_NAME for proxy environments
# Also add /frontend since we mount Dash in Flask via DispatcherMiddleware
script_name = os.environ.get('SCRIPT_NAME', '')
actual_script_name = script_name.rstrip('/')
# Path segment at which Dash is mounted in Flask via DispatcherMiddleware in webserver.py
dash_flask_mount_segment = '/frontend' # This must match the key in DispatcherMiddleware
calculated_routes_pathname_prefix = actual_script_name + dash_flask_mount_segment + '/'

app = Dash(__name__,
           requests_pathname_prefix=calculated_routes_pathname_prefix , # Add requests_pathname_prefix
           external_stylesheets=[dbc.themes.FLATLY])

app.title = t('navbar', 'brand')

@app.server.errorhandler(404)
def dash_page_not_found(e):
    return jsonify(error=404, text=str(e), source="Dash App (dash_frontend.py)"), 404

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(dbc.NavbarSimple(
            children=[
                dbc.NavItem(dbc.NavLink(t('navbar', 'home'), href="#")),
                dbc.NavItem(dbc.NavLink(t('navbar', 'about'), href="#")),
                dbc.NavItem(dbc.NavLink(t('navbar', 'contact'), href="#")),
            ],
            brand=t('navbar', 'brand'),
            brand_href="#",
            color="primary",
            dark=True,
        ), width=12)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(dbc.Label(t('date_picker', 'select_date_range'), className="mb-0")),
                dbc.CardBody(
                    dcc.DatePickerRange(
                        id='date-picker-range',
                        start_date=None,
                        end_date=None,
                        display_format='YYYY-MM-DD',
                        className="form-control"
                    )
                )
            ], className="mb-4"),

            dbc.Card([
                dbc.CardHeader(html.H5(t('data_explanation', 'title'), className="card-title mb-0")),
                dbc.CardBody([
                    html.P(t('data_explanation', 'p1')),
                    html.P(t('data_explanation', 'p2')),
                    html.P(t('data_explanation', 'p3'))
                ])
            ], className="mb-4")
        ], width=4),
        dbc.Col(
            dbc.Card([
                dbc.CardHeader(html.H5(t('graph', 'title'), className="card-title mb-0")),
                dbc.CardBody(
                    dcc.Graph(
                        id='time-series-graph',
                        config={'displayModeBar': False}
                    )
                )
            ]),
            width=8
        )
    ]),

    dbc.Row([
        dbc.Col(
            dbc.Accordion(
                [
                    dbc.AccordionItem(
                        html.Div([
                            html.H6(t('accordion', 'depth_range')),
                            dcc.RangeSlider(
                                id='depth-range-slider',
                                min=min(depth_set),
                                max=max(depth_set),
                                step=None,
                                marks=depth_marks,
                                value=[min(depth_set), max(depth_set)],
                                className="mb-3"
                            ),
                            html.H6(t('accordion', 'resampling_interval')),
                            dbc.RadioItems(
                                id='resampling-interval-radio',
                                options=[{'label': value, 'value': key} for key, value in resampling_intervals_dict.items()],
                                value='3H',
                                inline=True,
                                className="mb-3"
                            ),
                            html.H6(t('accordion', 'depth_data_display')),
                            dbc.RadioItems(
                                id='depth-aggregation-radio',
                                options=[{'label': value, 'value': key} for key, value in depth_aggregation_dict.items()],
                                value='all_selected',
                                inline=True,
                                className="mb-3"
                            ),
                            html.H6(t('accordion', 'parameters')),
                            dbc.Row([
                                dbc.Col(
                                    dbc.Checklist(
                                        id='parameter-checklist',
                                        options=[{'label': label, 'value': value} for value, label in parameters_dict.items()],
                                        value=all_parameter_values,
                                        inline=True,
                                        labelStyle={'margin-right': '10px'}
                                    ),
                                    width="auto"
                                )
                            ], className="mb-3 align-items-center"),
                            html.H6(t('accordion', 'download_format')),
                            dbc.RadioItems(
                                id='download-format-radio',
                                options=[{'label': value, 'value': key} for key, value in download_formats_dict.items()],
                                value='xlsx',
                                inline=True,
                                className="mb-3"
                            ),
                            dbc.Button(
                                t('accordion', 'download_data'),
                                id="download-button",
                                color="primary",
                                className="mb-3"
                            ),
                            dcc.Download(id="download-resampled-data"),
                        ]),
                        title=t('accordion', 'download_resampled_title'),
                        item_id="item-resampled",
                    ),
                    dbc.AccordionItem(
                        html.Div([
                            html.H6(t('accordion', 'resampling_interval')),
                            dbc.RadioItems(
                                id='surface-resampling-interval-radio',
                                options=[{'label': value, 'value': key} for key, value in resampling_intervals_dict.items()],
                                value='all',
                                inline=True,
                                className="mb-3"
                            ),
                            html.H6(t('accordion', 'parameters')),
                            dbc.Row([
                                dbc.Col(
                                    dbc.Checklist(
                                        id='surface-parameter-checklist',
                                        options=[{'label': label, 'value': value} for value, label in surface_parameters_dict.items()],
                                        value=all_surface_parameter_values,
                                        inline=True,
                                        labelStyle={'margin-right': '10px'}
                                    ),
                                    width="auto"
                                )
                            ], className="mb-3 align-items-center"),
                            html.H6(t('accordion', 'download_format')),
                            dbc.RadioItems(
                                id='surface-download-format-radio',
                                options=[{'label': value, 'value': key} for key, value in download_formats_dict.items()],
                                value='xlsx',
                                inline=True,
                                className="mb-3"
                            ),
                            dbc.Button(
                                t('accordion', 'download_surface_data'),
                                id="download-surface-button",
                                color="primary",
                                className="mb-3"
                            ),
                            dcc.Download(id="download-surface-data"),
                        ]),
                        title=t('accordion', 'download_surface_title'),
                        item_id="item-surface",
                    ),
                    dbc.AccordionItem(
                        html.Div([
                            html.H6(t('accordion', 'download_format')),
                            dbc.RadioItems(
                                id='raw-download-format-radio',
                                options=[{'label': value, 'value': key} for key, value in download_formats_dict.items()],
                                value='xlsx',
                                inline=True,
                                className="mb-3"
                            ),
                            dbc.Button(
                                t('accordion', 'download_raw_data'),
                                id="download-raw-button",
                                color="primary",
                                className="mb-3"
                            ),
                            dcc.Download(id="download-raw-data"),
                        ]),
                        title=t('accordion', 'download_raw_title'),
                        item_id="item-raw",
                    ),
                ],
                start_collapsed=True,
                flush=True,
                className="mt-4",
                active_item="item-resampled"
            ),
            width=12,
        )
    ])

], fluid=True)

# --- Download Callbacks ---
@app.callback(
    Output("download-resampled-data", "data"),
    Input("download-button", "n_clicks"),
    State("date-picker-range", "start_date"),
    State("date-picker-range", "end_date"),
    State("depth-range-slider", "value"),
    State("resampling-interval-radio", "value"),
    State("depth-aggregation-radio", "value"),
    State("parameter-checklist", "value"),
    State("download-format-radio", "value"),
    prevent_initial_call=True,
)
def func_download_resampled_data(n_clicks, start_date, end_date, depth_range,
                                 resampling_interval, depth_aggregation, parameters, download_format):
    if not n_clicks:
        return no_update

    # Call the actual get_download_data function from utils.py
    df_to_download = get_download_data(
        dbconn=DBCONN,  # Pass the database connection string
        start_date_str=start_date,
        end_date_str=end_date,
        depth_range_bounds_list=depth_range, # dash-frontend.py uses 'depth_range' for this
        resampling_interval_str=resampling_interval,
        depth_aggregation_str=depth_aggregation,
        selected_parameters_list=parameters
    )

    filename = f"resampled_data_{start_date}_to_{end_date}.{download_format}"

    if download_format == 'csv':
        return dcc.send_data_frame(df_to_download.to_csv, filename=filename)
    elif download_format == 'xlsx':
        # For Excel, we need an in-memory buffer
        buffer = io.BytesIO()
        df_to_download.to_excel(buffer, index=True, sheet_name='Resampled Data')
        buffer.seek(0) # Rewind the buffer to the beginning
        return dcc.send_bytes(buffer.getvalue(), filename=filename)
    else:
        # Handle other formats or raise an error for unsupported ones
        print(f"Unsupported download format: {download_format}")
        return no_update

@app.callback(
    Output("download-surface-data", "data"),
    Input("download-surface-button", "n_clicks"),
    State("date-picker-range", "start_date"), # Using the main date picker for consistency
    State("date-picker-range", "end_date"),
    State("surface-resampling-interval-radio", "value"),
    State("surface-parameter-checklist", "value"),
    State("surface-download-format-radio", "value"),
    prevent_initial_call=True,
)
def func_download_surface_data(n_clicks, start_date, end_date,
                               resampling_interval, parameters, download_format):
    if not n_clicks:
        return no_update

    # Call the get_surface_data function from utils.py
    df_to_download = get_surface_data(
        dbconn=DBCONN,
        start_date_str=start_date,
        end_date_str=end_date,
        resampling_interval_str=resampling_interval,
        selected_parameters_list=parameters
    )

    filename = f"surface_data_{start_date}_to_{end_date}.{download_format}"

    if df_to_download.empty:
        print(f"No surface data found for the selected criteria. Returning empty {download_format}.")
        if download_format == 'csv':
            return dcc.send_data_frame(lambda x: "", filename=filename) # Send empty string for CSV
        elif download_format == 'xlsx':
            buffer = io.BytesIO()
            # Create an empty Excel file or one with just headers
            pd.DataFrame().to_excel(buffer, index=False, sheet_name='Surface Data')
            buffer.seek(0)
            return dcc.send_bytes(buffer.getvalue(), filename=filename)
        return no_update

    if download_format == 'csv':
        return dcc.send_data_frame(df_to_download.to_csv, filename=filename)
    elif download_format == 'xlsx':
        buffer = io.BytesIO()
        df_to_download.to_excel(buffer, index=True, sheet_name='Surface Data') # index=True as 'ts' is the index
        buffer.seek(0)
        return dcc.send_bytes(buffer.getvalue(), filename=filename)

    return no_update # Fallback if format is not csv or xlsx

# --- End Download Callbacks ---

# --- Callback for Raw Data Download ---
@app.callback(
    Output("download-raw-data", "data"),
    Input("download-raw-button", "n_clicks"),
    State("date-picker-range", "start_date"),
    State("date-picker-range", "end_date"),
    State("raw-download-format-radio", "value"),
    prevent_initial_call=True,
)
def func_download_raw_data(n_clicks, start_date, end_date, download_format):
    if not n_clicks:
        return no_update

    print(f"Attempting to download raw data from {start_date} to {end_date} as {download_format}")

    filename_base = f"raw_data_{start_date}_to_{end_date}"
    empty_filename_base = "raw_data_empty"

    if download_format == 'csv':
        # Fetch session data
        df_sessions = get_data_sessions(DBCONN, start_date, end_date)
        # Fetch raw timeseries data
        df_raw = get_data_raw(DBCONN, start_date, end_date)

        zip_filename = f"{filename_base}.zip"
        empty_zip_filename = f"{empty_filename_base}.zip"

        if df_sessions.empty and df_raw.empty:
            print("No session or raw data found for CSV (ZIP) download. Sending empty ZIP with info.")
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, mode="w") as zf:
                zf.writestr("info.txt", "No data available for the selected period and parameters.")
            buffer.seek(0)
            return dcc.send_bytes(buffer.getvalue(), filename=empty_zip_filename)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            # Session data CSV
            sessions_csv_buffer = io.StringIO()
            if not df_sessions.empty:
                df_sessions.to_csv(sessions_csv_buffer, index=False)
                zf.writestr("session_data.csv", sessions_csv_buffer.getvalue())
            else:
                zf.writestr("session_data.csv", "No session data available for the selected period.")
            sessions_csv_buffer.close()

            # Raw timeseries data CSV
            raw_csv_buffer = io.StringIO()
            if not df_raw.empty:
                df_raw.to_csv(raw_csv_buffer, index=False)
                zf.writestr("raw_timeseries_data.csv", raw_csv_buffer.getvalue())
            else:
                zf.writestr("raw_timeseries_data.csv", "No raw timeseries data available for the selected period.")
            raw_csv_buffer.close()
        
        zip_buffer.seek(0)
        return dcc.send_bytes(zip_buffer.getvalue(), filename=zip_filename)

    elif download_format == 'xlsx':
        # Fetch session data
        df_sessions = get_data_sessions(DBCONN, start_date, end_date)
        # Fetch raw timeseries data
        df_raw = get_data_raw(DBCONN, start_date, end_date)

        if df_sessions.empty and df_raw.empty:
            print("No session or raw data found for Excel download. Sending empty Excel file with info sheet.")
            filename = f"{empty_filename_base}.xlsx"
            buffer = io.BytesIO()
            # Create an Excel file with an "Info" sheet stating no data
            pd.DataFrame(["No data available for the selected period and parameters."]).to_excel(
                buffer, index=False, header=False, sheet_name='Info'
            )
            buffer.seek(0)
            return dcc.send_bytes(buffer.getvalue(), filename=filename)

        filename = f"{filename_base}.xlsx"
        buffer = io.BytesIO()
        # Use openpyxl engine for creating multi-sheet Excel files
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            if not df_sessions.empty:
                df_sessions.to_excel(writer, sheet_name='Session Data', index=False)
            else:
                # Create a sheet with a note if session data is empty
                pd.DataFrame(["No session data available for the selected period."]).to_excel(
                    writer, sheet_name='Session Data', index=False, header=False
                )
            
            if not df_raw.empty:
                df_raw.to_excel(writer, sheet_name='Raw Timeseries Data', index=False)
            else:
                # Create a sheet with a note if raw timeseries data is empty
                pd.DataFrame(["No raw timeseries data available for the selected period."]).to_excel(
                    writer, sheet_name='Raw Timeseries Data', index=False, header=False
                )
        
        buffer.seek(0) # Rewind the buffer to the beginning
        return dcc.send_bytes(buffer.getvalue(), filename=filename)
    
    return no_update


# Callback to update the graph and date picker based on user interactions, synchronising
# the date selector with both range slider and graph selection. Tricky to get all
# three components to work together seamlessly, got some help from Google Gemini
# to make this monstrosity, may possbily be simplfied.
@app.callback(
    Output('time-series-graph', 'figure'),
    Output('date-picker-range', 'start_date'),
    Output('date-picker-range', 'end_date'),
    Input('date-picker-range', 'start_date'),  # Input from date picker
    Input('date-picker-range', 'end_date'),    # Input from date picker
    Input('time-series-graph', 'relayoutData'), # Input from graph interactions (zoom, pan, rangeslider)
    State('time-series-graph', 'figure')      # Current figure state (for initial load and _template fix)
)
def update_graph_and_date_picker(picker_start_date, picker_end_date, relayoutData, current_figure):
    ctx = callback_context

    # --- Initial Load ---
    if not ctx.triggered:
        # Fetch df_stats here instead of globally
        try:
            df_stats = get_freq(DBCONN, format='dataframe') # Fetch data using get_freq
            if df_stats.empty:
                print("Warning: get_freq returned an empty DataFrame. Falling back to dummy data for initial load.")
                raise ValueError("Empty data from get_freq for initial load")
        except Exception as e:
            print(f"Error fetching data using get_freq for initial load: {e}")
            print("Creating dummy DataFrame for development purposes (initial load).")
            dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
            data = {
                'dives': np.random.randint(1, 20, len(dates)) # Ensure 'dives' column for graph
            }
            df_stats = pd.DataFrame(data, index=dates)

        fig = go.Figure()
        if not df_stats.empty and 'dives' in df_stats.columns:
            fig.add_trace(go.Scatter(x=df_stats.index, y=df_stats['dives'], mode='markers', name='Dives'))
        else:
            fig.add_trace(go.Scatter(x=[], y=[], mode='markers', name='No data'))
            
        fig.update_layout(
                          xaxis_title='Dato',
                          yaxis_title='Antall dykk',
                          xaxis=dict(
                              rangeslider=dict(visible=True),
                              type="date"
                          ),
                          margin=dict(l=20, r=20, t=40, b=20)
        )
        
        initial_start = df_stats.index.min().strftime('%Y-%m-%d') if not df_stats.empty else pd.Timestamp('now').strftime('%Y-%m-%d')
        initial_end = df_stats.index.max().strftime('%Y-%m-%d') if not df_stats.empty else pd.Timestamp('now').strftime('%Y-%m-%d')
        
        fig.update_xaxes(range=[initial_start, initial_end])
        return fig, initial_start, initial_end

    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    # --- Prepare current figure to avoid _template error ---
    # We must try to reconstruct the figure to update its range,
    # but clean up the problematic 'yaxis' property from rangeslider if it exists.
    if current_figure is not None and 'xaxis' in current_figure['layout'] and \
       'rangeslider' in current_figure['layout']['xaxis'] and \
       'yaxis' in current_figure['layout']['xaxis']['rangeslider']:

        # Create a mutable copy of the layout and rangeslider dicts
        updated_figure_dict = current_figure.copy()
        updated_layout = updated_figure_dict['layout'].copy()
        updated_rangeslider = updated_layout['xaxis']['rangeslider'].copy()

        # Delete the problematic 'yaxis' key
        del updated_rangeslider['yaxis']

        # Update the layout with the modified rangeslider
        updated_layout['xaxis']['rangeslider'] = updated_rangeslider
        updated_figure_dict['layout'] = updated_layout

        fig = go.Figure(updated_figure_dict) # Recreate figure from cleaned dict
    else:
        fig = go.Figure(current_figure) if current_figure else go.Figure() # Fallback for initial or clean state

    # --- Logic when Graph (relayoutData: zoom, pan, rangeslider, selection) is the trigger ---
    if trigger_id == 'time-series-graph':
        graph_start_date = None
        graph_end_date = None

        if relayoutData:
            if 'xaxis.range' in relayoutData and isinstance(relayoutData['xaxis.range'], list) and len(relayoutData['xaxis.range']) == 2:
                graph_start_date = relayoutData['xaxis.range'][0]
                graph_end_date = relayoutData['xaxis.range'][1]
            elif 'xaxis.range[0]' in relayoutData and 'xaxis.range[1]' in relayoutData:
                graph_start_date = relayoutData['xaxis.range[0]']
                graph_end_date = relayoutData['xaxis.range[1]']

        if graph_start_date and graph_end_date:
            new_picker_start = pd.to_datetime(graph_start_date).strftime('%Y-%m-%d')
            new_picker_end = pd.to_datetime(graph_end_date).strftime('%Y-%m-%d')
            return fig, new_picker_start, new_picker_end
        else:
            return no_update, no_update, no_update

    # --- Logic when DatePickerRange is the trigger ---
    elif trigger_id == 'date-picker-range':
        fig.update_xaxes(range=[picker_start_date, picker_end_date])
        return fig, picker_start_date, picker_end_date

    return no_update, no_update, no_update


if __name__ == '__main__':
    app.run(debug=True, port=8051)

