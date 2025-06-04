import io
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, callback_context, no_update
import dash_bootstrap_components as dbc
from flask import jsonify
import zipfile
from utils import get_freq, get_download_data, get_surface_data, get_data_raw, get_data_sessions, load_config, load_translator

class DownloadFrontend:
    """
    Dash-based frontend for data download and visualization.

    Usage:
        from download_frontend import DownloadFrontend
        from utils import get_dbconn
        dbconn = get_dbconn()
        frontend = DownloadFrontend(dbconn, language='no', requests_pathname_prefix='/frontend/')
        frontend.app.run()

    Args:
        dbconn: Database connection object.
        language: Language code for translations (default 'no').
        requests_pathname_prefix: Path prefix for Dash app (default '/download/').
    """
    def __init__(self, dbconn, language='no', requests_pathname_prefix='/download/'):
        """
        Initialize the DownloadFrontend app.
        """
        self.dbconn = dbconn
        self.language = language
        self.requests_pathname_prefix = requests_pathname_prefix
        self.setup_language(language)
        self.app = Dash(__name__,
                        requests_pathname_prefix=self.requests_pathname_prefix,
                        external_stylesheets=[dbc.themes.FLATLY])
        self.app.title = self.t('navbar', 'brand')
        self.setup_layout()
        self.setup_callbacks()
        self.app.server.errorhandler(404)(self.dash_page_not_found)

    def setup_language(self, language):
        self.t = load_translator(language)
        self.depth_set = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.5]
        self.depth_marks = {str(depth): str(depth) for depth in self.depth_set}

        resampling_keys = ['all', '3H', '6H', '12H', '1D', '1W', '1M']
        self.resampling_intervals_dict = {k: self.t('resampling_intervals', k) for k in resampling_keys}

        depth_aggregation_keys = ['all_selected', 'average']
        self.depth_aggregation_dict = {k: self.t('depth_aggregation', k) for k in depth_aggregation_keys}

        download_format_keys = ['xlsx', 'csv']
        self.download_formats_dict = {k: self.t('download_formats', k) for k in download_format_keys}

        parameter_keys = ['oxygen', 'temperature', 'turbidity', 'salinity', 'fluorescence']
        self.parameters_dict = {k: self.t('parameters', k) for k in parameter_keys}

        surface_parameter_keys = ['airtemp', 'windspeed', 'winddirection', 'airpressure']
        self.surface_parameters_dict = {k: self.t('surface_parameters', k) for k in surface_parameter_keys}

        self.all_parameter_values = list(self.parameters_dict.keys())
        self.all_surface_parameter_values = list(self.surface_parameters_dict.keys())
        self.app = Dash(__name__,
                        requests_pathname_prefix=self.requests_pathname_prefix,
                        external_stylesheets=[dbc.themes.FLATLY])
        self.app.title = self.t('navbar', 'brand')
        self.setup_layout()
        self.setup_callbacks()
        self.app.server.errorhandler(404)(self.dash_page_not_found)

    def dash_page_not_found(self, e):
        """
        Custom 404 error handler for the Dash app.
        """
        return jsonify(error=404, text=str(e), source="Dash App (download_frontend.py)"), 404

    def setup_layout(self):
        """
        Define the Dash app layout (UI components).
        """
        t = self.t
        self.app.layout = dbc.Container([
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
                        dbc.CardBody([
                            dcc.DatePickerRange(
                                id='date-picker-range',
                                start_date=None,
                                end_date=None,
                                display_format='YYYY-MM-DD',
                                className="form-control"
                            ),
                            html.P(
                                id='dives-count-box',
                                style={'fontWeight': 'bold', 'marginTop': '10px', 'marginBottom': '0'}
                            )
                        ])
                    ], className="mb-4"),
                    dbc.Card([
                        dbc.CardHeader(html.H5(t('data_explanation', 'title'), className="card-title mb-0")),
                        dbc.CardBody([
                            html.P(t('data_explanation', 'p1')),
                            html.P(t('data_explanation', 'p2')),
                            html.P(t('data_explanation', 'p3'))
                        ])
                    ], className="mb-4"),
                ], width=4),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5(t('graph', 'title'), className="card-title mb-0")),
                        dbc.CardBody(
                            dcc.Graph(
                                id='time-series-graph',
                                config={'displayModeBar': False}
                            )
                        )
                    ]),
                ], width=8),
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
                                        min=min(self.depth_set),
                                        max=max(self.depth_set),
                                        step=None,
                                        marks=self.depth_marks,
                                        value=[min(self.depth_set), max(self.depth_set)],
                                        className="mb-3"
                                    ),
                                    html.H6(t('accordion', 'resampling_interval')),
                                    dbc.RadioItems(
                                        id='resampling-interval-radio',
                                        options=[{'label': value, 'value': key} for key, value in self.resampling_intervals_dict.items()],
                                        value='3H',
                                        inline=True,
                                        className="mb-3"
                                    ),
                                    html.H6(t('accordion', 'depth_data_display')),
                                    dbc.RadioItems(
                                        id='depth-aggregation-radio',
                                        options=[{'label': value, 'value': key} for key, value in self.depth_aggregation_dict.items()],
                                        value='all_selected',
                                        inline=True,
                                        className="mb-3"
                                    ),
                                    html.H6(t('accordion', 'parameters')),
                                    dbc.Row([
                                        dbc.Col(
                                            dbc.Checklist(
                                                id='parameter-checklist',
                                                options=[{'label': label, 'value': value} for value, label in self.parameters_dict.items()],
                                                value=self.all_parameter_values,
                                                inline=True,
                                                labelStyle={'margin-right': '10px'}
                                            ),
                                            width="auto"
                                        )
                                    ], className="mb-3 align-items-center"),
                                    html.H6(t('accordion', 'download_format')),
                                    dbc.RadioItems(
                                        id='download-format-radio',
                                        options=[{'label': value, 'value': key} for key, value in self.download_formats_dict.items()],
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
                                        options=[{'label': value, 'value': key} for key, value in self.resampling_intervals_dict.items()],
                                        value='all',
                                        inline=True,
                                        className="mb-3"
                                    ),
                                    html.H6(t('accordion', 'parameters')),
                                    dbc.Row([
                                        dbc.Col(
                                            dbc.Checklist(
                                                id='surface-parameter-checklist',
                                                options=[{'label': label, 'value': value} for value, label in self.surface_parameters_dict.items()],
                                                value=self.all_surface_parameter_values,
                                                inline=True,
                                                labelStyle={'margin-right': '10px'}
                                            ),
                                            width="auto"
                                        )
                                    ], className="mb-3 align-items-center"),
                                    html.H6(t('accordion', 'download_format')),
                                    dbc.RadioItems(
                                        id='surface-download-format-radio',
                                        options=[{'label': value, 'value': key} for key, value in self.download_formats_dict.items()],
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
                                        options=[{'label': value, 'value': key} for key, value in self.download_formats_dict.items()],
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
            ]),
        ], fluid=True)

    def setup_callbacks(self):
        """
        Register Dash callbacks for interactivity and data download.
        """
        app = self.app
        t = self.t
        # --- Download callbacks: only update download data ---
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
            df_to_download = get_download_data(
                dbconn=self.dbconn,
                start_date_str=start_date,
                end_date_str=end_date,
                depth_range_bounds_list=depth_range,
                resampling_interval_str=resampling_interval,
                depth_aggregation_str=depth_aggregation,
                selected_parameters_list=parameters
            )
            if df_to_download is None or df_to_download.empty:
                return dcc.send_string(t('download_status', 'no_data_period_parameters'))
            filename = f"resampled_data_{start_date}_to_{end_date}.{download_format}"
            if download_format == 'csv':
                return dcc.send_data_frame(df_to_download.to_csv, filename=filename)
            elif download_format == 'xlsx':
                buffer = io.BytesIO()
                df_to_download.to_excel(buffer, index=True, sheet_name='Resampled Data')
                buffer.seek(0)
                return dcc.send_bytes(buffer.getvalue(), filename=filename)
            else:
                print(f"Unsupported download format: {download_format}")
                return no_update

        @app.callback(
            Output("download-surface-data", "data"),
            Input("download-surface-button", "n_clicks"),
            State("date-picker-range", "start_date"),
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
            df_to_download = get_surface_data(
                dbconn=self.dbconn,
                start_date_str=start_date,
                end_date_str=end_date,
                resampling_interval_str=resampling_interval,
                selected_parameters_list=parameters
            )
            if df_to_download is None or df_to_download.empty:
                return dcc.send_string(t('download_status', 'no_data_period_parameters'))
            filename = f"surface_data_{start_date}_to_{end_date}.{download_format}"
            if download_format == 'csv':
                return dcc.send_data_frame(df_to_download.to_csv, filename=filename)
            elif download_format == 'xlsx':
                buffer = io.BytesIO()
                df_to_download.to_excel(buffer, index=True, sheet_name='Surface Data')
                buffer.seek(0)
                return dcc.send_bytes(buffer.getvalue(), filename=filename)
            return no_update

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
            filename_base = f"raw_data_{start_date}_to_{end_date}"
            df_sessions = get_data_sessions(self.dbconn, start_date, end_date)
            df_raw = get_data_raw(self.dbconn, start_date, end_date)
            if (df_sessions is None or df_sessions.empty) and (df_raw is None or df_raw.empty):
                return dcc.send_string(t('download_status', 'no_data_period_parameters'))
            if download_format == 'csv':
                zip_filename = f"{filename_base}.zip"
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    sessions_csv_buffer = io.StringIO()
                    if not df_sessions.empty:
                        df_sessions.to_csv(sessions_csv_buffer, index=False)
                        zf.writestr("session_data.csv", sessions_csv_buffer.getvalue())
                    else:
                        zf.writestr("session_data.csv", t('download_status', 'no_session_data_period'))
                    sessions_csv_buffer.close()
                    raw_csv_buffer = io.StringIO()
                    if not df_raw.empty:
                        df_raw.to_csv(raw_csv_buffer, index=False)
                        zf.writestr("raw_timeseries_data.csv", raw_csv_buffer.getvalue())
                    else:
                        zf.writestr("raw_timeseries_data.csv", t('download_status', 'no_raw_timeseries_data_period'))
                    raw_csv_buffer.close()
                zip_buffer.seek(0)
                return dcc.send_bytes(zip_buffer.getvalue(), filename=zip_filename)
            elif download_format == 'xlsx':
                xlsx_filename = f"{filename_base}.xlsx"
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    if not df_sessions.empty:
                        df_sessions.to_excel(writer, sheet_name=t('sheet_names', 'session_data'), index=False)
                    else:
                        pd.DataFrame([t('download_status', 'no_session_data_period')]).to_excel(
                            writer, sheet_name=t('sheet_names', 'session_data'), index=False, header=False
                        )
                    if not df_raw.empty:
                        df_raw.to_excel(writer, sheet_name=t('sheet_names', 'raw_timeseries_data'), index=False)
                    else:
                        pd.DataFrame([t('download_status', 'no_raw_timeseries_data_period')]).to_excel(
                            writer, sheet_name=t('sheet_names', 'raw_timeseries_data'), index=False, header=False
                        )
                buffer.seek(0)
                return dcc.send_bytes(buffer.getvalue(), filename=xlsx_filename)
            return no_update

        # --- Graph and date picker update callback ---
        @app.callback(
            Output('time-series-graph', 'figure'),
            Output('date-picker-range', 'start_date'),
            Output('date-picker-range', 'end_date'),
            Output('dives-count-box', 'children'),
            Input('date-picker-range', 'start_date'),
            Input('date-picker-range', 'end_date'),
            Input('time-series-graph', 'relayoutData'),
            State('time-series-graph', 'figure')
        )
        def update_graph_and_date_picker(picker_start_date, picker_end_date, relayoutData, current_figure):
            ctx = callback_context
            if not ctx.triggered:
                try:
                    df_stats = get_freq(self.dbconn, format='dataframe')
                    if df_stats.empty:
                        raise ValueError("Empty data from get_freq for initial load")
                except Exception as e:
                    dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
                    data = {'dives': np.random.randint(1, 20, len(dates))}
                    df_stats = pd.DataFrame(data, index=dates)
                fig = go.Figure()
                if not df_stats.empty and 'dives' in df_stats.columns:
                    fig.add_trace(go.Scatter(x=df_stats.index, y=df_stats['dives'], mode='markers', name='Dives'))
                else:
                    fig.add_trace(go.Scatter(x=[], y=[], mode='markers', name='No data'))
                fig.update_layout(
                    xaxis_title=self.t('date_picker', 'select_date_range'),
                    yaxis_title=self.t('graph', 'title'),
                    xaxis=dict(rangeslider=dict(visible=True), type="date"),
                    margin=dict(l=20, r=20, t=40, b=20)
                )
                initial_start = df_stats.index.min().strftime('%Y-%m-%d') if not df_stats.empty else pd.Timestamp('now').strftime('%Y-%m-%d')
                initial_end = df_stats.index.max().strftime('%Y-%m-%d') if not df_stats.empty else pd.Timestamp('now').strftime('%Y-%m-%d')
                fig.update_xaxes(range=[initial_start, initial_end])
                dives_count = int(df_stats['dives'].sum()) if not df_stats.empty and 'dives' in df_stats.columns else 0
                dives_text = f"{t('graph', 'dives_in_period')}: {dives_count}"
                return fig, initial_start, initial_end, dives_text
            trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
            if current_figure is not None and 'xaxis' in current_figure['layout'] and \
               'rangeslider' in current_figure['layout']['xaxis'] and \
               'yaxis' in current_figure['layout']['xaxis']['rangeslider']:
                updated_figure_dict = current_figure.copy()
                updated_layout = updated_figure_dict['layout'].copy()
                updated_rangeslider = updated_layout['xaxis']['rangeslider'].copy()
                del updated_rangeslider['yaxis']
                updated_layout['xaxis']['rangeslider'] = updated_rangeslider
                updated_figure_dict['layout'] = updated_layout
                fig = go.Figure(updated_figure_dict)
            else:
                fig = go.Figure(current_figure) if current_figure else go.Figure()
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
                    df_stats = get_freq(self.dbconn, format='dataframe')
                    mask = (df_stats.index >= new_picker_start) & (df_stats.index <= new_picker_end)
                    dives_count = int(df_stats.loc[mask, 'dives'].sum()) if not df_stats.empty and 'dives' in df_stats.columns else 0
                    dives_text = f"{t('graph', 'dives_in_period')}: {dives_count}"
                    return fig, new_picker_start, new_picker_end, dives_text
                else:
                    return no_update, no_update, no_update, no_update
            elif trigger_id == 'date-picker-range':
                fig.update_xaxes(range=[picker_start_date, picker_end_date])
                df_stats = get_freq(self.dbconn, format='dataframe')
                mask = (df_stats.index >= picker_start_date) & (df_stats.index <= picker_end_date)
                dives_count = int(df_stats.loc[mask, 'dives'].sum()) if not df_stats.empty and 'dives' in df_stats.columns else 0
                dives_text = f"{t('graph', 'dives_in_period')}: {dives_count}"
                return fig, picker_start_date, picker_end_date, dives_text
            return no_update, no_update, no_update, no_update
