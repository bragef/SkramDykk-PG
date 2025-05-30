import psycopg2
import pandas as pd

timeframe_sql_map = {
    "3H": "3 hours",
    "6H": "6 hours",
    "12H": "12 hours",
    "1D": "1 day",
    "1W": "1 week",
    "1M": "1 month"
}
    

valid_datatypes = [ "temperature", "salinity", "fluorescens", "turbidity", "oxygen"]
# For api v1 compatibility
names_old_to_new_map = {
    "salt": "salinity",
    "fluorescens": "fluorescens",
    "turbidity": "turbidity",
    "oxygene": "oxygen",
    "temp": "temperature",
    "pressure(dBAR)": "pressure_dbar"
}
names_new_to_old_map = {v: k for k, v in names_old_to_new_map.items()}
# Convert to new name if old name, error if not found 
def get_datatype_name(param):
    if param in names_old_to_new_map:
        return names_old_to_new_map[param]
    elif param in valid_datatypes:
        return param
    else:
        raise ValueError(f"Invalid datatype: {param}. Valid options are: {valid_datatypes}")

depth_set = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.5]

def generate_freq(title, dbconn):
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            # Fetch the number of dives per day
            cur.execute("""
                        SELECT DATE(startdatetime), COUNT(sessionid) as dives 
                        FROM session_data 
                        GROUP BY DATE(startdatetime) 
                        ORDER BY DATE(startdatetime);
            """)
            rows = cur.fetchall()
    dates = [row[0] for row in rows]
    counts = [row[1] for row in rows]
    graphs = [
        dict(
            data=[
                dict(
                    x=dates,
                    y=counts,
                    type='Scatter',
                    mode='markers'
                ),
            ],
            layout=dict(
                title=title
            )
        )
    ]
    return graphs

def get_valid_years(dbconn):
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT DISTINCT EXTRACT(year FROM startdatetime) as YR FROM session_data ORDER BY YR;
            """)
            years = [int(row[0]) for row in cur.fetchall()]
    return years


def get_download_data(datatype,params, dbconn):
    # Download data for a given datatype and parameters
    # Parameters should be a dictionary with keys like 'startdate', 'enddate', 'depths', etc.
    # Valid parameters are:
    # - startdate: YYYY-MM-DD format
    # - enddate: YYYY-MM-DD format
    # - depths: list of depths
    # - depthsmethods: 'all', 'mean', 'median', 'min', 'max'
    # - timeframe: 'all', '3H', '6H', '12H', '1D', '1W', '1M'
    # - parameters: all, selects all parameters, or a list of specific parameters to include
    
    # Create sql query based on parameters
    if datatype not in valid_datatypes:
        raise ValueError(f"Invalid datatype: {datatype}. Valid options are: {valid_datatypes}")
    if 'timeframe' not in params or params['timeframe'] not in timeframe_sql_map:
        raise ValueError(f"Invalid timeframe: {params.get('timeframe', 'None')}. Valid options are: {list(timeframe_sql_map.keys())}")
    if 'depths' not in params or not isinstance(params['depths'], list):
        raise ValueError("Invalid or missing 'depths' parameter. It should be a list of depths.")
    if 'depthsmethods' not in params or params['depthsmethods'] not in ['all', 'mean', 'median', 'min', 'max']:
        raise ValueError("Invalid or missing 'depthsmethods' parameter. Valid options are: 'all', 'mean', 'median', 'min', 'max'.")
    if 'startdate' not in params or 'enddate' not in params:
        raise ValueError("Missing 'startdate' or 'enddate' parameter. Both are required in YYYY-MM-DD format.")
    try:
        startdate = pd.to_datetime(params['startdate']).date()
        enddate = pd.to_datetime(params['enddate']).date()
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD for 'startdate' and 'enddate'.")
    #if 'parameters' not in params or params['parameters'] == 'all':
    #    parameters = valid_datatypes
    #elif isinstance(params['parameters'], list):
    #    parameters = [p for p in params['parameters'] if p in valid_datatypes]
    #    if not parameters:
    #        raise ValueError("No valid parameters provided in 'parameters' list.")
    #else:
    #    raise ValueError("Invalid 'parameters' parameter. It should be 'all' or a list of valid datatypes.")
    
    # Build the SQL query based on the parameters
    
    

    # Query session_data for the specified date range and depths
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            # Create the SQL query
            sql_query = f"""
                        SELECT AVG({datatype}) as val, pressure_dbar, 
                        date_bin('{timeframe_sql_map[params["timeframe"]]}', startdatetime, '2001-01-01 00:00') as ts
                        FROM interpolated_timeseries 
                        JOIN session_data USING(sessionid)
                        WHERE DATE(startdatetime) BETWEEN %s AND %s
                        AND pressure_dbar = ANY(%s)
                        GROUP BY pressure_dbar, ts
                        ORDER BY ts, pressure_dbar DESC;
            """
            cur.execute(sql_query, (startdate, enddate, params['depths']))
            rows = cur.fetchall()
            


def get_resampled_day(day, datatype, timeframe, dbconn):

    if datatype not in valid_datatypes:
        raise ValueError(f"Invalid datatype: {datatype}. Valid options are: {valid_datatypes}")
    if timeframe not in timeframe_sql_map:
        raise ValueError(f"Invalid timeframe: {timeframe}. Valid options are: {list(timeframe_sql_map.keys())}")
    # Parse YYYYMMDD format to a date object
    try:
        start_time = pd.to_datetime(day, format='%Y%m%d').date()
    except ValueError:
        return None, "Invalid date format. Use YYYYMMDD."
    
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            # Fetch the data for the specified day and datatype
            cur.execute(f"""
                        SELECT AVG({datatype}) as val, pressure_dbar, 
                        date_bin('{timeframe_sql_map[timeframe]}', startdatetime, '2001-01-01 00:00') as ts
                        FROM interpolated_timeseries 
                        JOIN session_data USING(sessionid)
                        WHERE DATE(startdatetime) = %s
                        GROUP BY pressure_dbar, ts
                        ORDER BY ts, pressure_dbar DESC;
            """, (start_time,))
            rows = cur.fetchall()

    end_time = start_time + pd.Timedelta(days=1)

    df = pd.DataFrame(rows, columns=[desc.name for desc in cur.description])
    df2 = df.pivot(index='pressure_dbar', columns='ts', values='val')
    # Fill in missing periods and depths with NaN 
    df2 = df2.reindex(pd.date_range(start=start_time, 
                                    end=end_time, 
                                    freq=timeframe)[:-1], 
                                    axis='columns')
    df2 = df2.reindex(depth_set, axis='index')  # Reindex to include all depths
    df2 = df2.sort_index(axis='columns')  # Sort columns by timestamp
    df2 = df2.sort_index(axis='index')  # Sort index by pressure_dbar
    # Fill NaN values with None for JSON serialization
    # df2 = df2.where(pd.notnull(df2), None)
    return df2.transpose()


def generate_datasets(timeframe, datatype, title, dbconn):

    # Error handling for invalid timeframe or datatype
    if timeframe not in timeframe_sql_map:
        raise ValueError(f"Invalid timeframe: {timeframe}. Valid options are: {list(timeframe_sql_map.keys())}")
    if datatype not in valid_datatypes:
        raise ValueError(f"Invalid datatype: {datatype}. Valid options are: {valid_datatypes}")

    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            # Fetch the data for the specified timeframe and datatype
            cur.execute(f"""
                        SELECT AVG({datatype}) as val, pressure_dbar, 
                        date_bin('{timeframe_sql_map[timeframe]}', startdatetime, '2001-01-01 00:00') as ts
                        FROM interpolated_timeseries 
                        JOIN session_data USING(sessionid)
                        WHERE STARTDATETIME >= '2025-01-01 00:00:00'
                        GROUP BY pressure_dbar, ts
                        ORDER BY ts, pressure_dbar ASC;
            """, {'datatype': datatype, 'timeframe': timeframe})
            rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=[desc.name for desc in cur.description])
    df2 = df.pivot(index='pressure_dbar', columns='ts', values='val')

    # Fill inn missing periods with NaN to make plotly happy
    df2 = df2.reindex(pd.date_range(start=df2.columns.min(), 
                                      end=df2.columns.max(), 
                                      freq=timeframe), 
                                      axis='columns')
    df2 = df2.reindex(depth_set, axis='index')  # Reindex to include all depths
    df2 = df2.sort_index(axis='columns')  # Sort columns by timestamp
    df2 = df2.sort_index(axis='index')  # Sort index by pressure_dbar

    x = df2.columns    # timestamps
    y = (-1 * df2.index).tolist()      # depths
    z = df2.values.round(4).tolist()   # values for the datatype

    graph = dict(
        data=[
            dict(
                z=z,
                x=x,
                y=y,
                type='heatmap',
            ),
        ],
        layout=dict(
            title=title,
            connectgaps=False,
            xaxis=dict(
                title="Tidspunkt",
                enumerated=True,
            ),
        )
    )
    return graph



def get_airtemp(title,dbconn):
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            # Fetch the average air temperature per day
            cur.execute("""
                        SELECT DATE(startdatetime) AS day, AVG(airtemp) AS airtemp 
                        FROM session_data 
                        WHERE airtemp IS NOT NULL 
                        GROUP BY day 
                        ORDER BY day;
            """)
            rows = cur.fetchall()
    days = [row[0] for row in rows]
    airtemps = [round(row[1],2) for row in rows]
    
    graphs = [
        dict(
            data=[
                dict(
                    x=days,
                    y=airtemps,
                    type='Scatter',
                    mode='markers'
                ),
            ],
            layout=dict(
                title=title,
                autosize=True,
                yaxis=dict(
                    title="Temperatur"
                ),
                # height=1000,
                # width=1200,
                xaxis=dict(
                    title="Dag"
                ),
                type="date",
                autorange=True
            )
        )
    ]

    return graphs

def get_count(dbconn):
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            # Fetch the number of dives per day
            cur.execute("""
                        SELECT COUNT(sessionid) as count 
                        FROM session_data;
            """)
            count = cur.fetchone()[0]
    return count


if __name__ == "__main__":
    # print(generate_datasets("1W",'temperature','hallo', dbconn))
    print(get_resampled_day_v1("20250521", "salt", dbconn).transpose())
