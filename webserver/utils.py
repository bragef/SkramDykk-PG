import psycopg2
import pandas as pd
import os
import json

timeframe_sql_map = {
    "3H": "3 hours",
    "6H": "6 hours",
    "12H":"12 hours",
    "1D": "1 day",
    "1W": "1 week",
    "1M": "1 month"
}
    
valid_datatypes = [ "temperature", "salinity", "fluorescence", "turbidity", "oxygen"]
# For api v1 compatibility
names_old_to_new_map = {
    "salt": "salinity",
    "fluorescens": "fluorescence",
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

# v2 api
def get_freq(dbconn, format='json'):
    # Get the number of dives per day
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT DATE(startdatetime) as date, COUNT(sessionid) as dives 
                        FROM session_data 
                        GROUP BY DATE(startdatetime) 
                        ORDER BY DATE(startdatetime);
            """)
            rows = cur.fetchall()
    
    df = pd.DataFrame(rows, columns=['date', 'dives'])
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')

    if format == 'dataframe':
        return df
    elif format == 'csv':
        return df.to_csv() # index=True by default
    else: # 'json' or default
        if df.empty:
            return {"dates": [], "counts": []}
        # Convert datetime index to string for JSON serialization
        dates_list = df.index.strftime('%Y-%m-%d').tolist()
        counts_list = df['dives'].tolist()
        return {"dates": dates_list, "counts": counts_list}

# v1 api and fig
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

def get_download_data(dbconn, start_date_str, end_date_str, depth_range_bounds_list,
                      resampling_interval_str, depth_aggregation_str, selected_parameters_list):
    """
    Fetch downloadable data from the interpolated_timeseries table
    based on specified parameters.
    
    Args:
        dbconn (str): Database connection string.
        start_date_str (str): Start date in 'YYYY-MM-DD' format.
        end_date_str (str): End date in 'YYYY-MM-DD' format.
        depth_range_bounds_list (list): List containing two numbers [min_depth, max_depth].
        resampling_interval_str (str): Resampling interval key (e.g., '3H', '1D', 'all').
        depth_aggregation_str (str): Depth aggregation method ('all_selected' or 'average').
        selected_parameters_list (list): List of frontend parameter names to download.

    Returns:
        pandas.DataFrame: DataFrame containing the requested data.

    Raises:
        ValueError: If input parameters are invalid.
    """
    # 1. Validate parameters
    if not selected_parameters_list:
        raise ValueError("No parameters selected for download.")

    try:
        start_date = pd.to_datetime(start_date_str).date()
        end_date = pd.to_datetime(end_date_str).date()
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD for start and end dates.")

    if not (isinstance(depth_range_bounds_list, list) and len(depth_range_bounds_list) == 2 and
            all(isinstance(d, (int, float)) for d in depth_range_bounds_list)):
        raise ValueError("depth_range_bounds_list must be a list of two numbers [min_depth, max_depth].")

    actual_depths_to_query = sorted([d for d in depth_set if depth_range_bounds_list[0] <= d <= depth_range_bounds_list[1]])
    if not actual_depths_to_query:
        # Return an empty DataFrame with expected columns if no depths match
        # This helps prevent errors in the frontend if it expects a DataFrame
        print(f"Warning: No valid depths found in the range {depth_range_bounds_list} based on available depths: {depth_set}. Returning empty DataFrame.")
        
        # Construct expected column names for the empty DataFrame
        expected_cols = ['ts']
        if depth_aggregation_str == 'all_selected':
            expected_cols.append('pressure_dbar')
        for p_frontend in selected_parameters_list:
            expected_cols.append(p_frontend)
        
        # Create an empty DataFrame with appropriate index
        idx = pd.MultiIndex(levels=[[], []], codes=[[], []], names=['ts', 'pressure_dbar']) if depth_aggregation_str == 'all_selected' else pd.Index([], name='ts')
        # Remove 'ts' and 'pressure_dbar' from expected_cols as they are handled by index
        data_cols = [col for col in expected_cols if col not in ['ts', 'pressure_dbar']]
        
        return pd.DataFrame(index=idx, columns=data_cols)


    if resampling_interval_str != 'all' and resampling_interval_str not in timeframe_sql_map:
        raise ValueError(f"Invalid resampling_interval_str: {resampling_interval_str}. Valid options are 'all' or {list(timeframe_sql_map.keys())}")

    if depth_aggregation_str not in ['all_selected', 'average']:
        raise ValueError(f"Invalid depth_aggregation_str: {depth_aggregation_str}. Must be 'all_selected' or 'average'.")

    db_parameters_map = {} # Stores frontend_name -> db_name
    for p_frontend in selected_parameters_list:
        try:
            p_db = get_datatype_name(p_frontend)
            if p_db not in valid_datatypes: # Should be caught by get_datatype_name
                 raise ValueError(f"Parameter {p_frontend} (maps to {p_db}) is not a valid queryable datatype.")
            db_parameters_map[p_frontend] = p_db
        except ValueError as e:
            raise ValueError(f"Invalid parameter: {p_frontend}. {e}")

    # 2. Construct SQL query
    final_select_clauses = []
    group_by_terms = []

    # Time expression, and grouping for time if resampling
    if resampling_interval_str == 'all':
        final_select_clauses.append("sd.startdatetime AS ts")
        # group_by_terms remains empty for 'all' data - no time-based grouping
    elif resampling_interval_str == '1M':
        final_select_clauses.append("date_trunc('month', sd.startdatetime) AS ts")
        group_by_terms.append("ts") # Group by the time alias
    else: # Other resampling intervals
        time_sql = timeframe_sql_map[resampling_interval_str]
        # 2001-01-03 00:00:00' is a random monday to ensure the week binning aligns with the week start
        final_select_clauses.append(f"date_bin('{time_sql}', sd.startdatetime, TIMESTAMP '2001-01-03 00:00:00') AS ts")
        group_by_terms.append("ts") # Group by the time alias

    # Depth column selection for SELECT clause, and grouping for depth if resampling
    if depth_aggregation_str == 'all_selected':
        final_select_clauses.append("it.pressure_dbar")
        if resampling_interval_str != 'all': # Only add pressure_dbar to GROUP BY if resampling
            group_by_terms.append("it.pressure_dbar")

    # Parameter expressions
    for p_frontend, p_db in db_parameters_map.items():
        if resampling_interval_str != 'all':
            # Resampling is active, so aggregate the parameter
            final_select_clauses.append(f'AVG(it.{p_db}) AS "{p_frontend}"')
        else:
            # No resampling ('all' data), select the direct value
            final_select_clauses.append(f'it.{p_db} AS "{p_frontend}"')
    
    sql_query = f"SELECT {', '.join(final_select_clauses)} " \
                f"FROM interpolated_timeseries it JOIN session_data sd USING(sessionid) " \
                f"WHERE DATE(sd.startdatetime) BETWEEN %s AND %s"
    
    query_params = [start_date, end_date]

    if actual_depths_to_query: # Only add depth filter if there are depths to query
        sql_query += " AND it.pressure_dbar = ANY(%s::real[])" # Explicit cast for array parameter
        query_params.append(actual_depths_to_query)
    
    # Add GROUP BY clause only if there are terms to group by (i.e., when resampling)
    if group_by_terms:
        sql_query += f" GROUP BY {', '.join(group_by_terms)}"
    
    # Construct ORDER BY clause
    # Always order by time ('ts'). If selecting individual depths, also order by depth.
    order_by_clause_parts = ['ts']
    if depth_aggregation_str == 'all_selected':
        # it.pressure_dbar is selected if depth_aggregation_str == 'all_selected'
        # and it should be part of the ordering for consistency,
        # especially when data is not grouped.
        order_by_clause_parts.append('it.pressure_dbar')
    
    sql_query += f" ORDER BY {', '.join(order_by_clause_parts)};"

    # 3. Execute query and fetch data
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            # print("Executing SQL:", cur.mogrify(sql_query, tuple(query_params)).decode()) # For debugging
            cur.execute(sql_query, tuple(query_params))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
    
    df = pd.DataFrame(rows, columns=colnames)

    # 4. Post-process DataFrame
    if not df.empty:
        if 'ts' in df.columns:
            df['ts'] = pd.to_datetime(df['ts'])

        # Explicitly convert data columns (from selected_parameters_list) to numeric.
        for p_frontend in selected_parameters_list:
            if p_frontend in df.columns:
                df[p_frontend] = pd.to_numeric(df[p_frontend], errors='coerce')
        
        try:
            if depth_aggregation_str == 'all_selected' and 'pressure_dbar' in df.columns:
                # df['pressure_dbar'] = pd.to_numeric(df['pressure_dbar'], errors='coerce') 
                df = df.set_index(['ts', 'pressure_dbar'])
            else:
                df = df.set_index('ts')
            df = df.sort_index()
            # Ensure column order matches selected_parameters_list if possible (excluding index cols)
            ordered_data_cols = [p for p in selected_parameters_list if p in df.columns]
            
            if ordered_data_cols:
                df = df[ordered_data_cols]

        except Exception as e:
            print("Error during DataFrame post-processing (indexing, sorting, reordering).")
            print(f"DataFrame dtypes before error:\\n{df.dtypes}")
            print(f"DataFrame head before error:\\n{df.head()}")
            print(f"Exception: {e}")
            raise
    return df

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
    df2 = df2.reindex(depth_set, axis='index')  # include all depths
    df2 = df2.sort_index(axis='columns')  # Sort columns by timestamp
    df2 = df2.sort_index(axis='index')  # Sort index by depth

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
            cur.execute("""
                        SELECT COUNT(sessionid) as count 
                        FROM session_data;
            """)
            count = cur.fetchone()[0]
    return count


def get_surface_data(dbconn, start_date_str, end_date_str, resampling_interval_str, selected_parameters_list):
    """
    Downloads surface data (e.g., air temperature, wind speed) based on specified parameters.

    Args:
        dbconn (str): Database connection string.
        start_date_str (str): Start date in 'YYYY-MM-DD' format.
        end_date_str (str): End date in 'YYYY-MM-DD' format.
        resampling_interval_str (str): Resampling interval key (e.g., '3H', '1D', 'all').
        selected_parameters_list (list): List of surface parameter names to download.
                                         (e.g., ['airtemp', 'windspeed'])
    Returns:
        pandas.DataFrame: DataFrame containing the requested surface data with a 'ts' DatetimeIndex.
    Raises:
        ValueError: If input parameters are invalid.
    """
    # 1. Validate parameters
    if not selected_parameters_list:
        raise ValueError("No surface parameters selected for download.")

    try:
        start_date = pd.to_datetime(start_date_str).date()
        end_date = pd.to_datetime(end_date_str).date()
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD for start and end dates.")

    if resampling_interval_str != 'all' and resampling_interval_str not in timeframe_sql_map:
        raise ValueError(
            f"Invalid resampling_interval_str: {resampling_interval_str}. "
            f"Valid options are 'all' or {list(timeframe_sql_map.keys())}"
        )

    valid_surface_params = ['airtemp', 'windspeed', 'winddirection', 'airpressure'] 
    for param in selected_parameters_list:
        if param not in valid_surface_params:
            raise ValueError(f"Invalid surface parameter: {param}. Supported: {valid_surface_params}")

    # 2. Construct SQL query
    final_select_clauses = []
    group_by_terms = []

    # Time expression
    if resampling_interval_str == 'all':
        final_select_clauses.append("startdatetime AS ts")
        # When resampling is 'all', we don't group by default, so group_by_terms remains empty.
        # This will result in all matching rows being returned without aggregation.
    elif resampling_interval_str == '1M':
        final_select_clauses.append("date_trunc('month', startdatetime) AS ts")
        group_by_terms.append("ts")
    else:
        time_sql = timeframe_sql_map[resampling_interval_str]
        final_select_clauses.append(f"date_bin('{time_sql}', startdatetime, TIMESTAMP '2001-01-03 00:00:00') AS ts")
        group_by_terms.append("ts")

    # Parameter expressions
    for param_frontend in selected_parameters_list:
        param_db = param_frontend 
        if resampling_interval_str != 'all':

            if param_db in ['airtemp', 'windspeed', 'airpressure']: # Numeric params
                final_select_clauses.append(f'AVG({param_db}) AS "{param_frontend}"')
            elif param_db == 'winddirection': 
                 # Use direction of average wind vector for averaging wind direction. 
                 final_select_clauses.append(f'DEGREES(ATAN2((AVG(windspeed * SIN(RADIANS(winddirection)))),AVG(windspeed * COS(RADIANS(winddirection))))) AS "{param_frontend}"')                             
            else: # Default to direct select if not numeric and not \\'all\\' (will pick one value from group)
                final_select_clauses.append(f'{param_db} AS "{param_frontend}"')
        else: # resampling_interval_str == 'all'
            final_select_clauses.append(f'{param_db} AS "{param_frontend}"')

    sql_query = f"SELECT {', '.join(final_select_clauses)} " \
                f"FROM session_data " \
                f"WHERE DATE(startdatetime) BETWEEN %s AND %s"

    query_params = [start_date, end_date]

    if group_by_terms: # Add GROUP BY only if there are terms (i.e., not 'all' or 'all' with specific grouping)
        sql_query += f" GROUP BY {', '.join(group_by_terms)}"

    order_by_terms = ['ts'] 
    sql_query += f" ORDER BY {', '.join(order_by_terms)};"

    # 3. Execute query and fetch data
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            # print("Executing Surface SQL:", cur.mogrify(sql_query, tuple(query_params)).decode()) # For debugging
            cur.execute(sql_query, tuple(query_params))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=colnames)

    # 4. Post-process DataFrame
    if not df.empty:
        if 'ts' in df.columns:
            df['ts'] = pd.to_datetime(df['ts'])
            df = df.set_index('ts')
        df = df.sort_index()
        # Ensure column order matches selected_parameters_list if possible
        ordered_data_cols = [p for p in selected_parameters_list if p in df.columns]
        df = df[ordered_data_cols]
    else: # If no data, return empty DataFrame with expected columns
        expected_cols = ['ts'] + selected_parameters_list
        df = pd.DataFrame(columns=selected_parameters_list)
        df.index = pd.to_datetime([]).rename('ts')

    return df

def get_data_raw(dbconn, start_date_str, end_date_str):
    """
    Downloads all raw data columns from the raw_timeseries table for a given date range.

    Args:
        dbconn (str): Database connection string.
        start_date_str (str): Start date in 'YYYY-MM-DD' format.
        end_date_str (str): End date in 'YYYY-MM-DD' format.

    Returns:
        pandas.DataFrame: DataFrame containing the requested raw data.

    Raises:
        ValueError: If input parameters are invalid.
    """
    # 1. Validate parameters
    try:
        start_date = pd.to_datetime(start_date_str).date()
        end_date = pd.to_datetime(end_date_str).date()
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD for start and end dates.")

    # 2. Construct SQL query
    # Selects session startdatetime as 'ts' and all columns from raw_timeseries
    sql_query = """
        SELECT sd.startdatetime AS ts, rt.*
        FROM raw_timeseries rt
        JOIN session_data sd ON rt.sessionid = sd.sessionid
        WHERE DATE(sd.startdatetime) BETWEEN %s AND %s
        ORDER BY ts, rt.sessionid, rt.seq;
    """
    query_params = [start_date, end_date]

    # 3. Execute query and fetch data
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query, tuple(query_params))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
    
    df = pd.DataFrame(rows, columns=colnames)

    # 4. Basic Post-process DataFrame
    if not df.empty:
        if 'ts' in df.columns:
            df['ts'] = pd.to_datetime(df['ts'])
        
        # Convert pressure_dbar to numeric if it exists from rt.*
        if 'pressure_dbar' in df.columns:
            df['pressure_dbar'] = pd.to_numeric(df['pressure_dbar'], errors='coerce')
        
        # Other columns from rt.* will retain their types as fetched from DB
        # or converted by pandas.DataFrame constructor.
        # More specific type conversions could be added here if needed.

    return df

def get_data_sessions(dbconn, start_date_str, end_date_str):
    """
    Downloads all columns from the session_data table for a given date range.

    Args:
        dbconn (str): Database connection string.
        start_date_str (str): Start date in 'YYYY-MM-DD' format.
        end_date_str (str): End date in 'YYYY-MM-DD' format.

    Returns:
        pandas.DataFrame: DataFrame containing the requested session data.

    Raises:
        ValueError: If input parameters are invalid.
    """
    # 1. Validate parameters
    try:
        start_date = pd.to_datetime(start_date_str).date()
        end_date = pd.to_datetime(end_date_str).date()
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD for start and end dates.")

    # 2. Construct SQL query
    # Selects all columns from session_data
    sql_query = """
        SELECT *, ST_AsText(location) AS location
        FROM session_data
        WHERE DATE(startdatetime) BETWEEN %s AND %s
        ORDER BY startdatetime;
    """
    query_params = [start_date, end_date]

    # 3. Execute query and fetch data
    with psycopg2.connect(dbconn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query, tuple(query_params))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
    
    df = pd.DataFrame(rows, columns=colnames)

    # 4. Basic Post-process DataFrame
    if not df.empty:
        if 'startdatetime' in df.columns:
            df['startdatetime'] = pd.to_datetime(df['startdatetime'])
        if 'enddatetime' in df.columns:
            df['enddatetime'] = pd.to_datetime(df['enddatetime'], errors='coerce') # Coerce errors for potential NaT
        
        # Other columns will retain their types as fetched from DB
        # or converted by pandas.DataFrame constructor.

    return df

def load_config():
    """
    Searches for config.json in the current directory (relative to this file), parent, and grandparent directories,
    loads it, and returns a dictionary with the config.

    Returns:
        dict: Configuration data loaded from config.json.

    Raises:
        FileNotFoundError: If config.json is not found in any of the searched directories.
        json.JSONDecodeError: If config.json is found but cannot be parsed.
    """
    base_dir = os.path.dirname(__file__)
    search_paths = [base_dir, os.path.join(base_dir, ".."), os.path.join(base_dir, "..", "..")]
    for path in search_paths:
        config_path = os.path.join(path, "config.json")
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(f"Error parsing {config_path}: {e}")
    raise FileNotFoundError("config.json not found in any of the searched directories (relative to this file): . , .. , ...")

def load_translator(lang_code='no'):
    """
    Loads the translation file for the given language code and returns a translation function t().
    Usage: t = load_translator('no') or t = load_translator('en')
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
