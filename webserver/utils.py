import psycopg2
import pandas as pd

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

def generate_datasets(timeframe, datatype, title, dbconn):
    timeframe_sql_map = {
        "3H": "3 hours",
        "6H": "6 hours",
        "12H": "12 hours",
        "1D": "1 day",
        "1W": "1 week",
        "1M": "1 month"
    }
    valid_datatypes = [ "temperature", "salt", "fluorescens", "turbidity", "oxygene"]
    if datatype == "temp":
        datatype = "temperature"  

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
                        -- WHERE STARTDATETIME >= '2022-01-01 00:00:00'
                        GROUP BY pressure_dbar, ts
                        ORDER BY ts, pressure_dbar DESC;
            """)
            rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=[desc.name for desc in cur.description])
    df2 = df.pivot(index='pressure_dbar', columns='ts', values='val')

    # Fill inn missing periods with NaN to make plotly happy
    df2 = df2.reindex(pd.date_range(start=df2.columns.min(), 
                                      end=df2.columns.max(), 
                                      freq=timeframe), 
                                      axis='columns')
    df2 = df2.sort_index(axis='columns')   

    x = df2.columns    # timestamps
    y = (-1 * df2.index).tolist()      # dephths
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
                # type="date",
                # tickformat="%Y-%m-%d %H:%M:%S",
                # dtick="D1",  # Daily ticks
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


if __name__ == "__main__":
    print(generate_datasets("1W",'temperature','hallo', dbconn))

