import pymongo
import pandas as pd


def generate_freq(title):
    coll = pymongo.MongoClient().saivasdata.gabrielraw

    l = list(coll.find(projection={"startdatetime": True, "_id": False}).sort([("startdatetime", pymongo.ASCENDING)]))
    df = pd.DataFrame(l)
    df.index = df['startdatetime']
    ndf = df.groupby(df.index.date).count()

    graphs = [
        dict(
            data=[
                dict(
                    x=ndf.index,
                    y=ndf['startdatetime'],
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


def generate_datasets(timeframe, datatype, title):
    coll = pymongo.MongoClient().saivasdata.resampled
    tempz = []
    y = []
    x = []
    for curs in coll.find({"timeframe": timeframe, "datatype": datatype}).sort("ts", pymongo.ASCENDING):
        # print(curs)

        col = []
        # sort the list so the dictionaries are sorted according to pressure
        newlist = sorted(curs['divedata'], key=lambda k: -k['pressure(dBAR)'])
        for i in newlist:
            col.append(i[datatype])
            if -i['pressure(dBAR)'] not in y:
                y.append(-i['pressure(dBAR)'])

        tempz.append(col)
        x.append(curs['ts'])
    z = list(map(list, zip(*tempz)))
    y = sorted(y)

    graph = dict(
        data=[
            dict(
                z=z,
                x=x,
                y=y,
                type='heatmap'
            ),
        ],
        layout=dict(
            title=title
        )
    )

    return graph


def get_airtemp(title):
    coll = pymongo.MongoClient().saivasdata.gabrielraw

    l = list(coll.find(projection={"startdatetime": True, "_id": False, "airtemp": True}).sort(
        [("startdatetime", pymongo.ASCENDING)]))

    df = pd.DataFrame(l)

    df.index = df['startdatetime']
    df = df.drop('startdatetime', 1).sort_index()
    df = df.resample('24H').mean()

    # print(df)
    graphs = [
        dict(
            data=[
                dict(
                    x=df.index,
                    y=df['airtemp'].tolist(),
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
                height=910,
                width=1620,
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
    #    print(generate_freq("hallo"))

    #   print(generate_datasets("3H",'temp','hallo'))

    print(get_airtemp("Lufttemperatur"))
