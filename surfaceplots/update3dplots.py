import plotly.plotly as py
import plotly.graph_objs as go
import plotly
import plotly.tools as tls
import numpy as np
# (*) Import module keep track and format current time
import datetime
import time
import pandas as pd
import pymongo

print(plotly.__version__)
py.sign_in('njberland', 'wmmhuc7bkj')


def get_graphs(timeframe, datatype, title):
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

    data = [go.Surface(
        z=z,
        x=x,
        y=y,
    )
    ]
'timeframe'
    layout = go.Layout(
        title=title,
        autosize=True,
        scene=dict(
            zaxis=dict(
                title=datatype
            ),
            yaxis=dict(
                title="Dybde"
            ),
            xaxis=dict(
                title="Tid"
            )
        ),
        width=1000,
        height=1000,
        margin=dict(
            l=65,
            r=50,
            b=65,
            t=90
        )
    )

    return go.Figure(data=data, layout=layout)


ids = []
graphs = []
for g in [{'id': 'temp', 'desc': 'temp vs dybde over tid'},
          {'id': 'oxygene', 'desc': 'oksygen vs dybde over tid'},
          {'id': 'salt', 'desc': 'salt vs dybde over tid'},
          {'id': 'fluorescens', 'desc': 'fluorescens vs dybde over tid'},
          {'id': 'turbidity', 'desc': 'turbiditet vs dybde over tid'}, ]:
    fig = get_graphs('3H', g['id'], g['desc'])
    py.iplot(fig, filename=g['id'] + '-3d-surface')
