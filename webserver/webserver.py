# -*- coding: utf-8 -*-
from flask import Flask, jsonify, Response, request
from flask import render_template, send_from_directory
from flask_cors import CORS, cross_origin
from datetime import datetime, timedelta
from bson import json_util
import json
import plotly

from utils import generate_datasets, generate_freq, get_airtemp, get_valid_years, get_count, get_resampled_day, get_datatype_name, names_new_to_old_map

with open("../config.json","r") as f:
    configdata = json.loads(f.read())
    PGCONN=configdata['pg_conn']

app = Flask(__name__)
# CORS(app)  # Handled by server config

# the main page
@app.route('/')
def frontpage():
    return render_template('frontpage.html', valid_years=get_valid_years(PGCONN))


@app.route('/api/v1/heatmap/<dtype>.json', methods=['GET'])
def heatmapapi(dtype):
    dtype = get_datatype_name(dtype)

    mapping = {'temperature' : 'temperatur vs dybde over tid',
               'oxygen' : 'oksygen vs dybde over tid',
               'salinity' : 'saltholdighet vs dybde over tid',
               'fluorescens' : 'fluorescens vs dybde over tid',
               'turbidity' : 'turbiditet vs dybde over tid'}
    graph = generate_datasets('3H', dtype, mapping[dtype], PGCONN)
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    resp = Response(graphJSON, status=200, mimetype='application/json')

    return resp


@app.route('/api/v1/graph/airtemp.json')
def airtempgraphsapi():

    g = {'id': 'Lufttemperatur', 'desc': 'Lufttemperatur gjennomsnitt pr døgn'}
    graph = get_airtemp(g['desc'],PGCONN)

    graphJSON = json.dumps(graph[0], cls=plotly.utils.PlotlyJSONEncoder)
    resp = Response(graphJSON, status=200, mimetype='application/json')

    return resp


@app.route('/api/v1/graph/stats.json')
def statsapi():
    ids = []
    graphs = []
    g ={'id': 'Freq', 'desc': 'Dykk pr dag'}
    graph = generate_freq(g['desc'],PGCONN)

    graphJSON = json.dumps(graph[0], cls=plotly.utils.PlotlyJSONEncoder)
    resp = Response(graphJSON, status=200, mimetype='application/json')

    return resp


# this resource will return a web-page with all graphs for the lifetime of the DTS
# it is not very fast because it contains a LOT of data - this can be improved by either updating a plot at plot.ly
# or using javascript queries
@app.route('/allgraphs')
def allgraphs():
    ids = []
    graphs = []
    for g in [{'id': 'temperature', 'desc': 'temp vs dybde over tid'},
              {'id': 'oxygen', 'desc': 'oksygen vs dybde over tid'},
              {'id': 'salinity', 'desc': 'salt vs dybde over tid'},
              {'id': 'fluorescens', 'desc': 'fluorescens vs dybde over tid'},
              {'id': 'turbidity', 'desc': 'turbiditet vs dybde over tid'}, ]:
        graph = generate_datasets('3H', g['id'], g['desc'],PGCONN)
        ids.append(g['id'])
        graphs.append(graph)

    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('graphview.html',
                           ids=ids,
                           graphJSON=graphJSON)


@app.route('/static/<path:path>')
def send_template(path):
    return send_from_directory('static', path)

@app.route('/airtemp')
def airtempgraphs():
    ids = []
    graphs = []
    for g in [{'id': 'Lufttemperatur', 'desc': 'Lufttemperatur gjennomsnitt pr døgn'},]:
        graph = get_airtemp(g['desc'], PGCONN)
        ids.append(g['id'])
        graphs.append(graph[0])

    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('graphview.html',
                           ids=ids,
                           graphJSON=graphJSON)


# return a json doc with all observations for a given type on a given day
# the day format must be YYYYMMDD this will return a json doc with ALL observations 
# resampled and interpolated for a given datatype.
# (In use from ektedata.uib.no, keep for compatibility)
# Note: uses old datatype names (salt, temp, oxygene).  
@app.route('/resampledday/<dtype>/<thisdate>.json')
def resampleddayjson(dtype,thisdate):
    dtype_in = get_datatype_name(dtype)
    
    df = get_resampled_day(thisdate, dtype_in, '3H', PGCONN)
    if df is None:
        return jsonify({"error": "Invalid date format. Use YYYYMMDD."}), 400

    res = []
    dtype_out = names_new_to_old_map.get(dtype)
    for index, row in df.iterrows():
        # Convert the index to a string
        output_row = {  'datatype': dtype_out, 
                        'ts' :index.strftime('%Y-%m-%d %H:%M:%S'),
                        'divedata': [ {'pressure(dBAR)':k,dtype_out:v} for k,v in row.items() ] 
                    } 
        res.append(output_row)
    return jsonify(res)


# # this will return a json doc with ALL observations resampled and interpolated for a given datatype
# @app.route('/resampled/<dtype>.json')
# def resampledjson(dtype):
#    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.resampled
#    alldives = []
#    # get all dives for a timeframe and datatype
#    divecursor = coll.find({'timeframe':'3H', 'datatype':dtype},{"_id":0}).sort('ts', pymongo.ASCENDING)
#    for dive in divecursor:
#        alldives.append(dive)
#    return jsonify(alldives) #json.dumps(alldives, default=json_util.default)


# # this will return a json doc with all raw dives for a given year
# @app.route('/raw/<year>.json')
# def rawjson(year):
#    # find a from and to statement
#    start = datetime(int(year),1,1,0,0,0)
#    end = datetime(int(year)+1,1,1,0,0,0)
#
#    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.gabrielraw
#    alldives = []
#    # get all dives for a timeframe and datatype
#    divecursor = coll.find({'startdatetime':{'$lt': end, '$gte': start}},{"_id":0}).sort('startdatetime', pymongo.ASCENDING)
#    for dive in divecursor:
#        alldives.append(dive)
#    return jsonify(alldives) # (alldives, default=json_util.default)

# @app.route('/dives')
# def dives():
#    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.gabrielraw
#    cdives = []
#    # get all dives
#    divecursor = coll.find().sort('startdatetime', pymongo.DESCENDING).limit(100)
#    for dive in divecursor:
#        cdives.append(dive)
#    return render_template('listdives.html', dives=cdives)


# @app.route('/dives/<diveid>')
# def onedive(diveid):
#    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.gabrielraw
#    searchid = int(diveid)
#    dive = coll.find_one({"profilenumber": searchid})
#    if dive != None:
#        retstring = json.dumps(dive, default=json_util.default)
#        return json.dumps(retstring)  # 'dive number {}'.format(dive['_id'])
#    else:
#        return 'None '


@app.route('/stats')
def stats():
    ids = []
    graphs = []
    for g in [{'id': 'Freq', 'desc': 'Dykk pr dag'},]:
        graph = generate_freq(g['desc'],PGCONN)
        ids.append(g['id'])
        graphs.append(graph[0])

    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('graphview.html',
                           ids=ids,
                           graphJSON=graphJSON)


# resource that tells how many dives are in the DB
@app.route('/count')
def count():
    return 'dives {}'.format(get_count(PGCONN))

if __name__ == "__main__":
    app.config['DEBUG'] = True
    app.run('0.0.0.0', port=8074, threaded=True, use_reloader=True)


