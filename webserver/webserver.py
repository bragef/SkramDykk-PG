# -*- coding: utf-8 -*-
from flask import Flask, jsonify, Response
from flask import render_template
import pymongo
from datetime import datetime, timedelta
from bson import json_util
import json
#import logging
import plotly
#import pandas as pd
from flask_cors import CORS, cross_origin

from utils import generate_datasets, generate_freq, get_airtemp

with open("../config.json","r") as f:
    configdata = json.loads(f.read())
    MONGOCONN=configdata['mongoconn']

app = Flask(__name__)
#CORS(app)

# the main page
@app.route('/')
def frontpage():
    return render_template('frontpage.html')
    # return 'Gabriel web server'


@app.route('/api/v1/heatmap/<dtype>.json', methods=['GET'])
def heatmapapi(dtype):
    mapping = {'temp' : 'temp vs dybde over tid',
               'oxygene' : 'oksygen vs dybde over tid',
               'salt' : 'salt vs dybde over tid',
               'fluorescens' : 'fluorescens vs dybde over tid',
               'turbidity' : 'turbiditet vs dybde over tid'}
    graph = generate_datasets('3H', dtype, mapping[dtype], MONGOCONN)
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    #js = json.dumps(data)

    resp = Response(graphJSON, status=200, mimetype='application/json')
    #resp.headers['Link'] = 'http://luisrei.com'

    return resp

    #return graphJSON


@app.route('/api/v1/graph/airtemp.json')
def airtempgraphsapi():

    g = {'id': 'Lufttemperatur', 'desc': 'Lufttemperatur gjennomsnitt pr døgn'}
    graph = get_airtemp(g['desc'],MONGOCONN)

    graphJSON = json.dumps(graph[0], cls=plotly.utils.PlotlyJSONEncoder)
    resp = Response(graphJSON, status=200, mimetype='application/json')

    return resp

@app.route('/api/v1/graph/stats.json')
def statsapi():
    ids = []
    graphs = []
    g ={'id': 'Freq', 'desc': 'Dykk pr dag'}
    graph = generate_freq(g['desc'],MONGOCONN)

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
    for g in [{'id': 'temp', 'desc': 'temp vs dybde over tid'},
              {'id': 'oxygene', 'desc': 'oksygen vs dybde over tid'},
              {'id': 'salt', 'desc': 'salt vs dybde over tid'},
              {'id': 'fluorescens', 'desc': 'fluorescens vs dybde over tid'},
              {'id': 'turbidity', 'desc': 'turbiditet vs dybde over tid'}, ]:
        graph = generate_datasets('3H', g['id'], g['desc'],MONGOCONN)
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
        graph = get_airtemp(g['desc'], MONGOCONN)
        ids.append(g['id'])
        graphs.append(graph[0])

    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('graphview.html',
                           ids=ids,
                           graphJSON=graphJSON)

# this will return a json doc with ALL observations resampled and interpolated for a given datatype
@app.route('/resampled/<dtype>.json')
def resampledjson(dtype):
    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.resampled
    alldives = []
    # get all dives for a timeframe and datatype
    divecursor = coll.find({'timeframe':'3H', 'datatype':dtype},{"_id":0}).sort('ts', pymongo.ASCENDING)
    for dive in divecursor:
        alldives.append(dive)
    return jsonify(alldives) #json.dumps(alldives, default=json_util.default)


# return a json doc with all observations for a given type on a given day
# the day format must be YYYYMMDD this will return a json doc with ALL observations resampled and interpolated for a given datatype
@app.route('/resampledday/<dtype>/<thisdate>.json')
def resampleddayjson(dtype,thisdate):
    print("hello")
    year = thisdate[0:4]
    month = thisdate[4:6]
    day = thisdate[6:8]
    start = datetime(int(year),int(month),int(day),0,0,0)
    end = start + timedelta(days=1)

    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.resampled
    alldives = []
    # get all dives for a timeframe and datatype
    divecursor = coll.find({'timeframe':'3H', 'datatype':dtype,'ts':{'$lt': end, '$gte': start}},
                           {"_id":0, 'timeframe':0}).sort('ts', pymongo.ASCENDING)
    for dive in divecursor:
        newdive = sorted(dive['divedata'], key=lambda k: k['pressure(dBAR)'])
        dive['divedata'] = newdive
        alldives.append(dive)
                        
    return jsonify(alldives) #json.dumps(alldives, default=json_util.default)

# this will return a json doc with all raw dives for a given year
@app.route('/raw/<year>.json')
def rawjson(year):
    # find a from and to statement
    start = datetime(int(year),1,1,0,0,0)
    end = datetime(int(year)+1,1,1,0,0,0)

    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.gabrielraw
    alldives = []
    # get all dives for a timeframe and datatype
    divecursor = coll.find({'startdatetime':{'$lt': end, '$gte': start}},{"_id":0}).sort('startdatetime', pymongo.ASCENDING)
    for dive in divecursor:
        alldives.append(dive)
    return jsonify(alldives) # (alldives, default=json_util.default)


@app.route('/dives')
def dives():
    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.gabrielraw
    cdives = []
    # get all dives
    divecursor = coll.find().sort('startdatetime', pymongo.DESCENDING).limit(100)
    for dive in divecursor:
        cdives.append(dive)
    return render_template('listdives.html', dives=cdives)


@app.route('/dives/<diveid>')
def onedive(diveid):
    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.gabrielraw
    searchid = int(diveid)
    dive = coll.find_one({"profilenumber": searchid})
    if dive != None:
        retstring = json.dumps(dive, default=json_util.default)
        return json.dumps(retstring)  # 'dive number {}'.format(dive['_id'])
    else:
        return 'None '


@app.route('/stats')
def stats():
    ids = []
    graphs = []
    for g in [{'id': 'Freq', 'desc': 'Dykk pr dag'},]:
        graph = generate_freq(g['desc'],MONGOCONN)
        ids.append(g['id'])
        graphs.append(graph[0])

    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('graphview.html',
                           ids=ids,
                           graphJSON=graphJSON)


# resource that tells how many dives are in the DB
@app.route('/count')
def count():
    coll = pymongo.MongoClient(MONGOCONN, uuidRepresentation="standard").saivasdata.gabrielraw
    return 'dives {}'.format(coll.find().count())



if __name__ == "__main__":
    app.config['DEBUG'] = True
    app.run('0.0.0.0')
