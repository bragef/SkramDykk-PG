# -*- coding: utf-8 -*-
from flask import Flask, jsonify, Response, request
from flask import render_template, send_from_directory
from flask_cors import CORS, cross_origin
from datetime import datetime, timedelta
from bson import json_util
import json
import plotly
import os
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from download_frontend import DownloadFrontend

from utils import generate_datasets, generate_freq, get_airtemp, get_valid_years, get_count, get_resampled_day, get_freq
from utils import get_datatype_name, names_new_to_old_map, load_config

configdata = load_config()
PGCONN=configdata['pg_conn']

app = Flask(__name__) # Changed from main_app
# CORS(app)  # Handled by server config

# We add the new download frontend to the original Flask app.
# (These are not dependent on the main app, and can be used independently)
global_prefix = os.environ.get('SCRIPT_NAME', '').rstrip('/')
download_frontend_no = DownloadFrontend(PGCONN, language='no', requests_pathname_prefix = global_prefix + '/download/')
download_frontend_en = DownloadFrontend(PGCONN, language='en', requests_pathname_prefix = global_prefix + '/download/en/')

# Remember no trailing slashes!
app.wsgi_app = DispatcherMiddleware(
    app.wsgi_app, {
    '/download': download_frontend_no.app.server,  
    '/download/en': download_frontend_en.app.server,
})

# the main page
@app.route('/')
def frontpage():
    return render_template('frontpage.html', valid_years=get_valid_years(PGCONN))


@app.route('/api/v1/heatmap/<dtype>.json', methods=['GET'])
def heatmapapi(dtype):
    try: 
        dtype = get_datatype_name(dtype)

        mapping = {'temperature' : 'temperatur vs dybde over tid',
                   'oxygen' : 'oksygen vs dybde over tid',
                   'salinity' : 'saltholdighet vs dybde over tid',
                   'fluorescence' : 'fluorescens vs dybde over tid',
                   'turbidity' : 'turbiditet vs dybde over tid'}
        graph = generate_datasets('3H', dtype, mapping[dtype], PGCONN)
        graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

        resp = Response(graphJSON, status=200, mimetype='application/json')
    except Exception as e:
        error_message = {
            "error": "An error occurred while processing your request.",
            "message": repr(e)
        }
        resp = Response(json.dumps(error_message), status=500, mimetype='application/json')

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
              {'id': 'fluorescence', 'desc': 'fluorescens vs dybde over tid'},
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
# Note: uses old (v1) datatype names (salt, temp, oxygene).  
@app.route('/resampledday/<dtype>/<thisdate>.json')
def resampleddayjson(dtype,thisdate):
    dtype_in = get_datatype_name(dtype)
    
    df = get_resampled_day(thisdate, dtype_in, '3H', PGCONN)
    if df is None:
        return jsonify({"error": "Invalid date format. Use YYYYMMDD."}), 400

    res = []
    dtype_out = names_new_to_old_map.get(dtype_in)
    for index, row in df.iterrows():
        # Convert the index to a string
        output_row = {  'datatype': dtype_out, 
                        'ts' :index.strftime('%Y-%m-%d %H:%M:%S'),
                        'divedata': [ {'pressure(dBAR)':k,dtype_out:v} for k,v in row.items() ] 
                    } 
        res.append(output_row)
    return jsonify(res)

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

@app.route('/api/v2/stats')
def dives():
    dives = get_freq(PGCONN,format="csv")
    resp = Response(dives, status=200, mimetype='text/csv')
    return resp 


# resource that tells how many dives are in the DB
@app.route('/count')
def count():
    return 'dives {}'.format(get_count(PGCONN))

if __name__ == "__main__":
    app.config['DEBUG'] = True # Changed from dispatcher_app.config
    app.run('0.0.0.0', port=8076, threaded=True, use_reloader=True) # Changed from dispatcher_app.run


