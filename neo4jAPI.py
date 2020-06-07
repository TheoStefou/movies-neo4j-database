from flask import Flask, request

import json
import queries

from datetime import datetime

app = Flask(__name__)
appname = '/app/'

'''
curl -d "{\"name\": \"Tom Hanks\"}" -H "Content-Type:application/json" -X POST http://localhost:5000/app/person-id-from-name
'''

@app.route(appname+'person-id-from-name', methods=['GET'])
def name_to_id_listener():

    try:
        name = request.args['name']
        return json.dumps(queries.id_from_name(name), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query1', methods=['GET'])
def query1_listener():

    try:
        person_id = request.args['id']
        int(person_id)
        return json.dumps(queries.query1(person_id), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query2-director', methods=['GET'])
def query2_director_listener():

    try:
        person_id = request.args['id']
        int(person_id)
        return json.dumps(queries.query2_director(person_id), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}
        
@app.route(appname+'query2-writer', methods=['GET'])
def query2_writer_listener():

    try:
        person_id = request.args['id']
        int(person_id)
        return json.dumps(queries.query2_writer(person_id), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query2-producer', methods=['GET'])
def query2_producer_listener():

    try:
        person_id = request.args['id']
        int(person_id)
        return json.dumps(queries.query2_producer(person_id), default=str), 200, { 'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

@app.route(appname+'query3', methods=['GET'])
def query3_listener():

    try:
        person_id = request.args['id']
        fr = request.args['from']
        to = request.args['to']
        int(person_id)
        datetime.strptime(fr, '%Y-%m-%d')
        datetime.strptime(to, '%Y-%m-%d')
        return json.dumps(queries.query3(person_id, fr, to), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query4-original', methods=['GET'])
def query4_original_listener():

    try:
        person_id = request.args['id']
        int(person_id)
        return json.dumps(queries.query4_original(person_id), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query4-spoken', methods=['GET'])
def query4_spoken_listener():

    try:
        person_id = request.args['id']
        int(person_id)
        return json.dumps(queries.query4_spoken(person_id), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query5', methods=['GET'])
def query5_listener():

    try:
        k = request.args['k']
        assert int(k) > 0
        return json.dumps(queries.query5(k), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query6-budget', methods=['GET'])
def query6_budget_listener():

    try:
        k = request.args['k']
        assert int(k) > 0
        return json.dumps(queries.query6_budget(k), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query6-revenue', methods=['GET'])
def query6_revenue_listener():

    try:
        k = request.args['k']
        assert int(k) > 0
        return json.dumps(queries.query6_revenue(k), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query7', methods=['GET'])
def query7_listener():

    try:
        year = request.args['year']
        assert int(year) >= 0 and int(year) <= datetime.now().year
        return json.dumps(queries.query7(year), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query8', methods=['GET'])
def query8_listener():

    try:
        year = request.args['year']
        assert int(year) >= 0 and int(year) <= datetime.now().year
        return json.dumps(queries.query8(year), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query9', methods=['GET'])
def query9_listener():

    try:
        year = request.args['year']
        assert int(year) >= 0 and int(year) <= datetime.now().year
        return json.dumps(queries.query9(year), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query10', methods=['GET'])
def query10_listener():

    try:
        person_id = request.args['id']
        int(person_id)
        return json.dumps(queries.query10(person_id), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}
        

@app.route(appname+'query11', methods=['GET'])
def query11_listener():

    try:
        person_id = request.args['id']
        int(person_id)
        return json.dumps(queries.query11(person_id), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}
    

@app.route(appname+'query12', methods=['GET'])
def query12_listener():

    try:
        person_id = request.args['id']
        k = request.args['k']
        int(person_id)
        assert int(k) > 0
        return json.dumps(queries.query12(person_id, k), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}


@app.route(appname+'query13', methods=['GET'])
def query13_listener():

    return json.dumps(queries.query13(), default=str), 200, {'content-type':'application/json'}

@app.route(appname+'query14', methods=['GET'])
def query14_listener():

    try:
        years = request.args['years']
        assert int(years) >= 0
        return json.dumps(queries.query14(years), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}
        
    

@app.route(appname+'query15', methods=['GET'])
def query15_listener():
        
    try:
        return json.dumps(queries.query15(), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query16-best', methods=['GET'])
def query16_best_listener():

    try:
        return json.dumps(queries.query16_best(), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query16-worst', methods=['GET'])
def query16_worst_listener():

    try:
        return json.dumps(queries.query16_worst(), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query17-best', methods=['GET'])
def query17_best_listener():
    
    try:
        return json.dumps(queries.query17_best(), default=str), 200, {'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query17-worst', methods=['GET'])
def query17_worst_listener():
    
    try:
        return json.dumps(queries.query17_worst(), default=str), 200, { 'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}

@app.route(appname+'query18', methods=['GET'])
def query18_listener():

    try:
        return json.dumps(queries.query18(), default=str), 200, { 'content-type':'application/json'}
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, {'content-type':'application/json'}