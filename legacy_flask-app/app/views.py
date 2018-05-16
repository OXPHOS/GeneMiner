import psycopg2
from app import app
from src import __credential__
from flask import jsonify, render_template, request

conn = psycopg2.connect(host=__credential__.host_psql, dbname=__credential__.dbname_psql,
                        user=__credential__.user_psql, password=__credential__.password_psql)
cur = conn.cursor()

@app.route('/index')
def index():
    user = {'nickname': 'Miguel'}  # fake user
    mylist = [1, 2, 3, 4]
    return render_template("index.html", title='Home', user=user, mylist=mylist)


@app.route('/')
@app.route('/example')
def example_request():
    return render_template("stage.html")


@app.route('/example_json')
def example_json():
    dstage = 'Stage I'
    cur.execute('''
        SELECT * FROM patient_info WHERE disease_stage = '%s'
        ''' % (dstage))
    cols = [desc[0] for desc in cur.description]
    vals = cur.fetchall()

    jsonresponse = [{cols[j]:vals[i][j]
                     for j in range(len(cols))}
                    for i in range(len(vals))]
    return jsonify(output=jsonresponse)


@app.route('/')
@app.route('/example', methods=['POST'])
def example_post():
    dtype = request.form["disease_type"]
    dstage = request.form["stage"]
    cur.execute('''
        SELECT * FROM patient_info WHERE disease_type = '%s' AND disease_stage = '%s'
        ''' % (dtype, dstage))
    cols = [desc[0] for desc in cur.description]
    vals = cur.fetchall()

    jsonresponse = [{cols[j]:vals[i][j]
                     for j in range(len(cols))}
                    for i in range(len(vals))]
    return render_template('stageres.html', output=jsonresponse)
