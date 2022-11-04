from flask import Flask, render_template, request, jsonify
from flask_mysqldb import MySQL
import json
import pandas as pd

app = Flask(__name__)

app.config['MYSQL_HOST'] = '172.31.16.1'
app.config['MYSQL_USER'] = 'linux'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DB'] = 'flasktest'
app.config['MYSQL_UNIX_SOCKET'] = 'TCP'

mysql = MySQL(app)

@app.route('/data')
def oildata():
    cursor = mysql.connection.cursor()

    wellno = request.args.get('well')
    well = int(wellno)
    df = pd.read_excel('data.xls')
    df.columns = ["API WELL  NUMBER", "Production Year", "QUARTER 1,2,3,4", "OWNER NAME", "COUNTY", "TOWNSHIP", "WELL NAME", "WELL NUMBER", "OIL", "GAS",  "BRINE", "DAYS"]
    df.columns = [c.replace(' ', '_') for c in df.columns]
    df.columns = [c.replace(',', '') for c in df.columns]
    q1 = df[(df.API_WELL__NUMBER == well) & (df.QUARTER_1234 == 1)].values
    q2 = df[(df.API_WELL__NUMBER == well) & (df.QUARTER_1234 == 2)].values
    q3 = df[(df.API_WELL__NUMBER == well) & (df.QUARTER_1234 == 3)].values
    q4 = df[(df.API_WELL__NUMBER == well) & (df.QUARTER_1234 == 4)].values
    oilsum = q1[0][8] + q2[0][8] + q3[0][8] + q4[0][8]
    gassum = q1[0][9] + q2[0][9] + q3[0][9] + q4[0][9]
    brinesum = q1[0][10] + q2[0][10] + q3[0][10] + q4[0][10]
    data = {"oil": oilsum, "gas": gassum, "brine": brinesum}
    json_object = json.dumps(data, indent = 4)
    cursor.execute("INSERT INTO oildata VALUES(%s, %s, %s, %s)", (well, oilsum, gassum, brinesum))
    mysql.connection.commit()
    print(json_object)

    cursor.close()

    return json_object

if __name__ == '__main__':
    app.run('127.0.0.1', 8080)
