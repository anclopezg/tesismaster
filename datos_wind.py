import json
import requests
import mysql.connector
from datetime import datetime
import math

conexion1=mysql.connector.connect(host="localhost", user="admin", passwd="Rita*1234", database="data1")
cursor1=conexion1.cursor()
Application = "instrumento-wind-ud"
APIKey = "NNSXS.5237MEEUB3WCMS5HY7IRF2I4QU4DRBVKASSZ5DA.QTTUEKBQFNJS5WWLXC4QGEIS4AAS32J4RTV6PIQRR37R2C3T4CAA"
Fields = "up.uplink_message.decoded_payload,up.uplink_message.frm_payload"
NumberOfRecords = 10
Fecha = "2021-08-20T00:00:00Z"
URL = "https://nam1.cloud.thethings.network/api/v3/as/applications/" + Application + "/packages/storage/uplink_message?order=-received_at&limit=" + str(NumberOfRecords) + "&field_mask=" + Fields
Header = { "Accept": "text/event-stream", "Authorization": "Bearer " + APIKey }
print("\n\nFetching from data storage...\n")

r = requests.get(URL, headers = Header)
JSON = "{\"data\": [" + r.text.replace("\n\n", ",")[:-1] + "]}";

url = ("URL: {}\n".format(r.url))
status = "Status: {}\n".format(r.status_code)
texto = ("Response: {}\n".format(r.text))
data = json.dumps(json.loads(JSON), indent = 4)
data = json.loads(data)
json = data["data"]
# En ese json estan todos los resultados
for i in range(len(json)):
   
    fecha = json[i]["result"]["uplink_message"]['received_at']
    envio = json[i]["result"]["uplink_message"]["decoded_payload"]
    #envio = envio.append({"fecha": fecha})
    envio["fecha"] = fecha
    velU = envio['velU']
    velV = envio['velV']
    velW = envio['velW']
    fecha = envio["fecha"].replace("T"," ").replace("Z"," ")
    sql="insert into tabla_wind(velU,velV,velW, fecha) values (%s, %s, %s, %s)"
    datos = (velU,velV,velW, fecha)
    cursor1.execute(sql,datos)
    conexion1.commit()
    print((velU,velV,velW, fecha))
conexion1.close()
    

