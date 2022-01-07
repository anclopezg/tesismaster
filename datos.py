
import json
import requests
import mysql.connector
from datetime import datetime

conexion1=mysql.connector.connect(host="localhost", user="admin", passwd="Rita*1234", database="data1")
cursor1=conexion1.cursor()
Application = "generadores-vortice-ud"
APIKey = "NNSXS.BXVZP63NH6FRMFEGLBXXNFM6ZNSTTXB5RFSIOKY.R6W37NGQYDHD3ZPEJKZOMDQK43K5I5IZP43GWDGLCESAQKIAUFZA"
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
contador = 4
for i in range(len(json)):
    contador = contador + 1
    fecha = json[i]["result"]["uplink_message"]['received_at']
    envio = json[i]["result"]["uplink_message"]["decoded_payload"]
    #envio = envio.append({"fecha": fecha})
    envio["fecha"] = fecha
    ID = contador + 1
    print(ID)
    V_G1 = envio["field1"]
    FR_G1 = envio["field2"]
    V_G2 = envio["field3"]
    FR_G2 = envio["field4"]
    fecha = envio["fecha"].replace("T"," ").replace("Z"," ")
    sql="insert into tabla_dataf(V_G1, FR_G1, V_G2, FR_G2,fecha) values (%s, %s, %s, %s,%s)"
    datos = (V_G1,FR_G1,V_G2,FR_G2,fecha)
    cursor1.execute(sql,datos)
    conexion1.commit()
    print(envio)
conexion1.close()

