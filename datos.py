import json
import requests
import mysql.connector
from datetime import datetime
import math

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
list_P_AT = []
list_P_AT2 = []
list_VG1 = []
list_VG2 = []
list_FRG1 = []
list_FRG2 = []
list_fecha = []
posicion = 0
# En ese json estan todos los resultados
for i in range(len(json)):
    print(json[i])
    fecha = json[i]["result"]["uplink_message"]['received_at']
    envio = json[i]["result"]["uplink_message"]["decoded_payload"]
    #envio = envio.append({"fecha": fecha})
    envio["fecha"] = fecha
    V_G1_i = envio["field1"]
    FR_G1_i = envio["field2"]
    V_G2_i = envio["field3"]
    FR_G2_i = envio["field4"]
    P_AT_i = (3*V_G1_i/(math.sqrt(3))**2)/ 1000
    P_AT2_i = (3*V_G2_i/(math.sqrt(3))**2)/ 1000
    list_VG1.append(V_G1_i)
    list_VG2.append(V_G2_i)
    list_FRG1.append(FR_G1_i)
    list_FRG2.append(FR_G2_i)
    list_P_AT.append(P_AT_i)
    list_P_AT2.append(P_AT2_i)
    list_fecha.append(fecha)
    

#Leer las listas de Potencia para hallar Energia cada 10 minutos
def sumar_elementos(lista):
    if lista == []:
        suma_lista = 0 
    else:
        suma_lista = lista[0] + sumar_elementos(lista[1:])
    return suma_lista
EG1 = sumar_elementos(list_P_AT)/10
EG2 = sumar_elementos(list_P_AT2)/10
print(EG1,EG2)

#Guardar todos los datos al SQL
for posicion in range(len(list_VG1)):
    V_G1 = list_VG1[posicion]
    V_G2 = list_VG2[posicion]
    FR_G1 = list_FRG1[posicion]
    FR_G2 = list_FRG2[posicion]
    P_AT = list_P_AT[posicion]
    P_AT2 = list_P_AT2[posicion]
    fecha = list_fecha[posicion]
    fecha = fecha.replace("T"," ").replace("Z"," ")
    sql="insert into tabla_dataf(V_G1, FR_G1, P_AT,EG1, V_G2, FR_G2, P_AT2, EG2, fecha) values (%s, %s, %s, %s, %s, %s,%s,%s, %s)"
    datos = (V_G1, FR_G1, P_AT,EG1, V_G2, FR_G2, P_AT2, EG2, fecha)
    posicion += 1
    cursor1.execute(sql,datos)
    conexion1.commit()
    print((V_G1, FR_G1, P_AT,EG1, V_G2, FR_G2, P_AT2, EG2, fecha))
conexion1.close()
    

