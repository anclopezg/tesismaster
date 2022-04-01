import json
import requests
import mysql.connector
from datetime import datetime
import math
import pandas as pd

conexion1=mysql.connector.connect(host="localhost", user="admin", passwd="Rita*1234", database="data1")
cursor1=conexion1.cursor()
Application = "generadores-vortice-ud"
APIKey = "NNSXS.BXVZP63NH6FRMFEGLBXXNFM6ZNSTTXB5RFSIOKY.R6W37NGQYDHD3ZPEJKZOMDQK43K5I5IZP43GWDGLCESAQKIAUFZA"
Fields = "up.uplink_message.decoded_payload,up.uplink_message.frm_payload"
NumberOfRecords = 50
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
df_salida = pd.DataFrame(columns=[])
# En ese json estan todos los resultados
for i in range(len(json)):
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
    df_tmp = pd.DataFrame({'V_G1':V_G1,'V_G2':V_G2,'FR_G1':FR_G1,'FR_G2':FR_G2,'P_AT':P_AT,'P_AT2':P_AT2,'EG1':EG1,'EG2':EG2,'fecha':fecha},index=[0])
    df_salida = df_salida.append(df_tmp, ignore_index = True)
    #cursor1.execute(sql,datos)
    #conexion1.commit()
    #print((V_G1, FR_G1, P_AT,EG1, V_G2, FR_G2, P_AT2, EG2, fecha))
#conexion1.close()
import json
import requests
import mysql.connector
from datetime import datetime
import math
import pandas as pd
Application = "instrumento-wind-ud"
APIKey = "NNSXS.5237MEEUB3WCMS5HY7IRF2I4QU4DRBVKASSZ5DA.QTTUEKBQFNJS5WWLXC4QGEIS4AAS32J4RTV6PIQRR37R2C3T4CAA"
Fields = "up.uplink_message.decoded_payload,up.uplink_message.frm_payload"
NumberOfRecords = 50
Fecha = "2021-08-20T00:00:00Z"
URL = "https://nam1.cloud.thethings.network/api/v3/as/applications/" + Application + "/packages/storage/uplink_message?order=-received_at&limit=" + str(NumberOfRecords) + "&field_mask=" + Fields
Header = { "Accept": "text/event-stream", "Authorization": "Bearer " + APIKey }
df_entrada = pd.DataFrame(columns=[])
print("\n\nFetching from data storage...\n")

r = requests.get(URL, headers = Header)
JSON2 = "{\"data\": [" + r.text.replace("\n\n", ",")[:-1] + "]}";

url = ("URL: {}\n".format(r.url))
status = "Status: {}\n".format(r.status_code)
texto = ("Response: {}\n".format(r.text))
data2 = json.dumps(json.loads(JSON2), indent = 4)
data2 = json.loads(data2)
json2 = data2["data"]
# En ese json estan todos los resultados
for i in range(len(json2)):
    fecha = json2[i]["result"]["uplink_message"]['received_at']
    envio = json2[i]["result"]["uplink_message"]["decoded_payload"]
    #envio = envio.append({"fecha": fecha})
    envio["fecha"] = fecha
    velU = envio['velU']
    velV = envio['velV']
    velW = envio['velW']
    xang = envio["xang"]
    xejz = envio["xejz"]
    xmag = envio["xmag"]
    fecha = envio["fecha"].replace("T"," ").replace("Z"," ")
    sql="insert into tabla_data_wind(velU,velV,velW,xang,xejz,xmag, fecha) values (%s,%s,%s,%s, %s, %s, %s)"
    datos = (velU,velV,velW,xang,xejz,xmag,fecha)
    df_tmp2 = pd.DataFrame({'velU':velU,'velV':velV,'velW':velW,'xang':xang,'xejz':xejz,'xmag':xmag,'fecha':fecha},index=[0])
    df_entrada = df_entrada.append(df_tmp2, ignore_index = True)
    #cursor1.execute(sql,datos)
    #conexion1.commit()
    #print((velU,velV,velW, xang,xejz,xmag,fecha))
#conexion1.close()

posicion = 0
df_entrada["valor"] = 0
df_entrada["fecha_final"] = 0
for valor in df_entrada["fecha"]:
    valor1 = datetime.strptime(valor.split(".")[0], "%Y-%m-%d %H:%M:%S")
    df_entrada["valor"][posicion] = valor1
    df_entrada["fecha_final"][posicion] = valor1.strftime("%Y-%m-%d %H:%M")
    posicion +=1
df_entrada = df_entrada.groupby(["fecha_final"]).mean()
posicion = 0
df_salida["valor"] = 0
df_salida["fecha_final"] = 0
for valor in df_salida["fecha"]:
    valor1 = datetime.strptime(valor.split(".")[0], "%Y-%m-%d %H:%M:%S")
    df_salida["valor"][posicion] = valor1
    df_salida["fecha_final"][posicion] = valor1.strftime("%Y-%m-%d %H:%M")
    if df_salida["V_G1"][posicion] == 0:
        df_salida["FR_G1"][posicion] = 0
    if df_salida["V_G2"][posicion] == 0:
        df_salida["FR_G2"][posicion] = 0
    posicion +=1
df_salida = df_salida.groupby(["fecha_final"]).mean()
df_final = pd.merge(left = df_entrada,right = df_salida, left_on = "fecha_final", right_on = "fecha_final", how = "inner")
df_final= df_final.rename_axis('fecha_final').reset_index()
for index,row in df_final.iterrows():
    sql="insert into tabla_data_wind(velU,velV,velW,xang,xejz,xmag,V_G1, FR_G1, P_AT,EG1, V_G2, FR_G2, P_AT2, EG2, fecha) values (%s, %s, %s, %s, %s, %s,%s,%s, %s,%s,%s,%s,%s,%s,%s)"
    datos = (row.velU,row.velV,row.velW,row.xang,row.xejz,row.xmag,row.V_G1, row.FR_G1, row.P_AT,row.EG1, row.V_G2, row.FR_G2, row.P_AT2, row.EG2, row.fecha_final)
    cursor1.execute(sql,datos)
    conexion1.commit()
conexion1.close()
