from binance.spot import Spot
from dotenv import load_dotenv
import os
import pandas as pd
import datetime as dt
import pygsheets
import time
import numpy as np
import requests
import hmac
import hashlib




#-----------------------------------------------------------------------------------------------------------------------------
def extraccion_general_p2p(tipo,fecha_inicial='',fecha_final='',pagina=''):

    if fecha_inicial!='':
        fecha_inicial=fecha_a_unix(fecha_inicial)
    
    if fecha_final!='':
        fecha_final=fecha_a_unix(fecha_final)+ 86399000
        
    # load_dotenv()
    # # # Claves de la API 
    api_key = os.getenv("API_KEY")
    secret_key = os.getenv("SECRET_KEY")
    
    # api_key=config.API_KEY
    # secret_key=config.SECRET_KEY

    # Endpoint de la API
    url = 'https://api.binance.com/sapi/v1/c2c/orderMatch/listUserOrderHistory'

    # Obtener el timestamp de un servidor externo
    time_response = requests.get('http://worldtimeapi.org/api/timezone/Etc/UTC')
    if time_response.status_code == 200:
        timestamp = time_response.json()['unixtime'] * 1000  # Convertir a milisegundos
    else:
        print(f'Error al obtener el timestamp: {time_response.status_code}')
        print(time_response.json())
        exit()

    # Parámetros de la solicitud
    params = {
        'tradeType': tipo,
        'timestamp': timestamp,
        'startTimestamp':fecha_inicial,
        'endTimestamp':fecha_final,
        'page': pagina
    }

    # Crear la cadena de consulta
    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])

    # Calcular la firma utilizando HMAC-SHA256
    signature = hmac.new(secret_key.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    # Añadir la firma a los parámetros de la consulta
    params['signature'] = signature

    # Encabezados, incluyendo la API key
    headers = {
        'X-MBX-APIKEY': api_key
    }

    # Realizar la solicitud GET al endpoint de la API
    response = requests.get(url, headers=headers, params=params)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        data = response.json()
        df=pd.DataFrame(data['data'])
        return df
        
    else:
        print(f'Error al acceder a la API: {response.status_code}')
        print(response.json())






def extraccion_general_spot(symbol,fecha_inicial='',fecha_final=''):

    # if fecha_inicial!='':
    #     fecha_inicial=fecha_a_unix(fecha_inicial)
    
    # if fecha_final!='':
    #     fecha_final=fecha_a_unix(fecha_final)+ 86399000

    # Claves de la API 
    load_dotenv()
    api_key = os.getenv("API_KEY")
    secret_key = os.getenv("SECRET_KEY")

    # api_key=config.API_KEY
    # secret_key=config.SECRET_KEY

    # Endpoint de la API
    url = 'https://api.binance.com/api/v3/myTrades'

    # Obtener el timestamp de un servidor externo
    time_response = requests.get('http://worldtimeapi.org/api/timezone/Etc/UTC')
    if time_response.status_code == 200:
        timestamp = time_response.json()['unixtime'] * 1000  # Convertir a milisegundos
    else:
        print(f'Error al obtener el timestamp: {time_response.status_code}')
        print(time_response.json())
        exit()

    # Parámetros de la solicitud
    params = {
        'symbol': symbol,
        'startTime':fecha_inicial,
        'endTime':fecha_final,
        'timestamp': timestamp
    }

    # Crear la cadena de consulta
    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])

    # Calcular la firma utilizando HMAC-SHA256
    signature = hmac.new(secret_key.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    # Añadir la firma a los parámetros de la consulta
    params['signature'] = signature

    # Encabezados, incluyendo la API key
    headers = {
        'X-MBX-APIKEY': api_key
    }
    
    # Realizar la solicitud GET al endpoint de la API
    response = requests.get(url, headers=headers, params=params)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        data = response.json()
        df=pd.DataFrame(data)
        return df
        
    else:
        print(f'Error al acceder a la API: {response.status_code}')
        print(response.json())


#-----------------------------------------------------------------------------------------------------------------------------

api_key = os.getenv("API_KEY")
secret_key = os.getenv("SECRET_KEY")


# api_key=config.API_KEY
# secret_key=config.SECRET_KEY

#Establecer conexion con Binance p2p (meterlo en un Try)
client = Spot(api_key=api_key, api_secret=secret_key)

def fecha_a_unix(fecha): # Fecha en formato dd/mm/AA
    año=int(fecha.split('/')[2])
    mes=int(fecha.split('/')[1])
    dia=int(fecha.split('/')[0])
    fecha_unix=dt.datetime(año,mes,dia,0,0,0)
    fecha_unix=int(fecha_unix.timestamp()*1000)
    return fecha_unix

#-----------------------------------------------------------------------------------------------------------------------------

# def extraer_datos_spot(inicio,fin):
#     "Extrae los movimientos en spot de las principales criptos de mi interes, para usarlo para mi planilla de arbitraje, no para la del contador"
#     fecha_inicial=fecha_a_unix(inicio)
#     fecha_final=fecha_a_unix(fin)
#     # df_total=[]
#     para_mi=[]
#     para_contador=[]
#     fecha=fecha_inicial
#     while fecha<=fecha_final:
#         # Extraccion de datos de spot
#         spot_USDTARS=client.my_trades(['USDTARS'],startTime=fecha,endTime=fecha+86399000)
#         spot_BTCUSDT=client.my_trades(['BTCUSDT'],startTime=fecha,endTime=fecha+86399000)
#         spot_ETHUSDT=client.my_trades(['ETHUSDT'],startTime=fecha,endTime=fecha+86399000)
#         spot_BNBUSDT=client.my_trades(['BNBUSDT'],startTime=fecha,endTime=fecha+86399000)
#         spot_XRPUSDT=client.my_trades(['XRPUSDT'],startTime=fecha,endTime=fecha+86399000)
#         spot_DOGEUSDT=client.my_trades(['DOGEUSDT'],startTime=fecha,endTime=fecha+86399000)
#         spot_ADAUSDT=client.my_trades(['ADAUSDT'],startTime=fecha,endTime=fecha+86399000)
#         datos=[spot_USDTARS,spot_BTCUSDT,spot_ETHUSDT,spot_BNBUSDT,spot_XRPUSDT,spot_DOGEUSDT,spot_ADAUSDT]
#         # convertir a dataframe 
#         df_final=[]
#         for i in datos:
#             df=pd.DataFrame(i)
#             df_final.append(df)
#         # Datos para contador 
#         para_contador.append(df_final[0])
#         df_final=pd.concat(df_final)
#         # Datos para planilla arbitraje
#         para_mi.append(df_final)
#         fecha+=86400000
   
#     para_mi=pd.concat(para_mi).reset_index(drop=True)
#     para_contador=pd.concat(para_contador).reset_index(drop=True)
#     return para_mi,para_contador  # devuelve 2 dataframes, uno con datos para mi y otros con datos para el contador



def extraer_datos_spot(inicio,fin):
    fecha_inicial=fecha_a_unix(inicio)
    fecha_final=fecha_a_unix(fin)

    para_mi=[]
    para_contador=[]
    fecha=fecha_inicial
    while fecha<=fecha_final:
        # Extraccion de datos de spot
        spot_USDTARS=extraccion_general_spot(symbol='USDTARS',fecha_inicial=fecha,fecha_final=fecha+86399000)
        spot_BTCUSDT=extraccion_general_spot(symbol='BTCUSDT',fecha_inicial=fecha,fecha_final=fecha+86399000)
        spot_ETHUSDT=extraccion_general_spot(symbol='ETHUSDT',fecha_inicial=fecha,fecha_final=fecha+86399000)
        spot_BNBUSDT=extraccion_general_spot(symbol='BNBUSDT',fecha_inicial=fecha,fecha_final=fecha+86399000)
        spot_XRPUSDT=extraccion_general_spot(symbol='XRPUSDT',fecha_inicial=fecha,fecha_final=fecha+86399000)
        spot_DOGEUSDT=extraccion_general_spot(symbol='DOGEUSDT',fecha_inicial=fecha,fecha_final=fecha+86399000)
        spot_ADAUSDT=extraccion_general_spot(symbol='ADAUSDT',fecha_inicial=fecha,fecha_final=fecha+86399000)
        datos=[spot_USDTARS,spot_BTCUSDT,spot_ETHUSDT,spot_BNBUSDT,spot_XRPUSDT,spot_DOGEUSDT,spot_ADAUSDT]
        # convertir a dataframe 
        
   
        df_final=[]
        for i in datos:
            df=pd.DataFrame(i)
            df_final.append(df)
        # Datos para contador 
        para_contador.append(df_final[0])
        df_final=pd.concat(df_final)
        # Datos para planilla arbitraje
        para_mi.append(df_final)
        fecha+=86400000
   
    para_mi=pd.concat(para_mi).reset_index(drop=True)
    para_contador=pd.concat(para_contador).reset_index(drop=True)
    return para_mi,para_contador  # devuelve 2 dataframes, uno con datos para mi y otros con datos para el contador
    

#-----------------------------------------------------------------------------------------------------------------------------


# def extraer_datos_p2p(inicio,fin):
#     fecha_inicial=fecha_a_unix(inicio)
#     fecha_final=fecha_a_unix(fin)+ 86399000

#     try:
#         tipo_transaccion=['BUY','SELL']
#         for tipo in tipo_transaccion:
#             lista_datos=[0]
#             datos=[]
#             pagina=0
#             while len(lista_datos)!=0:
#                 pagina+=1
#                 lista_datos=client.c2c_trade_history(tradeType=tipo,startTimestamp=fecha_inicial,endTimestamp=fecha_final,page=pagina)['data']
#                 df=pd.DataFrame(lista_datos)
#                 if df.shape[0]>0:
#                     datos.append(df)

#             if tipo=='BUY':
#                 compras=pd.concat(datos)
            
#             else:
#                 ventas=pd.concat(datos)
#         datos=pd.concat([compras,ventas]).reset_index(drop=True)
#         return datos
#     except Exception as e:
#         print('No se pudo establecer la conexion con Binance :',e)

def extraer_datos_p2p(inicio,fin): 

    filas_compras=1
    filas_ventas=1
    pag_compras=1
    pag_ventas=1
    compras_total=pd.DataFrame()
    ventas_total=pd.DataFrame()
    while filas_compras!=0:
        compras_parcial=extraccion_general_p2p('BUY',fecha_inicial=inicio,fecha_final=fin,pagina=pag_compras)
        compras_total=pd.concat([compras_total,compras_parcial]).reset_index(drop=True)

        filas_compras=compras_parcial.shape[0]
        pag_compras+=1

    while filas_ventas!=0:
        ventas_parcial=extraccion_general_p2p('SELL',fecha_inicial=inicio,fecha_final=fin,pagina=pag_ventas)
        ventas_total=pd.concat([ventas_total,ventas_parcial]).reset_index(drop=True)

        filas_ventas=ventas_parcial.shape[0]
        pag_ventas+=1
    
    datos_p2p=pd.concat([compras_total,ventas_total]).reset_index(drop=True)
    datos_p2p=datos_p2p.sort_values('createTime',ascending=True).reset_index(drop=True)
    
    return datos_p2p

#-----------------------------------------------------------------------------------------------------------------------------


def limpiar_sheet_top7():
    "Limpia de la primar hoja del archivo 'Planilla Arbitraje' las celdas donde se cargaron las ultimas 7 compras- ventas "
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    worksheet=gc.open('Planilla Arbitraje')[0]
    worksheet.clear(start='A3')#,end='N13')


def limpiar_sheet_historicos():
    "Limpia de la primar hoja del archivo 'Planilla Arbitraje' las celdas donde se cargaron los datos completos entre 2 fechas "
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    worksheet=gc.open('Planilla Arbitraje')[0]
    worksheet.clear('A15')
    


def cargar_movimientos(datos_procesados):
    "Carga un dataframe con los datos de compras y ventas comprendidos entre 2 fechas, en el documento 'Planilla Arbitraje' "
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    worksheet=gc.open('Planilla Arbitraje')[0]
    # Establece la posicion donde se cargara el dataframe 
    inicio=worksheet.get_col(1,include_tailing_empty=False)
    celda_inicio='A'+str(len(inicio)+1)
    # cargar dataframe a google sheets 
    if celda_inicio=='A1':
        worksheet.set_dataframe(datos_procesados,start=celda_inicio,copy_index=False,copy_head=True)
    else:
        worksheet.set_dataframe(datos_procesados,start=celda_inicio,copy_index=False,copy_head=False)



def cargar_top7_ventas(datos_procesados):
    "Carga  las ultimas 7 ventas realizadas en una fecha determinada, la carga se hace en el documento 'Planilla Arbitraje'"
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    worksheet=gc.open('Planilla Arbitraje')[0]
    worksheet.set_dataframe(datos_procesados,start='A3',copy_index=False,copy_head=False)



def cargar_top7_compras(datos_procesados):
    "Carga las ultimas 7 compras realizadas en una fecha determinada, la carga se hace en el documento 'Planilla Arbitraje'"
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    worksheet=gc.open('Planilla Arbitraje')[0]
    worksheet.set_dataframe(datos_procesados,start='H3',copy_index=False,copy_head=False)



def cargar_datos_contador(datos_procesados):
    "Carga un dataframe en la primer hoja del documento '02 - datos_arbitraje' " 
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    worksheet=gc.open('02 - datos_arbitraje')[0]
    # Limpia hoja de calculo
    worksheet.clear('A2')
    # Establece la posicion donde se cargara el dataframe 
    inicio=worksheet.get_col(1,include_tailing_empty=False)
    celda_inicio='A'+str(len(inicio)+1)
    # cargar dataframe a google sheets 
    if celda_inicio=='A1':
        worksheet.set_dataframe(datos_procesados,start=celda_inicio,copy_index=False,copy_head=True)
    else:
        worksheet.set_dataframe(datos_procesados,start=celda_inicio,copy_index=False,copy_head=False)





# inicio='01/05/2024'
# fin='05/05/2024'
# print(extraer_datos_p2p(inicio,fin)[1])