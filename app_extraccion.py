import pandas as pd
import Clase_extraccion_carga as app1
import ETL
import datetime as dt
import time


def planilla_contador(fecha_inicio, fecha_fin): # hacer que la fecha fin no sea obligatoria
    p2p=app1.extraer_datos_p2p(fecha_inicio,fecha_fin)
    p2p=ETL.ETL_p2p_contador(p2p)
    # Extaer datos de spot para ser cargados en la planila del contador 
    spot_usdt=app1.extraer_datos_spot(fecha_inicio,fecha_fin)[1]
    spot_usdt=ETL.ETL_spot_contador(spot_usdt)
    df=pd.concat([p2p,spot_usdt],axis=0)
    df=df.sort_values('Fecha').reset_index(drop=True)
    app1.cargar_datos_contador(df)


def extraccion_general(fecha_inicial,fecha_final):
    app1.limpiar_sheet_historicos()
    spot=app1.extraer_datos_spot(fecha_inicial,fecha_final)[0]
    spot=ETL.ETL_spot(spot)
    p2p=app1.extraer_datos_p2p(fecha_inicial,fecha_final)
    p2p=ETL.ETL_p2p(p2p)
    df=pd.concat([spot,p2p],axis=0)
    df=df.sort_values('Fecha').reset_index(drop=True)
    app1.cargar_movimientos(df)



def extarccion_top7(fecha_inicial,fecha_final):
    app1.limpiar_sheet_top7()
    spot=app1.extraer_datos_spot(fecha_inicial,fecha_final)[0]
    spot=ETL.ETL_spot(spot)
    p2p=app1.extraer_datos_p2p(fecha_inicial,fecha_final)
    p2p=ETL.ETL_p2p(p2p)
    df=pd.concat([spot,p2p],axis=0)
    df=df.sort_values('Fecha').reset_index(drop=True)

    ventas=df[df['Order_Type']=='SELL']#.tail(7)
    ventas=ventas.iloc[:,2:-1]
    compras=df[df['Order_Type']=='BUY']#.tail(7)
    compras=compras.iloc[:,2:-1]

    app1.cargar_top7_ventas(ventas)
    app1.cargar_top7_compras(compras)
    # time.sleep(50)



