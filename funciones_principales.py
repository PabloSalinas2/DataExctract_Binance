import pandas as pd
import Clase_extraccion as app1  # cambiar por nombre extraccion_datos
import ETL
import carga_datos_gsheet as load

def cargar_planilla_contador(fecha_inicio, fecha_fin): # hacer que la fecha fin no sea obligatoria / Ex funcion planilla_contador
    load.limpiar_sheet(1,'A3')
    p2p=app1.extraer_datos_p2p(fecha_inicio,fecha_fin)
    p2p=ETL.ETL_p2p_contador(p2p)
    # Extaer datos de spot para ser cargados en la planila del contador 
    spot_usdt=app1.extraer_datos_spot(fecha_inicio,fecha_fin)[1]
    spot_usdt=ETL.ETL_spot_contador(spot_usdt)
    df=pd.concat([p2p,spot_usdt],axis=0)
    df=df.sort_values('Fecha').reset_index(drop=True)

    ventas=df[df['Order_Type']=='SELL']
    compras=df[df['Order_Type']=='BUY']
  
    load.cargar_datos_miplanilla(1,'A3',compras)
    load.cargar_datos_miplanilla(1,'J3',ventas)



def cargar_planilla_misregistros(fecha_inicial,fecha_final): # Ex funcion extraccion_top7
    "Extrae datos de Binance, los procesa y luego los carga en planilla de google sheet donde se registran mis movimientos de arbitraje"
    load.limpiar_sheet(0,'A3')
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

    load.cargar_datos_miplanilla(0,'A3',ventas)
    load.cargar_datos_miplanilla(0,'H3',compras)

  



