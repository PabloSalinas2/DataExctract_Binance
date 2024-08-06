# Librerias
import pandas as pd
import extraccion_datos_binance as app1  
import ETL
import carga_datos_gsheet as load
import os 
from binance.spot import Spot


def cargar_planilla_contador(fecha_inicio, fecha_fin): 
    """
    Extrae datos de Binance entre 2 fechas, los procesa y transforma para finalmente cargarlos en una hoja de calculo de google
    """
    hoja='contador'
    load.clear_sheet(hoja,'A3')
    p2p=app1.extraer_datos_p2p(fecha_inicio,fecha_fin)
    p2p=ETL.ETL_p2p_contador(p2p)
    # Extaer datos de spot para ser cargados en la planila del contador 
    spot_usdt=app1.extraer_datos_spot(fecha_inicio,fecha_fin)[1]
    spot_usdt=ETL.ETL_spot_contador(spot_usdt)
    df=pd.concat([p2p,spot_usdt],axis=0)
    df=df.sort_values('Fecha').reset_index(drop=True)

    ventas=df[df['Order_Type']=='SELL']
    compras=df[df['Order_Type']=='BUY']
  
    
    load.load_to_sheet(hoja,'A3',compras)
    load.load_to_sheet(hoja,'J3',ventas)



def cargar_planilla_misregistros(fecha_inicial,fecha_final): 
    """
    Extrae datos de Binance entre 2 fechas, los procesa y transforma para finalmente cargarlos en una hoja de calculo de google
    """
    hoja='datos'
    load.clear_sheet(hoja,'A3')
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
    
    load.load_to_sheet(hoja,'A3',ventas)
    load.load_to_sheet(hoja,'H3',compras)

  

# Añadida recientemente 
# def cotizaciones_spot():
#     api_key = os.getenv("API_KEY")
#     secret_key = os.getenv("SECRET_KEY")    
#     cliente=Spot(api_key,secret_key)

#     datos=cliente.ticker_price(symbols=['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','DOGEUSDT','ADAUSDT'])

#     df=pd.DataFrame(datos)
#     df['price']=df['price'].astype(float)
#     cotizaciones=df['price'].round(4)
#     cotizaciones=cotizaciones.astype(str)
#     cotizaciones=cotizaciones.str.replace('.',',')
#     cotizaciones=pd.DataFrame(cotizaciones)
#     cotizaciones=cotizaciones.rename(columns={'price':'Cot. Spot (USDT)'})
#     cotizaciones['Cripto']=['BTC','ETH','BNB','XRP','DOGE','ADA']
#     cotizaciones['Cot. P2P(ARS)']=''
#     cotizaciones['Cot USDT']=''
#     new_order=['Cripto','Cot. P2P(ARS)','Cot. Spot (USDT)','Cot USDT']
#     cotizaciones=cotizaciones[new_order]
#     return cotizaciones
   

    #load.load_to_sheet('Simulador','C8',cotizaciones) # ACTUALIZAR: No cargar en sheet, mostrar en la misma interfaz de la app 

   
def cotizaciones_spot():
    api_key = os.getenv("API_KEY")
    secret_key = os.getenv("SECRET_KEY")    
    cliente=Spot(api_key,secret_key)

    datos=cliente.ticker_price(symbols=['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','DOGEUSDT','ADAUSDT'])

    df=pd.DataFrame(datos)
    df['price']=df['price'].astype(float)
    cotizaciones=df['price'].round(4)
    
    return cotizaciones
   