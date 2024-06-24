import pandas as pd
import datetime as dt
import numpy as np


def ETL_spot(df_final): 
    
    # Agregar columnas 
    if df_final.shape[0]!=0:
        df_final['Order_Type']=np.where(df_final['symbol']!='USDTARS',~df_final["isBuyer"],df_final['isBuyer'])
        df_final['Order_Type']=np.where(df_final['symbol']!='BTCARS',~df_final["isBuyer"],df_final['isBuyer'])
        df_final['Order_Type']=df_final['Order_Type'].replace({True:'BUY',False:'SELL'})
        df_final['Medio_pago']='-'
        df_final['Exchange_']='Binance Spot'
        df_final['Monto_usdt']=np.where(df_final['symbol']=='USDTARS',df_final['qty'],df_final['quoteQty'])
        # Seleccion de columnas reelevantes 
        df_final=df_final.loc[:,['orderId','Order_Type','time','symbol','qty','quoteQty','price','Monto_usdt','Exchange_','Medio_pago']] 
        # Cambiar tipos 
        df_final['qty']=df_final['qty'].astype(float)
        df_final['quoteQty']=df_final['quoteQty'].astype(float)
        df_final['price']=df_final['price'].astype(float)
        df_final['Monto_usdt']=df_final['Monto_usdt'].astype(float)
        # Agrupar datos 
        df_final=df_final.groupby('orderId').agg({'Order_Type':'first',
                                'time':'first',
                                'symbol':'first',
                                'qty':'sum',
                                'quoteQty':'sum',
                                'price':'mean',
                                'Monto_usdt': 'sum',              
                                'Exchange_':'first',
                                'Medio_pago':'first'})
        df_final=df_final.reset_index(drop=False)
        # Cambiar formato fecha 
        df_final['time']=df_final['time'].apply(lambda x: dt.datetime.fromtimestamp(x/1000))
        # Cambiar nombres 
        df_final.rename({'orderId':'Order_Number','time':'Fecha','symbol':'Tipo_Cripto','qty':'Cantidad_cripto','quoteQty':'Precio_total','price':'Precio_unitario'},axis=1,inplace=True)
        # Cambiar puntos por comas 
        df_final['Cantidad_cripto']=df_final['Cantidad_cripto'].astype(str).str.replace('.',',')
        df_final['Monto_usdt']=df_final['Monto_usdt'].astype(str).str.replace('.',',')
        df_final['Precio_unitario']=df_final['Precio_unitario'].astype(str).str.replace('.',',')
        df_final['Precio_total']=df_final['Precio_total'].astype(str).str.replace('.',',')
        # Dar formato a fecha 
        df_final['Fecha']=df_final["Fecha"].dt.strftime("%d-%m-%Y %H:%M:%S")
        df_final['Fecha']=pd.to_datetime(df_final['Fecha'],format="%d-%m-%Y %H:%M:%S")
        
        # Ordenar datos por fecha 
        df_final=df_final.sort_values('Fecha').reset_index(drop=True)
    
    else:
        df_final=None
    
    return df_final




def ETL_spot_contador(df_final): 
    
    
    

    if df_final.shape[0]==0:
        df_final=df_final
    else:
        if  'USDTARS' in df_final['symbol'].to_list(): # solo se aplican las modificacionse en caso de que el dataframe de los movimientos de spot contengan datos
            df_final['Order_Type']=np.where(df_final['symbol']!='USDTARS',~df_final["isBuyer"],df_final['isBuyer'])
            df_final['Order_Type']=df_final['Order_Type'].replace({True:'BUY',False:'SELL'})
            df_final['Medio_pago']='-'
            df_final['Exchange_']='Binance Spot'

            # Seleccion de columnas reelevantes 
            df_final=df_final.loc[:,['orderId','Order_Type','time','symbol','qty','quoteQty','price','Exchange_','Medio_pago']] 


            # Cambiar tipos 
            df_final['qty']=df_final['qty'].astype(float)
            df_final['quoteQty']=df_final['quoteQty'].astype(float)
            df_final['price']=df_final['price'].astype(float)
            # Agrupar datos 
            df_final=df_final.groupby('orderId').agg({'Order_Type':'first',
                                    'time':'first',
                                    'symbol':'first',
                                    'qty':'sum',
                                    'quoteQty':'sum',
                                    'price':'mean',             
                                    'Exchange_':'first',
                                    'Medio_pago':'first'})
            df_final=df_final.reset_index(drop=False)
            # Cambiar formato fecha 
            df_final['time']=df_final['time'].apply(lambda x: dt.datetime.fromtimestamp(x/1000))
            # Cambiar nombres 
            df_final.rename({'orderId':'Order_Number','time':'Fecha','symbol':'Tipo_Cripto','qty':'Cantidad_Cripto','quoteQty':'Monto_ARS','price':'Precio_unitario'},axis=1,inplace=True)
            # Cambiar puntos por comas 
            df_final['Cantidad_Cripto']=df_final['Cantidad_Cripto'].astype(str).str.replace('.',',')
            df_final['Precio_unitario']=df_final['Precio_unitario'].astype(str).str.replace('.',',')
            df_final['Monto_ARS']=df_final['Monto_ARS'].astype(str).str.replace('.',',')
            # Dar formato a fecha 
            df_final['Fecha']=df_final["Fecha"].dt.strftime("%d-%m-%Y %H:%M:%S")
            df_final['Fecha']=pd.to_datetime(df_final['Fecha'],format="%d-%m-%Y %H:%M:%S")
            
            # Ordenar datos por fecha 
            df_final=df_final.sort_values('Fecha').reset_index(drop=True)
        else:
            # Agregar columnas
            df_final['Order_Type']=''
            df_final['Exchange_']=''
            df_final['Medio_pago']='-'
            # Seleccion de columnas reelevantes 
            df_final=df_final.loc[:,['orderId','Order_Type','time','symbol','qty','quoteQty','price','Exchange_','Medio_pago']] 
    
    return df_final




def ETL_p2p(df_final):
    
    # Convierte la columna "createTime" a formato fecha
    df_final['createTime']=df_final['createTime'].apply(lambda x: dt.datetime.fromtimestamp(x/1000))
    # Filtrar por estados "completo"
    df_final=df_final[df_final['orderStatus']=='COMPLETED']
    # Ordenar dataframe por fecha en orden ascenente
    df_final=df_final.sort_values('createTime',ascending=True).reset_index(drop=True)
    # Añadir columna Exchange y columna medio de pago
    df_final['Exchange_']='Binance P2P'


    # ADAPTACION PRELIMINAR, para considerar comisiones
    # MODIFICACION QUE CONTEMPLA COMISION, SI LANZA ERROR BORRAR ESTA LINEA Y ANALIZAR EL CODIGO
    df_final['amount']=df_final['amount'].astype(float)
    df_final['commission']=df_final['commission'].astype(float)
    df_final['amount']=np.where(df_final['tradeType']=='SELL',df_final['amount']+df_final['commission'],df_final['amount']-df_final['commission'])




    df_final['Medio_pago']='-'
    df_final['Monto_usdt']=np.where((df_final['asset']=='USDT') | (df_final['asset']=='USDC'),df_final['amount'],'')  # MODIFICACION 



    # Eleccion de columnas reelevantes 
    df_final=df_final.loc[:,['orderNumber','tradeType','createTime','asset','amount','totalPrice','unitPrice','Monto_usdt','Exchange_','Medio_pago']]
    # Cambiar tipo (Adaptar numeros para google sheet) , ppara otros casos remplazar por float
    df_final['amount']=df_final['amount'].astype(str).str.replace('.',',')
    df_final['totalPrice']=df_final['totalPrice'].astype(str).str.replace('.',',')
    df_final['unitPrice']=df_final['unitPrice'].astype(str).str.replace('.',',')
    df_final['orderNumber']=df_final['orderNumber'].astype(str).str.replace('.',',')
    df_final['Monto_usdt']=df_final['Monto_usdt'].astype(str).str.replace('.',',')
    

    df_final=df_final.sort_values(by='createTime')
    # Renombrar columnas
    df_final.rename({'orderNumber':'Order_Number','tradeType':'Order_Type','createTime':'Fecha','asset':'Tipo_Cripto','amount':'Cantidad_cripto','totalPrice':'Precio_total','unitPrice':'Precio_unitario'},axis=1,inplace=True) # cambiar nombre segun DB mysql
    # Ordenar por fecha 
    return df_final


def ETL_p2p_contador(df_final):
    "Funcion encargada de aplicar ETL a los datos con la estructura del p2p"
    
    # Convierte la columna "createTime" a formato fecha
    df_final['createTime']=df_final['createTime'].apply(lambda x: dt.datetime.fromtimestamp(x/1000))
    # Filtrar por estados "completo"
    df_final=df_final[df_final['orderStatus']=='COMPLETED']
    # Ordenar dataframe por fecha en orden ascenente
    df_final=df_final.sort_values('createTime',ascending=True).reset_index(drop=True)
    # Añadir columna Exchange y columna medio de pago
    df_final['Exchange_']='Binance P2P'
    df_final['Medio_pago']='-'
    df_final['amount']=df_final['amount'].astype(float)
    df_final['commission']=df_final['commission'].astype(float)
    df_final['amount']=np.where(df_final['tradeType']=='SELL',df_final['amount']+df_final['commission'],df_final['amount']-df_final['commission'])
    

    # Eleccion de columnas reelevantes 
    df_final=df_final.loc[:,['orderNumber','tradeType','createTime','asset','amount','totalPrice','unitPrice','Exchange_','Medio_pago']]
    # Cambiar tipo (Adaptar numeros para google sheet) , ppara otros casos remplazar por float
    df_final['amount']=df_final['amount'].astype(str).str.replace('.',',')
    df_final['totalPrice']=df_final['totalPrice'].astype(str).str.replace('.',',')
    df_final['unitPrice']=df_final['unitPrice'].astype(str).str.replace('.',',')
    df_final['orderNumber']=df_final['orderNumber'].astype(str).str.replace('.',',')
    # Cambiar zona horaria (no aplicar por el momento)
    #df_final['createTime']=df_final['createTime']-dt.timedelta(hours=3)
    # Ordenar registros por fecha 
    df_final=df_final.sort_values(by='createTime')
    # Renombrar columnas
    df_final.rename({'orderNumber':'Order_Number','tradeType':'Order_Type','createTime':'Fecha','asset':'Tipo_Cripto','amount':'Cantidad_Cripto','totalPrice':'Monto_ARS','unitPrice':'Precio_unitario'},axis=1,inplace=True) # cambiar nombre segun DB mysql
    # Ordenar por fecha 
    return df_final


