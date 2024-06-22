# Librerias
import pygsheets


def limpiar_sheet(hoja,celda_inicial): # Ex funcion limpiar_sheet_top7
    "Limpia los datos de la hoja pasada como parametro a partir de la celda pasada como parametro celda_incial "
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    worksheet=gc.open('Planilla Arbitraje')[hoja]
    worksheet.clear(start=celda_inicial)#,end='N13')

 
def cargar_datos_miplanilla(hoja,celda,datos_procesados): # Ex funcion cargar_top7_ventas
    "Carga  las   ventas realizadas en una fecha determinada, la carga se hace en el documento 'Planilla Arbitraje'"
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    worksheet=gc.open('Planilla Arbitraje')[hoja]
    worksheet.set_dataframe(datos_procesados,start=celda,copy_index=False,copy_head=False)



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
