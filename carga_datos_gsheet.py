# Librerias
import pygsheets


def clear_sheet(hoja,celda_inicial): # Ex funcion limpiar_sheet_top7 / ex funcion limpiar_sheet
    "Limpia los datos de la hoja pasada como parametro a partir de la celda pasada como parametro celda_incial "
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    spread_sheet=gc.open('Planilla Arbitraje')
    worksheet=worksheet=spread_sheet.worksheet_by_title(hoja)
    worksheet.clear(start=celda_inicial)#,end='N13')

 
def load_to_sheet(hoja,celda,datos_procesados): # Ex funcion cargar_top7_ventas / ex funcion cargar_datos_myplanilla
    "Carga  las   ventas realizadas en una fecha determinada, la carga se hace en el documento 'Planilla Arbitraje'"
    # Establecer conexion con google sheets
    credenciales='credenciales_drive.json'
    gc=pygsheets.authorize(service_account_file=credenciales)
    # Abre la hoja de calculo
    spread_sheet=gc.open('Planilla Arbitraje')
    worksheet=spread_sheet.worksheet_by_title(hoja)
    worksheet.set_dataframe(datos_procesados,start=celda,copy_index=False,copy_head=False)

