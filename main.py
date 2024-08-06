# Librerias
import tkinter.ttk
import funciones_principales as ap
#import tkinter
from tkinter import messagebox
from tkinter import ttk
import threading 
import pandas as pd


import tkinter as tk
import random



def main():
    # Crear la ventana principal
    root = tk.Tk()
    root.title("Gestor de Datos Binance")
    root.geometry("1000x520")   

    # FRAME SUPERIOR -----------------------------------------------------------------
    fuente= ("Helvetica", 14)
    frame_superior=tk.Frame(root)
    frame_superior.grid(row=0,column=0,sticky="ns")
    root.grid_rowconfigure(0, weight=1)
    root.grid_rowconfigure(1, weight=1)
    root.grid_columnconfigure(0, weight=1)


    label_fecha_inicial=tk.Label(frame_superior,text='Fecha Inicial (dd/mm/aaaa)',font=fuente)
    label_fecha_final=tk.Label(frame_superior,text='Fecha Final (dd/mm/aaaa)',font=fuente)
    label_fecha_inicial.grid(row=0,column=0)
    label_fecha_final.grid(row=0,column=1)

    fuente= ("Helvetica", 14)
    campo_fecha_inicial=tk.Entry(frame_superior,font=fuente)
    campo_fecha_final=tk.Entry(frame_superior,font=fuente)

    campo_fecha_inicial.grid(row=1, column=0, padx=10, pady=10, sticky="ew")
    campo_fecha_final.grid(row=1, column=1, padx=10, pady=10, sticky="ew")

    # FRAME INFERIOR -------------------------------------------------------------------------
    frame_inferior=tk.Frame(root)
    frame_inferior.grid(row=1,column=0,sticky="ns")


    def mis_registros(): 
            label_estado.config(text="Cargando...", fg="blue",font=fuente)
            frame_inferior.update_idletasks()


            inicio=campo_fecha_inicial.get()
            fin=campo_fecha_final.get()
            ap.cargar_planilla_misregistros(inicio,fin)

            label_estado.config(text="Carga Exitosa", fg="green",font=fuente)
            messagebox.showinfo("Carga de Datos", "Los datos se han cargado exitosamente.")


    def registros_para_contador():
        label_estado.config(text="Cargando...", fg="blue",font=fuente)
        frame_inferior.update_idletasks()

        inicio=campo_fecha_inicial.get()
        fin=campo_fecha_final.get()
        ap.cargar_planilla_contador(inicio,fin)

        label_estado.config(text="Carga Exitosa", fg="green",font=fuente)
        messagebox.showinfo("Carga de Datos", "Los datos se han cargado exitosamente.")



    def iniciar_carga_datos_top7():
        # Iniciar el proceso de carga de datos en un hilo separado
        hilo = threading.Thread(target=mis_registros)
        hilo.start()

    def iniciar_carga_datos_contador():
        # Iniciar el proceso de carga de datos en un hilo separado
        hilo = threading.Thread(target=registros_para_contador)
        hilo.start()



    buton1=tk.Button(frame_inferior,text='Planilla Arbitrajes',command=iniciar_carga_datos_top7,width=20,height=2,font=fuente)
    #buton2=tkinter.Button(frame_inferior,text='Datos Arbitraje',command=datos_arbitraje,width=20,height=2,font=fuente)
    buton3=tk.Button(frame_inferior,text='Planilla Contador',command=iniciar_carga_datos_contador,width=20,height=2,font=fuente)

    buton1.grid(row=0,column=0,padx=5, pady=10, sticky="ew")
    #buton2.grid(row=0,column=1,padx=5, pady=10, sticky="ew")
    buton3.grid(row=0,column=2,padx=5, pady=10, sticky="ew")

    label_estado = tk.Label(frame_inferior)
    label_estado.grid(row=1,column=1)

    # FRAME PARA COTIZACIONES DE COMPRA--------------------------------------------
    bottom_frame = tk.Frame(root)
    bottom_frame.grid(row=2, column=0, columnspan=9, sticky='nsew')

    # Encabezados de columna
    headers = ['Cripto', 'Cot. P2P (ARS)', 'Cot. Spot (USDT)','Cot. USDT']
    for col, header in enumerate(headers):
        header_label = ttk.Label(bottom_frame, text=header, font=("Arial", 13, "bold"))
        header_label.grid(row=0, column=col, padx=5, pady=5)

    # Separador vertical
    separator = ttk.Separator(bottom_frame, orient='vertical')
    separator.grid(row=0, column=4, rowspan=7, sticky='ns', padx=10)


    for col, header in enumerate(headers):
        header_label = ttk.Label(bottom_frame, text=header, font=("Arial", 13, "bold"))
        header_label.grid(row=0, column=col+5, padx=5, pady=5)

    # Crear las listas para las entradas y los valores
    cripto=['BTC','ETH','BNB','ADA','XRP','DOGE','USDT']
    cripto1=['BTC','USDT']
   
    entries = []
    values = [0, 0, 0, 0, 0, 0]  # aca agregar los valores de la cotizacion de spot 
    sums = []
    value_labels = [] 

    entries1 = []
    values1 = [0,0]  # aca agregar los valores de la cotizacion de spot 
    sums1 = []
    value_labels1 = [] 


    valor_usdt0=[]
    coti_real0=[]


    valor_usdt=[]
    coti_real=[]



    # Función para actualizar los valores de la suma
    def update_cot_usdt(event, index):
        try:
            val1 = float(entries[index].get())
            val1=val1+(val1*0.0016)
            val2 = values[index]
            sums[index].config(text=str(round((val1 / val2),2)))

        except ValueError:
            sums[index].config(text="Error")


    def update_cot_usdt1(event, index):
        try:
            val1 = float(entries1[index].get())
            val1=val1-(val1*0.0016)
            val2 = values1[index]
            sums1[index].config(text=str(round((val1 / val2),2)))

        except ValueError:
            sums1[index].config(text="Error")





    def update_cot_usdt2(event, index):
        try:
            val1 = float(valor_usdt[index].get())
            val1=val1-(val1*0.0016)
            coti_real[index].config(text=str(val1))

        except ValueError:
            coti_real[index].config(text="Error")



    def update_cot_usdt0(event, index):
        try:
            val1 = float(valor_usdt0[index].get())
            val1=val1+(val1*0.0016)
            coti_real0[index].config(text=str(val1))

        except ValueError:
            coti_real0[index].config(text="Error")







    # Función para simular datos aleatorios y actualizar la columna de valores
    def fetch_data():
        nonlocal values, values1  
        try:

            df=ap.cotizaciones_spot()
            values=list(df.values)
            values1=list([list(df.values)[0]])

            # Actualiza las etiquetas de valores fijos en la interfaz
            for i, value in enumerate(values):
               
                value_labels[i].config(text=str(value))
                update_cot_usdt(None, i)  # Actualiza la suma al cambiar los valores
              
            for i, value in enumerate(values1):
                
                value_labels1[i].config(text=str(value))
                update_cot_usdt1(None, i) # Actualiza la suma al cambiar los valores
                
        except Exception as e:
            messagebox.showerror("Error", f"Error al obtener datos: {e}")
        
        # Programar la próxima actualización en 10 segundos
        root.after(7000, fetch_data)  # 10000 ms = 5 s






    for i in range(2):
        # Campo cripto
        cripto_label = ttk.Label(bottom_frame, text=str(cripto1[i]),font=("Arial", 10, "bold"))
        cripto_label.grid(row=i+1, column=0, padx=5, pady=5)
 

    # Entrada editable, campo Cot. P2P (ARS)
    entry = ttk.Entry(bottom_frame,font=("Arial", 10, "bold"))
    entry.grid(row=1, column=1, padx=5, pady=5)
    entry.bind('<KeyRelease>', lambda event, idx=0: update_cot_usdt1(event, idx))
    entries1.append(entry)


    entry = ttk.Entry(bottom_frame,font=("Arial", 10, "bold"))
    entry.grid(row=2, column=1, padx=5, pady=5)
    entry.bind('<KeyRelease>', lambda event, idx=0: update_cot_usdt2(event, idx))
    valor_usdt.append(entry)



    # entry_usdt = ttk.Entry(bottom_frame,font=("Arial", 10, "bold"))
    # entry_usdt.grid(row=1, column=1, padx=5, pady=5)
    # entry_usdt.bind('<KeyRelease>', lambda event, idx=0: update_cot_usdt2(event, idx))
    # entries1.append(entry_usdt)



    # Campo Cot. Spot (USDT)
    value_label = ttk.Label(bottom_frame, text=str(values1[0]),font=("Arial", 10, "bold"))
    value_label.grid(row=1, column=2, padx=5, pady=5)
    value_labels1.append(value_label)   

    # Campo Cot. USDT
    sum_label = ttk.Label(bottom_frame, text="0",font=("Arial", 10, "bold"))
    sum_label.grid(row=1, column=3, padx=5, pady=5)
    sums1.append(sum_label)

    sum_label_usdt = ttk.Label(bottom_frame, text="0",font=("Arial", 10, "bold"))
    sum_label_usdt.grid(row=2, column=3, padx=5, pady=5)
    coti_real.append(sum_label_usdt)



    # Crear las entradas, valores fijos y campos de suma en la parte inferior
    for i in range(6):
        # Campo cripto
        cripto_label = ttk.Label(bottom_frame, text=str(cripto[i]),font=("Arial", 10, "bold"))
        cripto_label.grid(row=i+1, column=5, padx=5, pady=5)
        

        # Entrada editable, campo Cot. P2P (ARS)
        entry = ttk.Entry(bottom_frame,font=("Arial", 10, "bold"))
        entry.grid(row=i+1, column=6, padx=5, pady=5)
        entry.bind('<KeyRelease>', lambda event, idx=i: update_cot_usdt(event, idx))
        entries.append(entry)

        # Campo Cot. Spot (USDT)
        value_label = ttk.Label(bottom_frame, text=str(values[i]),font=("Arial", 10, "bold"))
        value_label.grid(row=i+1, column=7, padx=5, pady=5)
        value_labels.append(value_label)   

        # Campo Cot. USDT
        sum_label = ttk.Label(bottom_frame, text="0",font=("Arial", 10, "bold"))
        sum_label.grid(row=i+1, column=8, padx=5, pady=5)
        sums.append(sum_label)



    # Campo cripto
    cripto_label = ttk.Label(bottom_frame, text=str(cripto[6]),font=("Arial", 10, "bold"))
    cripto_label.grid(row=8, column=5, padx=5, pady=5)

    # Entrada editable, campo Cot. P2P (ARS)
    entry = ttk.Entry(bottom_frame,font=("Arial", 10, "bold"))
    entry.grid(row=8, column=6, padx=5, pady=5)
    entry.bind('<KeyRelease>', lambda event, idx=0 : update_cot_usdt0(event, idx))
    valor_usdt0.append(entry)

    # Campo Cot. USDT
    sum_label = ttk.Label(bottom_frame, text="0",font=("Arial", 10, "bold"))
    sum_label.grid(row=8, column=8, padx=5, pady=5)
    coti_real0.append(sum_label)
    

    # Ajustar la expansión de las columnas y filas en el bottom_frame
    bottom_frame.grid_columnconfigure(0, weight=1)
    bottom_frame.grid_columnconfigure(2, weight=1)
    bottom_frame.grid_columnconfigure(3, weight=1)
    bottom_frame.grid_columnconfigure(5, weight=1)

    fetch_data()

    root.mainloop()

if __name__ == "__main__":
    main()