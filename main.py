# Librerias
import funciones_principales as ap
import tkinter
from tkinter import messagebox
import threading 

def main():
    ventana=tkinter.Tk()
    ventana.geometry("730x200")   
    ventana.title('Gestor de Datos Binance')


    fuente= ("Helvetica", 14)
    frame_superior=tkinter.Frame(ventana)
    frame_inferior=tkinter.Frame(ventana)

    frame_superior.grid(row=0,column=0,sticky="ns")
    frame_inferior.grid(row=1,column=0,sticky="ns")

    ventana.grid_rowconfigure(0, weight=1)
    ventana.grid_rowconfigure(1, weight=1)
    ventana.grid_columnconfigure(0, weight=1)

    label_fecha_inicial=tkinter.Label(frame_superior,text='Fecha Inicial',font=fuente)
    label_fecha_final=tkinter.Label(frame_superior,text='Fecha Final',font=fuente)
    label_fecha_inicial.grid(row=0,column=0)
    label_fecha_final.grid(row=0,column=1)

    fuente= ("Helvetica", 14)
    campo_fecha_inicial=tkinter.Entry(frame_superior,font=fuente)
    campo_fecha_final=tkinter.Entry(frame_superior,font=fuente)

    campo_fecha_inicial.grid(row=1, column=0, padx=10, pady=10, sticky="ew")
    campo_fecha_final.grid(row=1, column=1, padx=10, pady=10, sticky="ew")


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



    buton1=tkinter.Button(frame_inferior,text='Planilla Arbitrajes',command=iniciar_carga_datos_top7,width=20,height=2,font=fuente)
    #buton2=tkinter.Button(frame_inferior,text='Datos Arbitraje',command=datos_arbitraje,width=20,height=2,font=fuente)
    buton3=tkinter.Button(frame_inferior,text='Planilla Contador',command=iniciar_carga_datos_contador,width=20,height=2,font=fuente)

    buton1.grid(row=0,column=0,padx=5, pady=10, sticky="ew")
    #buton2.grid(row=0,column=1,padx=5, pady=10, sticky="ew")
    buton3.grid(row=0,column=2,padx=5, pady=10, sticky="ew")

    label_estado = tkinter.Label(frame_inferior)
    label_estado.grid(row=1,column=1)


    ventana.mainloop()

if __name__ == "__main__":
    main()