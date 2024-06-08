import socket
import threading
import time as t
import json
import cv2
import os

#image_folder = r'C:\Users\[NombreUsuarioWindows]\Documents\CAM_FRONT' #Ruta donde estarían las imagenes localmente
#temporal_output_folder = r'C:\Users\[NombreUsuarioWindows]\Documents\Video Renderizado' #Ruta de las partes temporales del video localmente
image_folder = r'H:\Mi unidad\Tarea5SD\ImagenesVideo' #Ruta donde estarían las imagenes en Carpeta Compartida de Google Drive
temporal_output_folder = r'H:\Mi unidad\Tarea5SD\Video' #Ruta de las partes temporales del video en Carpeta Compartida de Google Drive

def renderizar_parte_video():
    global image_folder,temporal_output_folder #Por si Python no agarra las variables de las rutas
    while True:
        bandera_terminado = False #Bandera para enviarle al servidor si se termino de renderizar el video
        # Recibir el mensaje JSON del servidor
        mensaje_json = server_socket.recv(1024).decode('utf-8')
        if not mensaje_json:
            print("No se recibio el mensaje en formato JSON del servidor")
            break
        
        # Convertir el mensaje JSON a un diccionario
        datos = json.loads(mensaje_json)
        
        if datos["mensaje"] == "No hay cargas disponibles":
            print("No hay más partes del video por renderizar. Se va a cerrar el nodo.")
            break
        elif datos["mensaje"] == "Error":
            print("Ocurrio un error en el servidor.")
            break
        print("Hay cargas disponibles, se procedera a recibir el numero de la parte del video y el rango de imagenes.")
        #Usamos mejor variables en lugar del diccionario para hacerlo mas claro en el resto del codigo
        numero_parte = datos["id_conjunto"]
        inicio_rango = datos["inicio_rango"]
        final_rango = datos["final_rango"]
        print(f"Tu parte del video es la '{numero_parte}'")
        # Seleccionar las imágenes correspondientes
        imagenes = [img for img in os.listdir(image_folder) if img.endswith(".jpg")][inicio_rango:final_rango]

        # Leer la primera imagen para obtener sus dimensiones
        primera_imagen = cv2.imread(os.path.join(image_folder, imagenes[0]))
        altura, ancho, _ = primera_imagen.shape

        # Crear el video temporal
        parte_video_nombre = os.path.join(temporal_output_folder, f'video_{numero_parte}.mp4')
        video_writer = cv2.VideoWriter(parte_video_nombre, cv2.VideoWriter_fourcc(*'mp4v'), 16, (ancho, altura))
        print("Se va a empezar a renderizar tu parte del video")
        #Se renderiza el video temporal
        for imagen in imagenes:
            frame = cv2.imread(os.path.join(image_folder, imagen))
            video_writer.write(frame)
        video_writer.release() #Liberamos al video temporal para usarlo en el servidor
        print(f"Parte {numero_parte} del video renderizada")
        bandera_terminado = True #Como ya terminamos entonces cambiamos el valor de la bandera que enviaremos al servidor
        #print(f"Se va a enviar esto al servidor: '{bandera_terminado}'") #DEBUG
        server_socket.send(str(bandera_terminado).encode('utf-8'))
        print(f"Tu parte del video fue enviada al servidor.")

    server_socket.close()


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.connect(('127.0.0.1', 5555))

nodo_renderizado = threading.Thread(target=renderizar_parte_video)
nodo_renderizado.start()