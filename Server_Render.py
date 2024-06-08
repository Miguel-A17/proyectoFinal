import socket
import threading
import os
import cv2
import json

numDivisiones = 8 #Nuestro numero de Batch's o partes

def obtener_archivos_de_imagen(carpeta_de_imagenes):
    #Obtiene una lista de archivos de imagen en una carpeta.
    imagenes = [img for img in os.listdir(carpeta_de_imagenes) if img.endswith(".jpg")]
    if not imagenes:
        print("No se encontraron imágenes en el directorio.")
        return []
    return imagenes

#Puse afuera estas variables para hacer el programa mas facilmente manejable
carpeta_de_imagenes_global = r'H:\Mi unidad\Tarea5SD\ImagenesVideo' #Ruta del Google Drive donde estarian las imagenes
imagenes_global = obtener_archivos_de_imagen(carpeta_de_imagenes_global)
longitudPorDivision_global = int(len(imagenes_global)/numDivisiones) #Vamos a necesitar usar el resultado de la division para ir iterando entre los Batch
residuoDivision_global = len(imagenes_global)%numDivisiones #Se necesita el residuo para captar las imagenes que esten fuera del rango del ultimo batch
conjuntos_de_imagenes_global = {str(i): {'Estado': 'A', 'Imagenes': [os.path.join(carpeta_de_imagenes_global, img) for img in imagenes_global[i:i+longitudPorDivision_global]]} for i in range(numDivisiones)} #Creamos el diccionario de los diferentes batch con sus estados

def renderizar_video(carpeta_de_salida): #Para la parte final se creo esta funcion que junta las partes del video
    global conjuntos_de_imagenes_global #Se declara esto por sino lo capta la funcion
    nombre_video = os.path.join(carpeta_de_salida, 'Video_Completo.mp4') #Juntamos el nombre del video completo con su ruta

    #print(f"La ruta del video completo es: '{nombre_video}'") #Debug
    #print(f"La primer imagen es: {conjuntos_de_imagenes_global['0']['Imagenes'][0]}") #Debug

    #Necesitamos la primer imagen para sacar sus dimensiones
    primera_imagen_path = conjuntos_de_imagenes_global['0']['Imagenes'][0] 
    frame = cv2.imread(primera_imagen_path)
    if frame is None:
        print(f"Error al leer la primera imagen: {primera_imagen_path}")
        return False #Para indicar que no se termino la funcion
    altura, ancho, capas = frame.shape

    # Inicializa el objeto de escritura del video completo
    video = cv2.VideoWriter(nombre_video, cv2.VideoWriter_fourcc(*'mp4v'), 16, (ancho, altura))    
    if not video.isOpened():
        print(f"Error al crear el archivo de video: {nombre_video}")
        return False #Para indicar que no se termino la funcion
    print("Se va a empezar a juntar las partes del video en uno solo")

    # Aqui se empiezan a combinar todas las partes del video en uno solo
    for i in range(numDivisiones):
        parte_video_nombre = os.path.join(carpeta_de_salida, f'video_{i}.mp4') #Captamos el nombre de la parte del video        
        cap = cv2.VideoCapture(parte_video_nombre) #Se abre esa parte del video
        #Aqui empezamos a escribir los frames de la parte del video en el video completo
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            video.write(frame)
        
        cap.release() #Se tiene que liberar cada parte del video una vez se termine de usar
    # Se libera el video y como nos puso el profe, se cierran las ventanas
    video.release()
    cv2.destroyAllWindows()
    print("Se termino de renderizar el video completo")

    # Aqui se eliminan las partes del video en la carpeta donde esten
    for i in range(numDivisiones):
        parte_video_nombre = os.path.join(carpeta_de_salida, f'video_{i}.mp4')
        try:
            os.remove(parte_video_nombre)
        except PermissionError as e:
            print(f"Error borrando el video '{parte_video_nombre}': {e}")
            return False #Para indicar que no se termino la funcion
    print("Se removieron los videos temporales")
    print(f"Video renderizado: {nombre_video}")
    return True #Para indicar que termino bien la funcion
 
def manejar_cliente(conjuntos_de_imagenes, carpeta_de_salida, conn, addr, conexiones_activas, finalizacion_evento): #Embajador con intento de un evento para terminar el servidor una vez se haya renderizado el video
#def manejar_cliente(conjuntos_de_imagenes, carpeta_de_salida, conn, addr, conexiones_activas): #Embajador sin un evento para terminar el servidor cuando se renderice el video completo
    print(f"Nueva conexión: {addr}")
    while True: #Circuit Breaker
        conjunto_disponible = s0(conjuntos_de_imagenes, conn, addr)
        if conjunto_disponible is not None:
            token = s1(conjuntos_de_imagenes, conn, addr, conjunto_disponible)
            if token is not None:
                if not s2(conjuntos_de_imagenes, carpeta_de_salida, conn, addr, conjunto_disponible):
                    print("Ocurrio un Error al momento de recibir la confirmacion de si el cliente termino")
                    break
        else: #Una vez que ya no haya Estados 'A' entonces se cierra la conexion
            print(f"Se va a cerrar la conexion para '{addr}'")
            #Debido a que el cliente tiene que decodificar un JSON, tube que convertir el mensaje de si hay o no cargas en un JSON tambien
            datos = {
                "mensaje": "No hay cargas disponibles"
            }
            mensaje_json = json.dumps(datos)
            conn.send(mensaje_json.encode('utf-8')) #Enviamos al cliente el mensaje de que ya no hay cargas
            break
    print("Se salio del Circuit Breaker")
    conn.close()
    conexiones_activas.remove(conn)
    print("Ahora se revisara si no hay conexiones ni conjuntos disponibles")
    #Puramente DEBUG
    #print(f"Las conexiones activas son: '{conexiones_activas}'.")
    #print(f"Las conexiones activas son: '{conexiones_activas}', y los valores dentro del diccionario son: '{conjunto_disponible.values()}'")
    #print(f"Mientras que las llaves dentro del diccionario 'conjuntos_de_imagenes' son: '{conjuntos_de_imagenes.keys()}'")
    #print(f"Mientras que las llaves dentro del diccionario son: '{conjuntos_de_imagenes.keys()}'")
    #print(f"Mientras que las llaves dentro del diccionario[0] son: '{conjuntos_de_imagenes['0'].keys()}'")
    #print(f"Mientras que los valores dentro del diccionario[0] son: '{conjuntos_de_imagenes['0'].values()}'")
    #print(f"Mientras que el valor dentro del diccionario[0]['Estado'] es: '{conjuntos_de_imagenes['0']['Estado']}'")
    #print(f"Mientras que los valores dentro del diccionario 'conjunto_disponible' son: '{conjunto_disponible}'")

    #if len(conexiones_activas) == 0 and all(conjunto['Estado'] == 'C' for conjunto in conjuntos_de_imagenes.values()): #Una forma de checar si ya quedaron todos los Batch completos
    if len(conexiones_activas) == 0 and conjunto_disponible == None: #Otra forma ya que realmente se podría checar en el S0
        print("Se va a meter a renderizar el video completo")
        renderizado_completo = renderizar_video(carpeta_de_salida)
        if renderizado_completo == True:
            print("Se metio al if de renderizado_completo")
            finalizacion_evento.set()

def s0(conjuntos_de_imagenes, conn, addr):
    print(f"S0: Buscando nodos disponibles para {addr}")
    for id_conjunto, info_conjunto in conjuntos_de_imagenes.items():
        if info_conjunto['Estado'] == 'A':
            return id_conjunto
    print("No hay nodos disponibles.")
    return None

def s1(conjuntos_de_imagenes, conn, addr, id_conjunto):
    print(f"S1: Nodo disponible encontrado para {addr}. El ID del conjunto es: {id_conjunto}")
    conjuntos_de_imagenes[id_conjunto]['Estado'] = 'B'
    return f"token_{id_conjunto}"

def s2(conjuntos_de_imagenes, carpeta_de_salida, conn, addr, id_conjunto): 
    print(f"S2: Generando renderización para {addr}. ID de conjunto: {id_conjunto}")
    #Esta parte no se utiliza realmente salvo para indicar al servidor donde estara localizado la parte del video durante la ejecucion
    rutas_de_imagenes = conjuntos_de_imagenes[id_conjunto]['Imagenes']
    nombre_video = os.path.join(carpeta_de_salida, f"video_{id_conjunto}.mp4")

    #Aqui se definen los rangos de los Batch o Cargas
    if int(id_conjunto)==0:
        print("Esta es la 1ra Carga")
        inicio_rango = 0
        final_rango = inicio_rango + len(rutas_de_imagenes)
        print(f"Las cargas van de la imagen '{inicio_rango}' hasta la imagen '{final_rango}'")
    elif int(id_conjunto)==numDivisiones-1:
        print("Esta es la ultima carga")
        inicio_rango = int(id_conjunto) * longitudPorDivision_global + 1
        final_rango = (inicio_rango) + len(rutas_de_imagenes) + residuoDivision_global #Agregamos el residuo por si quedaron imagenes sueltas del batch
        print(f"Las cargas van de la imagen '{inicio_rango}' hasta la imagen '{final_rango}'")
    else:
        inicio_rango = int(id_conjunto) * longitudPorDivision_global + 1
        final_rango = (inicio_rango-1) + len(rutas_de_imagenes)
        print(f"Las cargas van de la imagen '{inicio_rango}' hasta la imagen '{final_rango}'")
    
    # Crear un diccionario con todos los datos para enviarlos por JSON
    datos = {
        "mensaje": "Hay cargas disponibles",
        "id_conjunto": id_conjunto,
        "inicio_rango": inicio_rango,
        "final_rango": final_rango
    }
    # Convertir el diccionario a JSON
    mensaje_json = json.dumps(datos)
    # Enviar el JSON al cliente
    conn.send(mensaje_json.encode('utf-8'))
    print(f"Se enviaron los datos al cliente: '{datos}'")

    conn.settimeout(600)  # Establece un timeout de 600 segundos (10 minutos)
    try:
        termino_el_cliente = bool(conn.recv(1024).decode('utf-8'))
        if termino_el_cliente:
            conjuntos_de_imagenes[id_conjunto]['Estado'] = 'C'
            print(f"Video recibido y guardado como: {nombre_video}")
            return True
    except socket.timeout:
        print("Se acabaron los 10 minutos, no se recibió confirmación del cliente.")    
    print(f"El video no fue recibido")
    conjuntos_de_imagenes[id_conjunto]['Estado'] = 'A'
    datos = {
        "mensaje": "Error"
    }
    mensaje_json = json.dumps(datos)
    conn.send(mensaje_json.encode('utf-8'))  # Enviamos al cliente el mensaje de que ocurrió un error
    return False

def iniciar_servidor(carpeta_de_imagenes, carpeta_de_salida, host='localhost', puerto=5555):
    #conjuntos_de_imagenes = preparar_conjuntos_de_imagenes(carpeta_de_imagenes)
    conexiones_activas = set() #Para saber si todavia hay conexiones activas
    global conjuntos_de_imagenes_global #Por si no lo capta de la variable global
    conjuntos_de_imagenes = conjuntos_de_imagenes_global #Como creo que vamos a usarla de diferentes formas lo capto de uno original
    finalizacion_evento = threading.Event() #Esto se supone que es para poder terminar el servidor una vez finalizado el video completo
    #print(f"Estas son las llaves del diccionario: '{conjuntos_de_imagenes.keys()}'")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, puerto))
        s.listen()
        print(f"Servidor escuchando en {host}:{puerto}")

        #while True: #Forma predeterminada de manejar cada cliente
        while not finalizacion_evento.is_set(): #Forma de manejar a los clientes por medio de un evento para en teoria finalizarlo despues de renderizar el video completo
            conn, addr = s.accept()
            conexiones_activas.add(conn)
            #threading.Thread(target=manejar_cliente, args=(conjuntos_de_imagenes, carpeta_de_salida, conn, addr, conexiones_activas)).start() #Sin el evento para finalizar el servidor
            threading.Thread(target=manejar_cliente, args=(conjuntos_de_imagenes, carpeta_de_salida, conn, addr, conexiones_activas, finalizacion_evento)).start() #Con el evento para finalizar el servidor
        print("El servidor ha terminado correctamente.") #Se deberia imprimir cuando finalice el servidor

if __name__ == "__main__":
    #carpeta_de_imagenes = r'C:\Users\[UsuarioWindows]\Documents\CAM_FRONT' #Ruta local donde estarian las imagenes
    #carpeta_de_salida = r'C:\Users\[UsuarioWindows]\Documents\Video Renderizado' #Ruta local donde se almacenaria el video y sus partes temporales
    carpeta_de_imagenes = carpeta_de_imagenes_global #Ruta del Google Drive donde estarian las imagenes (Aunque por congruencia lo iguale a la ruta global)
    carpeta_de_salida = r'H:\Mi unidad\Tarea5SD\Video' #Ruta del Google Drive donde se almacenaria el video y sus partes temporales
    iniciar_servidor(carpeta_de_imagenes, carpeta_de_salida)