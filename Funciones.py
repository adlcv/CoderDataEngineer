import requests
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv('credenciales.env')

CLAVE_API = os.getenv("CLAVE_API")
URL_BASE = "https://api.openweathermap.org/data/3.0/onecall"

# Configuración de la conexión a Redshift
configuracion_redshift = {
    'host': os.getenv('REDSHIFT_HOST'),
    'port': int(os.getenv('REDSHIFT_PORT')),
    'dbname': os.getenv('REDSHIFT_DBNAME'),
    'user': os.getenv('REDSHIFT_USER'),
    'password': os.getenv('REDSHIFT_PASSWORD')
}

def leer_ciudades(nombre_archivo):
    """Lee las ciudades y sus coordenadas desde un archivo txt."""
    ciudades = []
    with open(nombre_archivo, 'r', encoding='utf-8') as archivo:
        for linea in archivo:
            partes = linea.strip().split(',')
            if len(partes) == 3:
                nombre = partes[0].strip()
                lat = float(partes[1].strip())
                lon = float(partes[2].strip())
                ciudades.append((nombre, (lat, lon)))
    return ciudades

def filtrar_datos_clima(datos):
    """Filtra y formatea los datos del clima."""
    actual = datos.get('current', {})
    return {
        'temperatura': actual.get('temp'),
        'sensacion_termica': actual.get('feels_like'),
        'humedad': actual.get('humidity'),
        'velocidad_viento': actual.get('wind_speed')
    }

def obtener_datos_clima(nombre_ciudad, lat, lon):
    parametros = {
        "lat": lat,
        "lon": lon,
        "exclude": "minutely,hourly,daily,alerts",
        "appid": CLAVE_API,
        "units": "metric"  # Para obtener temperaturas en Celsius
    }
    
    respuesta = requests.get(URL_BASE, params=parametros)
    
    if respuesta.status_code == 200:
        datos = respuesta.json()
        datos_filtrados = filtrar_datos_clima(datos)
        datos_filtrados['nombre_ciudad'] = nombre_ciudad
        return datos_filtrados
    else:
        print(f"Error al obtener datos para {nombre_ciudad}. Código de estado: {respuesta.status_code}")
        return None

def insertar_en_redshift(conexion, datos):
    """Inserta los datos en Redshift."""
    with conexion.cursor() as cur:
        sql = """
        INSERT INTO datos_clima 
        (nombre_ciudad, temperatura, sensacion_termica, humedad, velocidad_viento)
        VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(sql, (
            datos['nombre_ciudad'],
            datos['temperatura'],
            datos['sensacion_termica'],
            datos['humedad'],
            datos['velocidad_viento']
        ))
    conexion.commit()

# Leer ciudades desde el archivo txt
ciudades = leer_ciudades('50ciudades.txt')

# Conectar Redshift
try:
    conexion = psycopg2.connect(**configuracion_redshift)
    print("Conexión a Redshift exitosa!")

    for nombre_ciudad, (lat, lon) in ciudades:
        datos_clima = obtener_datos_clima(nombre_ciudad, lat, lon)
        if datos_clima:
            insertar_en_redshift(conexion, datos_clima)
            print(f"Datos del clima para {nombre_ciudad} insertados en Redshift")

except Exception as e:
    print(f"Error: {e}")
finally:
    if conexion:
        conexion.close()
        print("Conexión a Redshift cerrada.")

print("Todos los datos del clima han sido obtenidos y guardados en Redshift.")
