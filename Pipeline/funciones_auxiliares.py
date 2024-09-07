import requests
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def leer_ciudades(nombre_archivo):
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
    actual = datos.get('current', {})
    return {
        'temperatura': actual.get('temp'),
        'sensacion_termica': actual.get('feels_like'),
        'humedad': actual.get('humidity'),
        'velocidad_viento': actual.get('wind_speed'),
        'tiempo_medicion': datetime.fromtimestamp(actual.get('dt', 0))
    }

def obtener_datos_clima(nombre_ciudad, lat, lon, clave_api):
    url_base = "https://api.openweathermap.org/data/3.0/onecall"
    parametros = {
        "lat": lat,
        "lon": lon,
        "exclude": "minutely,hourly,daily,alerts",
        "appid": clave_api,
        "units": "metric"
    }
    
    respuesta = requests.get(url_base, params=parametros)
    
    if respuesta.status_code == 200:
        datos = respuesta.json()
        datos_filtrados = filtrar_datos_clima(datos)
        datos_filtrados['nombre_ciudad'] = nombre_ciudad
        datos_filtrados['fecha_carga'] = datetime.now()
        return datos_filtrados
    else:
        logging.error(f"Error al obtener datos para {nombre_ciudad}. Código de estado: {respuesta.status_code}")
        return None

def insertar_en_redshift(conexion, datos):
    with conexion.cursor() as cur:
        sql = """
        INSERT INTO datos_clima 
        (nombre_ciudad, temperatura, sensacion_termica, humedad, velocidad_viento, tiempo_medicion, fecha_carga)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (
            datos['nombre_ciudad'],
            datos['temperatura'],
            datos['sensacion_termica'],
            datos['humedad'],
            datos['velocidad_viento'],
            datos['tiempo_medicion'],
            datos['fecha_carga']
        ))
    conexion.commit()