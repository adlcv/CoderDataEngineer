import psycopg2
from dotenv import load_dotenv
import os
from funciones_auxiliares import leer_ciudades, obtener_datos_clima, insertar_en_redshift
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar variables de entorno desde el archivo .env
load_dotenv('credenciales.env')

CLAVE_API = os.getenv("CLAVE_API")

# Configuración de la conexión a Redshift
configuracion_redshift = {
    'host': os.getenv('REDSHIFT_HOST'),
    'port': int(os.getenv('REDSHIFT_PORT')),
    'dbname': os.getenv('REDSHIFT_DBNAME'),
    'user': os.getenv('REDSHIFT_USER'),
    'password': os.getenv('REDSHIFT_PASSWORD')
}

def main():
    # Leer ciudades desde el archivo txt
    ciudades = leer_ciudades('50ciudades.txt')

    # Conectar Redshift
    try:
        conexion = psycopg2.connect(**configuracion_redshift)
        logging.info("Conexión a Redshift exitosa!")

        for nombre_ciudad, (lat, lon) in ciudades:
            datos_clima = obtener_datos_clima(nombre_ciudad, lat, lon, CLAVE_API)
            if datos_clima:
                insertar_en_redshift(conexion, datos_clima)
                logging.info(f"Datos del clima para {nombre_ciudad} insertados en Redshift")
            else:
                logging.warning(f"No se pudieron obtener datos para {nombre_ciudad}")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if conexion:
            conexion.close()
            logging.info("Conexión a Redshift cerrada.")

    logging.info("Proceso ETL completado.")

if __name__ == "__main__":
    main()
