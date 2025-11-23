import boto3
import uuid
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Configuración de Selenium en modo headless para Lambda
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    chrome_options.binary_location = "/opt/chrome/chrome"

    driver = webdriver.Chrome(
        executable_path="/opt/chromedriver",
        options=chrome_options
    )

    try:
        driver.get(url)
        time.sleep(3)  # Espera a que cargue contenido dinámico

        # Localizar filas de la tabla
        filas = driver.find_elements(By.CSS_SELECTOR, ".views-table tbody tr")

        if len(filas) == 0:
            return {
                "statusCode": 404,
                "body": "No se encontró la tabla en la página del IGP"
            }

        registros = []
        for fila in filas[:10]:  # Solo los 10 últimos
            columnas = fila.find_elements(By.TAG_NAME, "td")
            if len(columnas) < 6:
                continue

            registro = {
                "fecha": columnas[0].text,
                "hora": columnas[1].text,
                "magnitud": columnas[2].text,
                "profundidad": columnas[3].text,
                "referencia": columnas[4].text,
                "latitud_longitud": columnas[5].text,
                "id": str(uuid.uuid4())
            }
            registros.append(registro)

        # Guardar datos en DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table("TablaSismos")

        # Eliminar items previos
        scan = table.scan()
        with table.batch_writer() as batch:
            for item in scan.get('Items', []):
                batch.delete_item(Key={'id': item['id']})

        # Insertar nuevos
        with table.batch_writer() as batch:
            for r in registros:
                batch.put_item(Item=r)

        return {
            "statusCode": 200,
            "body": registros
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e)
        }

    finally:
        driver.quit()
