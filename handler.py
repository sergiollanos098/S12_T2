import json
import boto3
import requests
from bs4 import BeautifulSoup
import random
import time
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("SismosIGP")

URL = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-PE,es;q=0.9"
}

def scrape_igp():
    # delay pequeño random (anti-ban)
    time.sleep(random.uniform(0.8, 1.6))

    response = requests.get(URL, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")

    rows = soup.select("table tbody tr")[:10]  # solo top 10

    data = []
    for r in rows:
        cols = [c.text.strip() for c in r.find_all("td")]
        if len(cols) < 5:
            continue

        sismo = {
            "id": cols[0],  
            "fecha": cols[1],
            "hora": cols[2],
            "magnitud": cols[3],
            "referencia": cols[4],
            "insertedAt": datetime.utcnow().isoformat()
        }
        data.append(sismo)

    return data


def store_in_dynamo(items):
    for item in items:
        table.put_item(Item=item)


def main(event, context):
    sismos = scrape_igp()
    store_in_dynamo(sismos)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Scraping almacenado correctamente",
            "count": len(sismos),
            "items": sismos
        })
    }
