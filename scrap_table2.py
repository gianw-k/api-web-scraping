from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web que contiene la tabla
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # run Chromium headless
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")     # wait for network activity to finish
        html = page.content()                        # get full rendered HTML
        browser.close()

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(html, 'html.parser')

    # Encontrar la tabla en el HTML
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Extraer los encabezados de la tabla
    headers = [header.text for header in table.find_all('th')]

    # Extraer las filas de la tabla
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        rows.append({headers[i+1]: cell.text for i, cell in enumerate(cells)})

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping2')

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    # Insertar los nuevos datos
    i = 1
    for row in rows:
        row['#'] = i
        row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
        table.put_item(Item=row)
        i = i + 1

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': rows
    }
