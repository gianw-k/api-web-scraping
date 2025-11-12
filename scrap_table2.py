from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler2(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    
    # Configurar Selenium con Chrome headless
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=options)
    
    try:
        driver.get(url)
        
        # Esperar a que la tabla cargue (máximo 10 segundos)
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "tr"))
        )
        
        # Obtener el HTML después de que JavaScript haya cargado
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        
        if not table:
            return {
                'statusCode': 404,
                'body': 'No se encontró la tabla'
            }
        
        headers = [header.text.strip() for header in table.find_all('th')]
        
        rows = []
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if cells:
                row_data = {headers[i]: cells[i].text.strip() for i in range(len(cells))}
                rows.append(row_data)
        
        # Guardar los datos en DynamoDB
        dynamodb = boto3.resource('dynamodb')
        dynamo_table = dynamodb.Table('TablaWebScrapping2')
        
        # Eliminar todos los elementos de la tabla antes de agregar los nuevos
        scan = dynamo_table.scan()
        with dynamo_table.batch_writer() as batch:
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
            dynamo_table.put_item(Item=row)
            i = i + 1
        
        return {
            'statusCode': 200,
            'body': rows
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }
    
    finally:
        driver.quit()