import requests
import time
from dotenv import load_dotenv
import os
from prefect import flow, task
import json 
from datetime import datetime
from typing import Any, Dict, List
from google.cloud import storage
 
load_dotenv()


GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_BUCKET_PATH_TEST = os.getenv("GCS_BUCKET_PATH_TEST")
GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_CREDENTIALS_PATH

@task(log_prints=True, name="Fetch Cornershop data", retries=3)
def get_product_data(query, postal, country, delay=1):
    """Fetches data from Cornershop API.

    Args:
        query (str): The query parameter to search for.
        postal (str): The postal code.
        country (str): The country code.

    Returns:
        dict: A dictionary representing the JSON response.
    """

    base_url = "https://cornershopapp.com/api/v2/branches/search"
    params = {
        "query": query,
        "locality": postal,
        "country": country,
    }

    response = requests.get(base_url, params=params)

    if response.status_code != 200:
        raise Exception(f"Request failed with status {response.status_code}")

    time.sleep(delay)  # delay for `delay` seconds
    return response.json()


##--- helper functions for create_data --- ##

def get_nested(data: Dict, *args: str) -> Any:
    """
    Safely navigates a nested dictionary.
    """
    for arg in args:
        if data is None:
            return None
        data = data.get(arg)
    return data

def create_row(data: Dict, aisle: Dict, product: Dict) -> Dict:
    """
    Create a dictionary for a single row in the dataframe.
    """
    return {
        'date': datetime.today().strftime("%d-%m-%Y"),
        'aisle_name': aisle.get('aisle_name', None),           
        'product_name': product.get('name', None),
        'brand': get_nested(product, 'brand', 'name'),                  
        'price': get_nested(product, 'pricing', 'price', 'amount'),  
        'package': product.get('package', None), 
        'store_name': get_nested(data, 'store', 'name'),    
        'store_city': get_nested(data, 'store', 'closest_branch', 'city'),
        'search_term': get_nested(data, 'search_result', 'search_term'),
    }

@task
def create_data(results: List[Dict]):
    """
    Create a ready dataframe format from the given results.
    """
    rows = [create_row(data, aisle, product) 
            for data in results 
            for aisle in get_nested(data, 'search_result', 'aisles') 
            for product in aisle.get('products', [])]
    return rows


@task
def send_to_bucket(data, bucket_name, blob_name):
    """
    Save the given data as a JSON blob in the specified GCS bucket.

    Parameters
    ----------
    data : any
        The data to be saved.
    bucket_name : str
        The name of the GCS bucket where the blob will be saved.
    blob_name : str
        The name of the blob.
    """    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data))
    

@flow
def crawler_to_gsc_test():
    #query = "doritos"
    postal = "88010560"
    country = "BR"

    lista_doces = ['haribo', 'fini']


    for item in lista_doces:
        raw_json_data = get_product_data(item, postal, country)
        json_data = raw_json_data.get("results", [])
        data = create_data(json_data)    
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"{GCS_BUCKET_PATH_TEST}/{item}_{postal}_{country}_{timestamp}.json"
    
        send_to_bucket(data, GCS_BUCKET_NAME, blob_name)
    
if __name__ == "__main__":
    crawler_to_gsc_test()

# prefect deployment build prefect/write_to_gcs_test.py:crawler_to_gsc_test -n val-flow-gcs -p val-pool
# prefect deployment apply crawler_to_gsc_test-deployment.yaml
#prefect agent start -q 'val-pool'