import requests as request
from datetime import date
import boto3
import json

def lambda_handler(event, context):
    #cities = get_cities()
    cities = [{'name':'maldonado', 'id':'TUxVUE1BTFo5OWMx'}, {'name':'montevideo', 'id':'TUxVUE1PTlo2MDIy'}, {'name':'canelones', 'id':'TUxVUENBTnMxNzliYw'}, {'name':'rocha', 'id':'TUxVUFJPQ1ozNWRm'}]
    for city in cities:
        print('city ' + city.get('name'))
        neighborhoods = get_city_neighborhoods(city.get('id'))
        for neighborhood in neighborhoods:
            print('neighborhood ' + neighborhood.get('name'))
            get_houses(neighborhood.get('id'))
    
    return {
        'statusCode': 200,
        'body': json.dumps('JOB done!')
    }

def get_cities(country = 'UY'):
    url = 'https://api.mercadolibre.com/classified_locations/countries/'+country
    response = request.get(url)
    if(response.ok):
        return response.json()['states']
    else:
        print('Error on get city neighborhood')

def get_city_neighborhoods(city='TUxVUE1PTlo2MDIy'):
    url = 'https://api.mercadolibre.com/classified_locations/states/'+city
    response = request.get(url)
    if(response.ok):
        return response.json()['cities']
    else:
        print('Error on get city neighborhood')

def get_houses(neighborhood = 'TUxVQ0NPUjZmZjNm', category = 'MLU1459'):
    OFFSET_INCREMENT = 50
    houses = []
    offset = 0
    total_paging = 99999
    raw_url = 'https://api.mercadolibre.com/sites/MLU/search?category={}&city={}&limit={}&offset={}&since=today'

    while (offset < total_paging and offset <= 10000):
        url = str.format(raw_url, category, neighborhood, OFFSET_INCREMENT, offset)
        paging = internal_get_houses(url, offset, houses, neighborhood)
        total_paging = paging['total']
        offset = offset + OFFSET_INCREMENT
        print(str.format('total items {} - offset {}', total_paging, offset))

def internal_get_houses(url, offset, houses, neighborhood):
    search_response = request.get(url)
    if(not search_response.ok):
        print(str.format('Error on get city neighborhood, offset {}', offset))
        return None
    json_response = search_response.json()
    s3_search_path = str.format('mercadolibre/search/{}-{}-{}.json', date.today(), neighborhood, offset)
    save_s3(s3_search_path, search_response.text)
    
    for item in json_response['results']:
        item_id = item.get('id')
        s3_item_path = str.format('mercadolibre/items/{}/{}.json', date.today(), item_id)
        save_s3(s3_item_path, get_item(item_id))
        
    return json_response['paging']
        
def get_item(item_id):
    url = str.format('https://api.mercadolibre.com/items/{}', item_id)
    response = request.get(url)
    if(not response.ok):
        print(str.format('Error on get item info, item id {}', item_id))
        return None
    return response.text
    
def save_s3(path, content):
    bucket_name = "realstate-storage"
    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=path, Body=content)
