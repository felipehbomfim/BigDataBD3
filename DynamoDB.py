#!/usr/bin/env python3

import decimal
import json
import logging
import time
import boto3
import csv
import sys
import humanfriendly
from botocore.config import Config
from botocore.exceptions import NoCredentialsError

def myParseFloat(s):
    return decimal.Decimal(str(round(float(s), 2)))

def dynamoConnect():
    config = Config(connect_timeout=1, read_timeout=(15*60), retries={'max_attempts': 0})
    return boto3.resource(
        'dynamodb',
        endpoint_url='http://localhost:8000',
        config=config,
        region_name='us-west-2',
        aws_access_key_id='fakeMyKeyId',
        aws_secret_access_key='fakeSecretAccessKey'
    )

def criaTabelaCovidDynamo():
    db = dynamoConnect()
    table_name = 'covid_data'
    existing_tables = db.meta.client.list_tables()['TableNames']
    if table_name in existing_tables:
        db.Table(table_name).delete()
        logging.info(f"Tabela {table_name} excluída.")
        while table_name in db.meta.client.list_tables()['TableNames']:
            time.sleep(1)

    try:
        table = db.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'date', 'KeyType': 'HASH'},
                {'AttributeName': 'city', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'date', 'AttributeType': 'S'},
                {'AttributeName': 'city', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        logging.info(f"Tabela {table_name} criada com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao criar tabela DynamoDB: {e}")

def carregaTabelaCovidDynamo(csvfile):
    db = dynamoConnect()
    table = db.Table('covid_data')
    start_time = time.time()
    with open(csvfile, mode='r') as f:
        reader = csv.DictReader(f)
        with table.batch_writer() as batch:
            for row in reader:
                item = {
                    'date': row['date'],
                    'state': row['state'],
                    'city': row['city'],
                    'place_type': row['place_type'],
                    'confirmed': int(row['confirmed']) if row['confirmed'] else None,
                    'deaths': int(row['deaths']) if row['deaths'] else None,
                    'is_last': row['is_last'].lower() == 'true',
                    'estimated_population': int(row['estimated_population']) if row['estimated_population'] else None,
                    'city_ibge_code': int(row['city_ibge_code']) if row['city_ibge_code'] else None,
                    'confirmed_per_100k_inhabitants': myParseFloat(row['confirmed_per_100k_inhabitants']) if row['confirmed_per_100k_inhabitants'] else None,
                    'death_rate': myParseFloat(row['death_rate']) if row['death_rate'] else None
                }
                batch.put_item(Item=item)

    exec_time = time.time() - start_time
    logging.info(f"DynamoDB: --- {exec_time} seconds --- ({humanfriendly.format_timespan(exec_time)})")
    return exec_time

def execute_query_dynamo(table, filter_expression=None):
    start_time = time.time()
    if filter_expression:
        response = table.scan(FilterExpression=filter_expression)
    else:
        response = table.scan()
    docs = response['Items']
    duration = time.time() - start_time
    return {'docs': docs, 'duration': int(duration * 1000)}

def get_all_places_dynamo(table):
    return execute_query_dynamo(table)

def get_all_states_dynamo(table):
    start_time = time.time()
    response = table.scan(
        ProjectionExpression='#st',
        ExpressionAttributeNames={'#st': 'state'}
    )
    states = list({item['state'] for item in response['Items']})
    duration = time.time() - start_time
    return {'docs': states, 'duration': int(duration * 1000)}

def get_all_cities_dynamo(table, state):
    from boto3.dynamodb.conditions import Key
    return execute_query_dynamo(table, Key('state').eq(state))

def get_data_by_date_and_place_dynamo(table, date, place):
    from boto3.dynamodb.conditions import Key
    state, city = list(place.items())[0]
    return execute_query_dynamo(table, Key('date').eq(date) & Key('state').eq(state) & Key('city').eq(city))

def get_total_by_date_range_and_place_dynamo(table, start_date, end_date, place):
    from boto3.dynamodb.conditions import Key
    state, city = list(place.items())[0]
    start_time = time.time()
    response = table.scan(
        FilterExpression=Key('date').between(start_date, end_date) & Key('state').eq(state) & Key('city').eq(city),
        ProjectionExpression='confirmed, deaths'
    )
    total_confirmed = sum(item['confirmed'] for item in response['Items'])
    total_deaths = sum(item['deaths'] for item in response['Items'])
    duration = time.time() - start_time
    return {'docs': [{'total_confirmed': total_confirmed, 'total_deaths': total_deaths}], 'duration': int(duration * 1000)}

def get_average_by_date_range_and_place_dynamo(table, start_date, end_date, place):
    from boto3.dynamodb.conditions import Key
    state, city = list(place.items())[0]
    start_time = time.time()
    response = table.scan(
        FilterExpression=Key('date').between(start_date, end_date) & Key('state').eq(state) & Key('city').eq(city),
        ProjectionExpression='confirmed, deaths'
    )
    count = len(response['Items'])
    total_confirmed = sum(item['confirmed'] for item in response['Items'])
    total_deaths = sum(item['deaths'] for item in response['Items'])
    avg_confirmed = total_confirmed / count if count > 0 else 0
    avg_deaths = total_deaths / count if count > 0 else 0
    duration = time.time() - start_time
    return {'docs': [{'avg_confirmed': avg_confirmed, 'avg_deaths': avg_deaths}], 'duration': int(duration * 1000)}

def display_benchmark(description, query_fn, *args):
    DESCRIPTION_LENGTH = 60
    COUNT_LENGTH = 17
    DURATION_LENGTH = 8

    try:
        result = query_fn(*args)
        docs, duration = result['docs'], result['duration']

        logging.info('   %s %s %s' % (
            description.ljust(DESCRIPTION_LENGTH),
            f"{len(docs)} registro{'' if len(docs) == 1 else 's'}".rjust(COUNT_LENGTH),
            f"{duration}ms".rjust(DURATION_LENGTH)
        ))
    except Exception as error:
        logging.error(f"Erro ao executar benchmark {description}: {error}")

def run_benchmarks_dynamo():
    db = dynamoConnect()
    table = db.Table('covid_data')
    try:
        display_benchmark('DynamoDB: Obter número de casos confirmados em uma cidade específica em uma data específica', get_data_by_date_and_place_dynamo, table, '2020-06-01', {'PR': 'Curitiba'})
        display_benchmark('DynamoDB: Contar a quantidade total de casos confirmados em um estado específico', get_all_states_dynamo, table)
        display_benchmark('DynamoDB: Listar a quantidade de casos confirmados por cidade dentro de um estado', get_all_cities_dynamo, table, 'AC')
        display_benchmark('DynamoDB: Encontrar o número de mortes em uma cidade específica em uma data específica', get_data_by_date_and_place_dynamo, table, '2020-06-01', {'PR': 'Curitiba'})
        display_benchmark('DynamoDB: Contar o número total de mortes em um estado específico', get_total_by_date_range_and_place_dynamo, table, '2020-03-01', '2020-03-31', {'PR': 'Curitiba'})
        display_benchmark('DynamoDB: Obter todas as cidades que não registraram novos casos em uma data específica', get_average_by_date_range_and_place_dynamo, table, '2020-06-01', '2020-06-30', {'PR': 'Curitiba'})
    finally:
        pass

if __name__ == "__main__":
    logging.basicConfig(
        filename='LogDynamoDB.log',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

    if len(sys.argv) < 2:
        print("Uso: python DynamoDB.py <caminho_para_csv>")
        sys.exit(1)

    csvfile = sys.argv[1]

    logging.info("Criando tabela COVID-19 no DynamoDB")
    criaTabelaCovidDynamo()

    logging.info("Carregando dados COVID-19 no DynamoDB")
    exec_time = carregaTabelaCovidDynamo(csvfile)
    logging.info("DynamoDB: --- %s seconds --- (%s)\n\n" %
                 (exec_time, humanfriendly.format_timespan(exec_time)))

    logging.info("Executando benchmarks no DynamoDB")
    run_benchmarks_dynamo()
