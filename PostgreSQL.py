#!/usr/bin/env python3

import decimal
import logging
import time
import humanfriendly
import psycopg2
import csv
import sys

def myParseFloat(s):
    return decimal.Decimal(str(round(float(s), 2)))

def psqlConnect():
    return psycopg2.connect(dbname="bigdata", user="bigdata", password="bigdata", host="localhost", port=5432)

def criaTabelaCovidPSQL():
    db = psqlConnect()
    cur = db.cursor()
    create_table = (""" CREATE TABLE IF NOT EXISTS covid_data (
                         date varchar,
                         state varchar,
                         city varchar,
                         place_type varchar,
                         confirmed int,
                         deaths int,
                         is_last boolean,
                         estimated_population int,
                         city_ibge_code int,
                         confirmed_per_100k_inhabitants numeric,
                         death_rate numeric
                       )
                    """)
    try:
        cur.execute(create_table)
        db.commit()
    except Exception as e:
        logging.error(f"Erro ao criar tabela PostgreSQL: {e}")
    finally:
        cur.close()
        if db is not None:
            db.close()

def carregaTabelaCovidPSQL(csvfile):
    db = psqlConnect()
    cur = db.cursor()
    start_time = time.time()
    with open(csvfile, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            query_sql = """ INSERT INTO covid_data (date, state, city, place_type, confirmed, deaths, is_last, estimated_population, city_ibge_code, confirmed_per_100k_inhabitants, death_rate)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            try:
                cur.execute(query_sql, (
                    row['date'],
                    row['state'],
                    row['city'],
                    row['place_type'],
                    int(row['confirmed']) if row['confirmed'] else None,
                    int(row['deaths']) if row['deaths'] else None,
                    row['is_last'].lower() == 'true',
                    int(row['estimated_population']) if row['estimated_population'] else None,
                    int(row['city_ibge_code']) if row['city_ibge_code'] else None,
                    decimal.Decimal(row['confirmed_per_100k_inhabitants']) if row['confirmed_per_100k_inhabitants'] else None,
                    decimal.Decimal(row['death_rate']) if row['death_rate'] else None
                ))
                db.commit()
            except Exception as e:
                db.rollback()
                logging.error(f"Erro ao inserir dados: {e}")
                logging.error(f"Dado problemático: {row}")
                pass

    cur.close()
    db.close()
    return (time.time() - start_time)

def execute_query(conn, query, params=None):
    start_time = time.time()
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        docs = cursor.fetchall()
        duration = time.time() - start_time
    return {'docs': docs, 'duration': int(duration * 1000)}

def get_all_places(conn):
    query = 'SELECT * FROM covid_data'
    return execute_query(conn, query)

def get_all_states(conn):
    query = 'SELECT DISTINCT state FROM covid_data'
    return execute_query(conn, query)

def get_all_cities(conn, state):
    query = 'SELECT * FROM covid_data WHERE state = %s'
    return execute_query(conn, query, (state,))

def get_data_by_date_and_place(conn, date, place):
    state, city = list(place.items())[0]
    query = 'SELECT * FROM covid_data WHERE date = %s AND state = %s AND city = %s'
    return execute_query(conn, query, (date, state, city))

def get_total_by_date_range_and_place(conn, start_date, end_date, place):
    state, city = list(place.items())[0]
    query = '''SELECT SUM(confirmed), SUM(deaths) FROM covid_data
               WHERE date BETWEEN %s AND %s AND state = %s AND city = %s'''
    return execute_query(conn, query, (start_date, end_date, state, city))

def get_average_by_date_range_and_place(conn, start_date, end_date, place):
    state, city = list(place.items())[0]
    query = '''SELECT AVG(confirmed), AVG(deaths) FROM covid_data
               WHERE date BETWEEN %s AND %s AND state = %s AND city = %s'''
    return execute_query(conn, query, (start_date, end_date, state, city))

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

def run_benchmarks():
    pg_conn = psqlConnect()
    try:
        display_benchmark('Postgres: Obter número de casos confirmados em uma cidade específica em uma data específica', get_data_by_date_and_place, pg_conn, '2020-06-01', {'PR': 'Curitiba'})
        display_benchmark('Postgres: Contar a quantidade total de casos confirmados em um estado específico', get_all_states, pg_conn)
        display_benchmark('Postgres: Listar a quantidade de casos confirmados por cidade dentro de um estado', get_all_cities, pg_conn, 'AC')
        display_benchmark('Postgres: Encontrar o número de mortes em uma cidade específica em uma data específica', get_data_by_date_and_place, pg_conn, '2020-06-01', {'PR': 'Curitiba'})
        display_benchmark('Postgres: Contar o número total de mortes em um estado específico', get_total_by_date_range_and_place, pg_conn, '2020-03-01', '2020-03-31', {'PR': 'Curitiba'})
        display_benchmark('Postgres: Obter todas as cidades que não registraram novos casos em uma data específica', get_average_by_date_range_and_place, pg_conn, '2020-06-01', '2020-06-30', {'PR': 'Curitiba'})
    finally:
        pg_conn.close()

if __name__ == "__main__":
    logging.basicConfig(
        filename='LogPostgreSQL.log',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

    if len(sys.argv) < 2:
        print("Uso: python PostgreSQL.py <caminho_para_csv>")
        sys.exit(1)

    csvfile = sys.argv[1]

    logging.info("Criando tabela COVID-19 no PostgreSQL")
    criaTabelaCovidPSQL()

    logging.info("Carregando dados COVID-19 no PostgreSQL")
    exec_time = carregaTabelaCovidPSQL(csvfile)
    logging.info("PostgreSQL: --- %s seconds --- (%s)\n\n" %
                 (exec_time, humanfriendly.format_timespan(exec_time)))

    logging.info("Executando benchmarks no PostgreSQL")
    run_benchmarks()
