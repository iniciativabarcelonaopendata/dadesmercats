import csv

import pandas as pd
import json

from utils import database_operation


# DATABASE-RELATED FUNCTIONS

@database_operation
def create_database(cursor):
    # Create tables
    cursor.execute("DROP TABLE IF EXISTS mercats;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mercats(
            id SMALLINT NOT NULL AUTO_INCREMENT UNIQUE,
            nom VARCHAR(100),
            posicio POINT,
            municipi VARCHAR(50),
            PRIMARY KEY(id),
            FOREIGN KEY(municipi) REFERENCES municipis(nom)
        );
        """)
    cursor.execute("DROP TABLE IF EXISTS populartimes;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS populartimes(
            mercat_id SMALLINT NOT NULL,
            day_of_week CHAR(9),
            hour TINYINT,
            value TINYINT,
            timestamp TIMESTAMP
        );
        """)
    cursor.execute("DROP TABLE IF EXISTS plots;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plots(
            id SMALLINT NOT NULL AUTO_INCREMENT UNIQUE,
            plot_type VARCHAR(10),
            added_parameters VARCHAR(50),
            html TEXT,
            timestamp TIMESTAMP,
            PRIMARY KEY(id)
        );
        """)
    cursor.execute("DROP TABLE IF EXISTS municipis;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS municipis(
            nom VARCHAR(50),
            area TINYINT,
            cens INT,
            dia_mercat VARCHAR(70),
            renda_familiar FLOAT,
            lloguer_mensual FLOAT,
            index_envelliment FLOAT,
            PRIMARY KEY(nom)
        );
        """)


# 'Insert' functions: they read the data and pull it to the database

@database_operation
def insert_mercats(cursor):
    mercats = []
    filename = "data/mercats.csv"
    cursor.execute("DELETE FROM mercats;")

    with open(file=filename, mode='r', encoding='UTF-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            mercat_name = row[0]
            mercat_lat = float(row[1])
            mercat_lng = float(row[2])
            mercat_point = f'POINT({mercat_lat} {mercat_lng})'
            mercat_municipi = row[3]
            mercats.append((mercat_name, mercat_point, mercat_municipi))

    query = "INSERT INTO mercats (nom, posicio, municipi) VALUES (%s, ST_GeomFromText(%s), %s);"
    cursor.executemany(query, mercats)


@database_operation
def insert_popular_times(cursor, popular_times):
    query = "INSERT INTO populartimes (mercat_id, day_of_week, hour, value) VALUES (%s, %s, %s, %s);"
    cursor.executemany(query, popular_times)


@database_operation
def insert_municipis(cursor):
    filename = "data/municipis.csv"
    cursor.execute("DELETE FROM municipis;")

    with open(file=filename, mode='r', encoding='UTF-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        values = []
        for row in reader:
            nom = row[0]
            cens = int(row[1])
            dia_mercat = row[2]
            renda_bruta_familiar_disponible = int(row[3])
            lloguer_mensual = float(row[4]) if row[4] != '' else 0  # Notice this means SQL will have to avoid 0's
            index_envelliment = float(row[5]) if row[5] != '' else 0  # Notice this means SQL will have to avoid 0's
            values.append(
                (nom, 1, cens, dia_mercat, renda_bruta_familiar_disponible, lloguer_mensual, index_envelliment))
    query = "INSERT INTO municipis(nom, area, cens, dia_mercat, renda_familiar, lloguer_mensual, index_envelliment) VALUES(%s, %s, %s, %s, %s, %s, %s);"
    cursor.executemany(query, values)


# 'Read' functions: they display the result of a select upon each table

@database_operation
def read_mercats(cursor):
    cursor.execute("SELECT * FROM mercats;")
    print(cursor.fetchall())


@database_operation
def read_populartimes(cursor):
    cursor.execute("SELECT * FROM populartimes;")
    print(cursor.fetchall())


@database_operation
def read_municipis(cursor):
    cursor.execute("SELECT * FROM municipis;")
    print(cursor.fetchall())


# 'Load' functions: they take the data from database and load into memory to be able to work with them

@database_operation
def load_mercats(cursor):
    cursor.execute(
        "SELECT id, nom, ST_X(posicio), ST_Y(posicio) FROM mercats;")  # ******************************* CHANGE THIS ********
    return cursor.fetchall()


@database_operation
def load_populartimes(cursor):
    cursor.execute("SELECT * FROM populartimes;")
    return cursor.fetchall()


# The following 'load' functions receive an already open connection (notice lack of @database_operation)
# due to them all being made in one single connection upon the plotting process

def load_geojson(cnx):
    df = pd.read_sql("""
        SELECT nom, ST_AsGeoJSON(posicio) posicio, municipi, page_id 
        FROM mercats mer INNER JOIN mercatspages mp ON mer.id = mp.mercat_id;
    """,
                     con=cnx)

    feature_collection = {'type': "FeatureCollection", 'features': []}
    for i in range(0, len(df.index)):
        point = json.loads(df['posicio'][i])
        feature = {
            'type': "Feature",
            'geometry': point,
            'properties': {
                'nom': df['nom'][i],
                'municipi': df['municipi'][i],
                'page_id': int(df['page_id'][i])
            }
        }
        feature_collection['features'].append(feature)

    return feature_collection


def load_avg_popular_times_per_hour_all_time(cnx):
    df = pd.read_sql("""
        SELECT AVG(value) AS valor_mitja 
        FROM populartimes 
        GROUP BY hour;
        """,
                     con=cnx)
    return df


def load_avg_popular_times_per_hour_last_week(cnx):
    df = pd.read_sql("""
        SELECT AVG(value) AS valor_mitja 
        FROM populartimes 
        WHERE TIMESTAMPDIFF(WEEK, timestamp, CURRENT_TIMESTAMP()) <= 1 
        GROUP BY hour;
        """,
                     con=cnx)
    return df


def load_avg_all_days_vs_market_days(cnx):
    df1 = load_avg_popular_times_per_hour_all_time(cnx)
    df2 = pd.read_sql("""
        SELECT AVG(pt.value) AS valor_mitja_dies_mercat
        FROM populartimes pt 
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        WHERE mun.dia_mercat LIKE CONCAT('%', pt.day_of_week, '%')
        GROUP BY pt.hour;
        """,
                      con=cnx)
    return df1.join(df2)


def load_avg_all_days_vs_market_days_no_bcn(cnx):
    df1 = load_avg_popular_times_per_hour_all_time(cnx)
    df2 = pd.read_sql("""
        SELECT AVG(pt.value) AS valor_mitja_dies_mercat
        FROM populartimes pt 
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        WHERE mun.dia_mercat LIKE CONCAT('%', pt.day_of_week, '%')
        AND mun.nom != 'Barcelona'
        GROUP BY pt.hour;
        """,
                      con=cnx)
    return df1.join(df2)


# esto es una cagada celestial
# no exporta las horas
# no ordena los datos
# puede recibir varios (no es el máximo sino los de la última semana, y no los promedia)
def load_indiv_market_this_week(id_mercat, cnx):
    df = pd.read_sql(f"""
        SELECT value, day_of_week
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
        WHERE mer.nom = \"{id_mercat}\"
            AND TIMESTAMPDIFF(WEEK, pt.timestamp, CURRENT_TIMESTAMP()) <= 1
        ORDER BY
            day_of_week
        LIMIT 126;
        """,
                     con=cnx)
    return df

def load_indiv_market_this_week2(id_mercat, cnx):
    df = pd.read_sql(f"""
       SELECT 
            `mercat_id`, `day_of_week`, `hour`, `value`, max(timestamp) 
       FROM  
            populartimes 
        WHERE 
            `mercat_id` =  \"{id_mercat}\"
        GROUP BY 
            mercat_id, `day_of_week`, `hour`
        """,
                     con=cnx)
    return df


def load_indiv_market_last_week2(id_mercat, cnx):
    df = pd.read_sql(f"""
       SELECT 
            `mercat_id`, `day_of_week`, `hour`, `value`, max(timestamp) 
       FROM  
            populartimes 
        WHERE 
            `mercat_id` = \"{id_mercat}\" AND
            TIMESTAMPDIFF(WEEK, `timestamp`, CURRENT_TIMESTAMP()) BETWEEN 1 AND 2 
        GROUP BY 
            mercat_id, `day_of_week`, `hour`
        """,
                     con=cnx)
    return df


def load_indiv_market_last_week(id_mercat, cnx):
    df = pd.read_sql(f"""
        SELECT value, day_of_week
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
        WHERE mer.nom = \"{id_mercat}\"
            AND TIMESTAMPDIFF(WEEK, pt.timestamp, CURRENT_TIMESTAMP()) BETWEEN 1 AND 2
        LIMIT 168;
        """,
                     con=cnx)
    return df


def load_indiv_market_all_time(id_mercat, cnx):
    df = pd.read_sql(f"""
        SELECT value, day_of_week
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
        WHERE mer.id = \"{id_mercat}\"
        GROUP BY day_of_week, hour;
        """,
                     con=cnx)
    return df

def load_indiv_market_all_time2(id_mercat, cnx):
    df = pd.read_sql(f"""
        SELECT 
            `mercat_id`, `day_of_week`, `hour`, AVG(`value`) 
        FROM 
            populartimes 
        WHERE 
            `mercat_id` = \"{id_mercat}\"
        GROUP BY 
            mercat_id, `day_of_week`, `hour`
        """,
                     con=cnx)
    return df


def load_avg_population_groups(cnx):
    df = pd.read_sql("""
        SELECT AVG(pt.value) value, pt.hour hour,
        CASE 
            WHEN mun.cens < 1000 THEN "< 1000"
            WHEN mun.cens BETWEEN 1001 AND 5000 THEN "1001-5000"
            WHEN mun.cens BETWEEN 5001 AND 10000 THEN "5001-10000"
            WHEN mun.cens BETWEEN 10001 AND 50000 THEN "10001-50000"
            WHEN mun.cens BETWEEN 50001 AND 1000000 THEN "50001-1000000"
            WHEN mun.cens > 1000000 THEN "> 1000000"
        END AS tram_poblacio
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        GROUP BY tram_poblacio, hour;
        """,
                     con=cnx)
    return df


def load_avg_renda_familiar(cnx):
    df = pd.read_sql("""
        SELECT AVG(pt.value) value, pt.hour hour,
        CASE 
            WHEN mun.renda_familiar BETWEEN 11000 AND 14000 THEN "11000€-14000€"
            WHEN mun.renda_familiar BETWEEN 14001 AND 17000 THEN "14001€-17000€"
            WHEN mun.renda_familiar BETWEEN 17001 AND 20000 THEN "17001€-20000€"
            WHEN mun.renda_familiar BETWEEN 20001 AND 23000 THEN "20001€-23000€"
            WHEN mun.renda_familiar > 23000 THEN "> 23000€"
        END AS tram_renda_familiar
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        GROUP BY tram_renda_familiar, hour;
        """,
                     con=cnx)
    return df


def load_avg_renda_familiar_no_bcn(cnx):
    df = pd.read_sql("""
        SELECT AVG(pt.value) value, pt.hour hour,
        CASE 
            WHEN mun.renda_familiar BETWEEN 11000 AND 14000 THEN "11000€-14000€"
            WHEN mun.renda_familiar BETWEEN 14001 AND 17000 THEN "14001€-17000€"
            WHEN mun.renda_familiar BETWEEN 17001 AND 20000 THEN "17001€-20000€"
            WHEN mun.renda_familiar BETWEEN 20001 AND 23000 THEN "20001€-23000€"
            WHEN mun.renda_familiar > 23000 THEN "> 23000€"
        END AS tram_renda_familiar
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        WHERE mun.nom != 'Barcelona'
        GROUP BY tram_renda_familiar, hour;
        """,
                     con=cnx)
    return df


def load_municipi_i_renda_familiar(cnx):
    df = pd.read_sql("""
        SELECT nom,
        CASE 
            WHEN renda_habitant BETWEEN 11000 AND 14000 THEN "11000€-14000€"
            WHEN renda_habitant BETWEEN 14001 AND 17000 THEN "14001€-17000€"
            WHEN renda_habitant BETWEEN 17001 AND 20000 THEN "17001€-20000€"
            WHEN renda_habitant BETWEEN 20001 AND 23000 THEN "20001€-23000€"
            WHEN renda_habitant > 23000 THEN "> 23000€"
        END AS tram_renda_familiar
        FROM municipis
        ORDER BY nom;
        """,
                     con=cnx)
    return df


def load_avg_preu_lloguer(cnx):
    df = pd.read_sql("""
        SELECT AVG(pt.value) value, pt.hour hour,
        CASE 
            WHEN lloguer_mensual BETWEEN 1 AND 300 THEN "< 300€"
            WHEN lloguer_mensual BETWEEN 301 AND 400 THEN "301€-400€"
            WHEN lloguer_mensual BETWEEN 401 AND 500 THEN "401€-500€"
            WHEN lloguer_mensual BETWEEN 501 AND 700 THEN "501€-700€"
            WHEN lloguer_mensual BETWEEN 701 AND 900 THEN "701€-900€"
            WHEN lloguer_mensual > 901 THEN "> 901€"
        END AS tram_lloguer_mensual
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        GROUP BY tram_lloguer_mensual, hour;
        """,
                     con=cnx)
    return df.loc[18:]


def load_avg_preu_lloguer_no_bcn(cnx):
    df = pd.read_sql("""
        SELECT AVG(pt.value) value, pt.hour hour,
        CASE 
            WHEN lloguer_mensual BETWEEN 1 AND 300 THEN "< 300€"
            WHEN lloguer_mensual BETWEEN 301 AND 400 THEN "301€-400€"
            WHEN lloguer_mensual BETWEEN 401 AND 500 THEN "401€-500€"
            WHEN lloguer_mensual BETWEEN 501 AND 700 THEN "501€-700€"
            WHEN lloguer_mensual BETWEEN 701 AND 900 THEN "701€-900€"
            WHEN lloguer_mensual > 901 THEN "> 901€"
        END AS tram_lloguer_mensual
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        WHERE mun.nom != 'Barcelona'
        GROUP BY tram_lloguer_mensual, hour;
        """,
                     con=cnx)
    return df.loc[18:]


def load_avg_index_envelliment(cnx):
    df = pd.read_sql("""
        SELECT AVG(pt.value) value, pt.hour hour,
        CASE 
            WHEN index_envelliment BETWEEN 0 AND 75 THEN "< 75%"
            WHEN index_envelliment BETWEEN 75 AND 100 THEN "75-100%"
            WHEN index_envelliment BETWEEN 100 AND 150 THEN "100-150%"
            WHEN index_envelliment > 150 THEN "> 150%"
        END AS tram_index_envelliment
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        GROUP BY tram_index_envelliment, hour;
        """,
                     con=cnx)
    return df


def load_avg_index_envelliment_no_bcn(cnx):
    df = pd.read_sql("""
        SELECT AVG(pt.value) value, pt.hour hour,
        CASE 
            WHEN index_envelliment BETWEEN 0 AND 75 THEN "< 75%"
            WHEN index_envelliment BETWEEN 75 AND 100 THEN "75-100%"
            WHEN index_envelliment BETWEEN 100 AND 150 THEN "100-150%"
            WHEN index_envelliment > 150 THEN "> 150%"
        END AS tram_index_envelliment
        FROM populartimes pt
            INNER JOIN mercats mer
                ON pt.mercat_id = mer.id
            INNER JOIN municipis mun
                ON mer.municipi = mun.nom
        WHERE mun.nom != 'Barcelona'
        GROUP BY tram_index_envelliment, hour;
        """,
                     con=cnx)
    return df


def load_indiv_market_data(cnx):
    df = pd.read_sql("""
        SELECT id, nom
        FROM mercats;
        """,
                     con=cnx)
    return df


# 'write_plot' function: takes all the generated plots and writes them to DB

@database_operation
def write_plots(cursor, plots):
    cursor.execute(
        "INSERT INTO plots_backup(plot_type, added_parameters, html, timestamp) SELECT plot_type, added_parameters, html, timestamp FROM plots;")
    cursor.execute("DELETE FROM plots;")
    query = "INSERT INTO plots(plot_type, added_parameters, html) VALUES (%s, %s, %s);"
    cursor.executemany(query, plots)
