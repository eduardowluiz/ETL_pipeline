import pandas as pd
from datetime import datetime
from lat_lon_parser import parse  # for decimal coordinates
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def import_cities_information():
    cities_df = pd.DataFrame({
        "city": ['Frankfurt', 'Berlin', 'Hamburg', 'Munich', 'Cologne', 'Bonn', 'Hannover']
    })
    connection_string = create_connection_string()
    update_cities_table(connection_string, cities_df)
    update_city_info_table(connection_string, cities_df)


def create_connection_string():
    schema = "gans_case_study"
    host = "34.38.230.232"
    user = "root"
    password = "edu161"
    port = 3306

    return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


def city_details(city_list):
    city_data = []

    for city in city_list:
        url = "https://en.wikipedia.org/wiki/" + city
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        country = soup.find(string="Country").find_next().get_text()

        lat_str = soup.find('span', class_='latitude').get_text()
        lon_str = soup.find('span', class_='longitude').get_text()

        population = soup.find('table', class_='vcard').find(
            string="Population").find_next(class_="infobox-data").get_text()
        population = population.replace(',', '')
        population = int(population)
        today = datetime.today().strftime("%d.%m.%Y")

        city_data.append({
            "city": city,
            "country": country,
            "latitude": round(parse(lat_str), 2),
            "longitude": round(parse(lon_str), 2),
            "population": population,
            "timestamp": today
        })

    df = pd.DataFrame(city_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    return df


def update_cities_table(connection_string, cities_df):
    engine = create_engine(connection_string)

    try:
        # Using a transaction block to ensure that the operation is atomic
        with engine.begin() as connection:
            # Using the text construct to ensure the SQL command is executed as raw SQL (otherwise won't work)
            connection.execute(text("TRUNCATE TABLE cities"))
            cities_df.to_sql('cities', con=connection, if_exists='append', index=False)

    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")

    print('Table cities updated')


def update_city_info_table(connection_string, cities_df):
    city_info_df = city_details(cities_df['city'])
    cities_from_sql = pd.read_sql("cities", con=connection_string)
    city_info_df = city_info_df.merge(cities_from_sql, on="city", how="left")
    city_info_df = city_info_df.drop(columns='city')

    engine = create_engine(connection_string)

    try:
        # Using a transaction block to ensure that the operation is atomic
        with engine.begin() as connection:
            # Using the text construct to ensure the SQL command is executed as raw SQL (otherwise won't work)
            connection.execute(text("TRUNCATE TABLE city_info"))
            city_info_df.to_sql('city_info', con=connection, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")

    print('Table city_info updated')


import_cities_information()
