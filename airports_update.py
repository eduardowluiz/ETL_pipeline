import pandas as pd
import requests

def update_airports_sql():
    connection_string = create_connection_string()
    cities_from_sql, city_info_from_sql = load_city_info_sql(connection_string)
    latitudes, longitudes = get_lat_lon(cities_from_sql, city_info_from_sql)
    airports_df = icao_airport_codes(latitudes, longitudes)
    update_sql(airports_df, connection_string)

    return 'Table airports updated in SQL'


def create_connection_string():
    schema = ...
    host = ...
    user = ...
    password = ...
    port = ...

    return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


def load_city_info_sql(connection_string):
    cities_from_sql = pd.read_sql("cities", con=connection_string)
    city_info_from_sql = pd.read_sql("city_info", con=connection_string)
    return cities_from_sql, city_info_from_sql


def get_lat_lon(cities_from_sql, city_info_from_sql):
    city_info = cities_from_sql.merge(city_info_from_sql, on="city_id", how="left")
    latitudes = list(round(city_info['latitude'], 2))
    longitudes = list(round(city_info['longitude'], 2))

    return latitudes, longitudes


def icao_airport_codes(latitudes, longitudes):
    list_for_df = []
    api_key = "14a44098c8mshe4536a007985112p1e3b4bjsn8fd805eb6bd4"
    base_url = "https://aerodatabox.p.rapidapi.com/airports/search/location"

    for lat, lon in zip(latitudes, longitudes):
        url = f"{base_url}/{lat}/{lon}/km/30/10"
        headers = {
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
            "X-RapidAPI-Key": api_key
        }
        querystring = {"withFlightInfoOnly": "true"}

        response = requests.get(url, headers=headers, params=querystring)
        response_data = response.json().get('items', [])
        list_for_df.append(pd.json_normalize(response_data))

    return pd.concat(list_for_df, ignore_index=True)


def update_sql(airports_df, connection_string):
    airports_df = airports_df[['icao', 'iata', 'name', 'municipalityName']]
    airports_df = airports_df.rename(columns={'municipalityName': 'city'})
    airports_df['city'] = airports_df['city'].replace('Hanover', 'Hannover')

    # Load city names from SQL and update airport city names
    cities_from_sql = pd.read_sql("cities", con=connection_string)['city']
    for city in cities_from_sql:
        airports_df.loc[airports_df['city'].str.contains(city, case=False, na=False), 'city'] = city

    airports_to_sql = airports_df.drop(columns='city').drop_duplicates()

    engine = create_engine(connection_string)

    try:
        # Using a transaction block to ensure that the operation is atomic
        with engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE airports"))
            airports_to_sql.to_sql('airports', con=connection, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")


update_airports_sql()