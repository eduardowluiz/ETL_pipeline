import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def update_weather_sql():
    connection_string = create_connection_string()
    cities_from_sql = load_city_list(connection_string)
    weather_df = create_weather_df(cities_from_sql['city'])
    update_sql(cities_from_sql, weather_df, connection_string)

    return 'Table weather_info updated in SQL'


def create_connection_string():
    schema = "gans_case_study"
    host = "34.38.230.232"
    user = "root"
    password = "edu161"
    port = 3306

    return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


def load_city_list(connection_string):
    return pd.read_sql("cities", con=connection_string)


def create_weather_df(city_list):
    APIkey = 'be5e64506aa380c0f15a4244bc962978'
    weather_dictionary = []

    for city in city_list:
        url = f"https://api.openweathermap.org/data/2.5/forecast?q={city},DE&appid={APIkey}&units=metric"
        weather_data = requests.get(url)
        wd_js = weather_data.json()

        for entry in wd_js['list']:
            weather_dictionary.append({
                "city": city,
                "timestamp": datetime.today().strftime("%Y-%m-%d %H:%M"),
                "forecast_time": entry['dt_txt'],
                "outlook": entry['weather'][0]['description'],
                "temperature": entry['main']['temp'],
                "feels_like": entry['main']['feels_like'],
                "wind_speed": entry['wind']['speed'],
                "rain_prob": entry['pop'],
                "rain_amount": entry.get('rain', {}).get('3h', 0),
                "snow_amount": entry.get('snow', {}).get('3h', 0)
            })

    df = pd.DataFrame(weather_dictionary)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["forecast_time"] = pd.to_datetime(df["forecast_time"])

    return df


def update_sql(cities_from_sql, weather_df, connection_string):
    weather_info_df = weather_df.merge(cities_from_sql, on="city", how="left")
    weather_info_df = weather_info_df.drop(columns='city')

    engine = create_engine(connection_string)

    try:
        # Using a transaction block to ensure that the operation is atomic
        with engine.begin() as connection:
            # Using the text construct to ensure the SQL command is executed as raw SQL (otherwise won't work)
            connection.execute(text("TRUNCATE TABLE weather_info"))
            weather_info_df.to_sql('weather_info', con=connection, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")


update_weather_sql()
