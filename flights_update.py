from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import requests
import pandas as pd

def update_flights_sql():
    connection_string = create_connection_string()
    airport_list = get_airport_list(connection_string)
    flights_df = get_flights(airport_list)
    update_sql(flights_df, connection_string)

    return 'Flights table updated'


def create_connection_string():
    schema = "..."
    host = "..."
    user = "..."
    password = "..."
    port = ...

    return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


def get_airport_list(connection_string):
    airports_from_sql = pd.read_sql("airports", con=connection_string)
    return list(airports_from_sql['iata'])


def get_flights(airport_list):
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    ini_times = ['00', '12']
    fin_times = ['11', '23']

    flights_df = []

    for iata in airport_list:
        flights = []

        for ini_time, fin_time in zip(ini_times, fin_times):
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/iata/{iata}/{tomorrow}T{ini_time}:00/{tomorrow}T{fin_time}:59"
            querystring = {
                "withLeg": "false",
                "direction": "Arrival",
                "withCancelled": "false",
                "withCodeshared": "true",
                "withCargo": "false",
                "withPrivate": "false",
                "withLocation": "false"
            }
            headers = {
                "X-RapidAPI-Key": "...",
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
            }

            response = requests.get(url, headers=headers, params=querystring)
            output = pd.json_normalize(response.json().get('arrivals', []))

            if not output.empty:
                flights.append(output[['number', 'movement.airport.iata', 'movement.scheduledTime.local']])

        if flights:
            temp_flights_df = pd.concat(flights, ignore_index=True)
            temp_flights_df['arrival_iata'] = iata
            temp_flights_df.rename(columns={
                'number': 'flight_num',
                'movement.airport.iata': 'departure_iata',
                'movement.scheduledTime.local': 'arrival_time'
            }, inplace=True)
            flights_df.append(temp_flights_df)

    flights_df = pd.concat(flights_df, ignore_index=True)
    flights_df["arrival_time"] = pd.to_datetime(flights_df["arrival_time"])

    return flights_df


def update_sql(flights_df, connection_string):
    engine = create_engine(connection_string)

    try:
        # Using a transaction block to ensure that the operation is atomic
        with engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE flights"))
            flights_df.to_sql('flights', con=connection, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")


update_flights_sql()
