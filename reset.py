def reset_dataset():
    connection_string = create_connection_string()
    drop_all_tables(connection_string)
    create_gans_case_study_tables(connection_string)

def drop_all_tables(connection_string):
    from sqlalchemy import create_engine, text
    
    engine = create_engine(connection_string)
    
    # Connect to the database
    with engine.connect() as connection:
        # Temporarily disable foreign key checks
        connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        # Fetch all table names
        result = connection.execute(text("SHOW TABLES"))
        tables = result.fetchall()
        
        # Drop all tables
        for table in tables:
            connection.execute(text(f"DROP TABLE IF EXISTS {table[0]}"))
    
    return "All tables dropped successfully."

def create_gans_case_study_tables(connection_string):
    from sqlalchemy import create_engine, text
    
    engine = create_engine(connection_string)
    
    create_cities_sql = """
        CREATE TABLE cities (
            city_id INT AUTO_INCREMENT,
            city VARCHAR(255) NOT NULL UNIQUE,
            PRIMARY KEY (city_id)
        );
    """
    
    create_city_info_sql = """
        CREATE TABLE city_info (
            country VARCHAR(50) NOT NULL, 
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            population INT NOT NULL, 
            timestamp DATE NOT NULL,
            city_id INT,
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        );
    """
    
    create_weather_info_sql = """
        CREATE TABLE weather_info (
            timestamp DATETIME NOT NULL,
            forecast_time DATETIME NOT NULL,
            outlook VARCHAR(50) NOT NULL, 
            temperature DOUBLE NOT NULL,
            feels_like DOUBLE NOT NULL,
            wind_speed DOUBLE NOT NULL, 
            rain_prob DOUBLE NOT NULL,
            rain_amount DOUBLE NOT NULL,
            snow_amount DOUBLE NOT NULL,
            city_id INT,
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        );
    """
    
    create_airports_sql = """
        CREATE TABLE airports (
            iata VARCHAR(255) NOT NULL,
            icao VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY (iata)
        );
    """
    
    create_flights_sql = """
        CREATE TABLE flights (
            flight_num VARCHAR(255) NOT NULL,
            departure_iata VARCHAR(255),
            arrival_time DATETIME NOT NULL,
            arrival_iata VARCHAR(255) NOT NULL,
            FOREIGN KEY (arrival_iata) REFERENCES airports(iata)
        );
    """
    
    create_city_airport_sql = """
        CREATE TABLE city_airport (
            city_id INT AUTO_INCREMENT,
            iata VARCHAR(255) NOT NULL,
            icao VARCHAR(255) NOT NULL,
            FOREIGN KEY (iata) REFERENCES airports(iata),
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        );
    """
    
    with engine.connect() as connection:
        connection.execute(text(create_cities_sql))
        print('Table cities created')
        connection.execute(text(create_city_info_sql))
        print('Table city_info created')
        connection.execute(text(create_weather_info_sql))
        print('Table weather_info created')
        connection.execute(text(create_airports_sql))
        print('Table airports created')
        connection.execute(text(create_city_airport_sql))
        print('Table city_airport created')
        connection.execute(text(create_flights_sql))
        print('Table flights created')

def create_connection_string():
    schema = ...
    host = ...
    user = ...
    password = ...
    port = ...

    return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

reset_dataset()
