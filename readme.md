# ETL-pipeline

This project was developed during the Data Science Bootcamp from WBS Coding School.

The project is about a fictitious e-scooter company, which is trying to efficiently distribute its scooter in several cities. 
The task was to collect data for these cities, e.g. population, location (latitude and longitude), weather forecast and data on arrivals in the local airports of these cities.

The ETL pipeline uses Python to gather information by:

web scraping wikipedia.org.
API calls for weather information
API calls for information on arriving flights for the next day
in the folder /Notebooks you will find the python code for the data extraction, transformation and loading into a local MySQL instance, as well as comments to the code.

All data is stored in a relational database containing the tables city_table, airports_table, city_airport_table, city_data_table, weather_table, and arrivals_table. 

To run this project, you need an API key for the Weather API - 5-day forecast as well as AeroDataBox. Free options with monthly limited requests are available.
