import pandas as pd 
import requests as req
from datetime import datetime
import json
import sqlalchemy 
import psycopg2


# Identify the city and country (code country)
city_name = "Alexandria"
country_code = "EG"

# we created a .txt file that contains only the API Key 
with open('API.txt',"r") as f :
    API_key = f.read()

# Our full url with identified city , country and API key
url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name},{country_code}&appid={API_key}"



# Extraction into json file 

try :
    r = req.get(url)
    data = r.json()
    #print(data)
    print("\n ..... Data Extracted successfully .....")

except KeyError as e:
        print(f"There is something wrong with your url as the error is : {e} ")

#we need convert Kelvin temp. unit into Celsius temp. unit
def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius



# we can take a specific informations 
city = data["name"]
country = data['sys']['country']
description = data["weather"][0]["description"]
temperature = kelvin_to_celsius(data["main"]["temp"])
feel_temp = kelvin_to_celsius(data["main"]["feels_like"])
min_temp = kelvin_to_celsius(data["main"]["temp_min"])
max_temp = kelvin_to_celsius(data["main"]["temp_max"])
humidity = data["main"]["humidity"]
wind_speed = data["wind"]["speed"]
wind_direction = data["wind"]["deg"]
record_time = datetime.utcfromtimestamp(data['dt']+data['timezone'])

renamed_columns_of_data =[{
    "city":city,
    "country":country,
    "description":description,
    "temp_c":temperature,
    "feeling_temp_c":feel_temp,
    "max_temp_c":max_temp,
    "min_temp_c":min_temp,
    "humidity":humidity,
    "speed_of_wind":wind_speed,
    "direction_of_wind":wind_direction,
    "time_of_record":record_time
    }]

data_df = pd.DataFrame(renamed_columns_of_data)
print(data_df)
    


'''We can save our data to csv file or to DataBase , But i choose to save it to DataBase'''

transformed_data = data_df

connection_uri = "postgresql://postgres:1234@localhost:5432/open_weather_api"

db_engine = sqlalchemy.create_engine(connection_uri)

transformed_data.to_sql(
    name = "alexandria_weather",
    con = db_engine,
    schema="public",
    if_exists="append",
    index= False
    )
