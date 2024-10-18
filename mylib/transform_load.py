'''Takes a csv and loads it as a db'''

import csv
import os
from dotenv import load_dotenv
from databricks import sql
import pandas

PATH = 'data/bad-drivers.csv'

def trans_load(path = PATH):
    '''Takes the csv file and transforms it onto databricks db'''
    payload = csv.reader(open(path, newline = ""), delimiter = ",")
    print(*payload)
    load_dotenv()
    print(os.getenv("SERVER_HOSTNAME"))
    print(os.getenv("HTTP_PATH"))
    print(os.getenv("DATABRICKS_KEY"))

    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                     http_path       = os.getenv("HTTP_PATH"),
                     access_token    = os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM samples.nyctaxi.trips LIMIT 2")
            # corsor.execute('''CREATE TABLE jm_badDrivers 
            # (state STRING, drivers_count DOUBLE, speeding_percen FLOAT, alc_percent FLOAT, no_distraction_percent FLOAT, 
            # no_prev_percent FLOAT, car_insurance FLOAT, insurance_losses FLOAT)''')
    
            result = cursor.fetchall()

            for row in result:
                print(row)

            cursor.close()
            connection.close()

if __name__ == "__main__":
    print('is this running?')
    trans_load()
    print('trans_load completed')
