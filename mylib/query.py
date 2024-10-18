'''Functions to read dataset'''

import os
from databricks import sql
from dotenv import load_dotenv

def create_table2():
    '''Creates a record in bad drivers data based for a made up state - NewState'''
    load_dotenv()
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                     http_path       = os.getenv("HTTP_PATH"),
                     access_token    = os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:
            # if table exists, drop it
            cursor.execute('''DROP TABLE IF EXISTS jm_baddrivers_speed;''')
            # Create main table
            cursor.execute('''CREATE TABLE jm_baddrivers_speed 
                           AS SELECT state, drivers_count*speeding_percent/100 as speed_ct FROM jm_baddrivers;''')
            cursor.close()
            connection.close()

def query_complex():
    load_dotenv()
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                     http_path       = os.getenv("HTTP_PATH"),
                     access_token    = os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:
            # Combine both tables
            cursor.execute('''SELECT round(df.drivers_count) as rounded_driv_ct, COUNT(df.state) as num_states, AVG(df_sp.speed_ct) as avg_ct_speed
                           FROM jm_baddrivers df
                           LEFT JOIN jm_baddrivers_speed df_sp ON df.state = df_sp.state
                           GROUP BY round(df.drivers_count)
                           ORDER BY round(df.drivers_count);''')
            output = cursor.fetchall()
            for row in output:
                print(row)
            cursor.close()
            connection.close()
    

if __name__ == "__main__":
    create_table2()
    query_complex()

# def read_db():
#     '''Reads the bad drivers db and shows those results'''
#     # Create connection
#     connection = sqlite3.connect('badDrivers.db')
#     cursor = connection.cursor()

#     # Read rows
#     print('[READ] Reading the first 5 rows...')
#     query = 'SELECT * FROM badDrivers ORDER BY state LIMIT 5;'
#     cursor.execute(query)
#     output = cursor.fetchall()

#     # Display results
#     print(f'   Query: \n\t{query}\n   Output:')
#     for row in output:
#         print('\t', row)
    
#     # Close connection
#     connection.close()
#     print()
#     return output

# def update_ca():
#     '''Updates bad drivers db California drivers_count value'''
#     # Create connection
#     connection = sqlite3.connect('badDrivers.db')
#     cursor = connection.cursor()

#     # Update Row
#     print('[UPDATE] Updating Califonia data...')
#     query1 = "UPDATE badDrivers SET drivers_count = 13 WHERE state = 'California'; "
#     cursor.execute(query1)

#     # Display Results
#     query2 = "SELECT * FROM badDrivers WHERE state = 'California';"
#     cursor.execute(query2)
#     output = cursor.fetchall()
#     print(f'   Queries: \n\t{query1}\n\t{query2}\n   Output:')
#     for row in output:
#         print('\t', row)
    
#     # Close connection
#     connection.commit()
#     connection.close()
#     print()
#     return output

# def delete_ca():
#     '''Deletes rows from bad drivers db when the state value is California'''
#     # Create connection
#     connection = sqlite3.connect('badDrivers.db')
#     cursor = connection.cursor()

#     # Delete row
#     print('[DLETE] Deleting a row...')
#     query1 = "DELETE FROM badDrivers WHERE state = 'California'; "
#     cursor.execute(query1)

#     # Display results
#     query2 = "SELECT * FROM badDrivers order by state LIMIT 5;"
#     cursor.execute(query2)
#     output = cursor.fetchall()
#     print(f'   Queries: \n\t{query1}\n\t{query2}\n   Output:')
#     for row in output:
#         print('\t', row)

#     # Close connection
#     connection.commit()
#     connection.close()
#     return output
