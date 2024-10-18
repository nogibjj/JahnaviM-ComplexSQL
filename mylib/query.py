'''Functions to read dataset'''

import sqlite3

def create_record():
    '''Creates a record in bad drivers data based for a made up state - NewState'''
    # Create connection
    connection = sqlite3.connect("badDrivers.db")
    cursor = connection.cursor()

    # Insert row
    print('[CREATE] Inserting a row for a NewState...')
    cursor.execute("INSERT INTO badDrivers (state, drivers_count, speeding_percent, alc_percent, no_distraction_percent, no_prev_percent, car_insurance, insurance_losses) VALUES ('NewState',13,25,22,81,94,872.46,195.63);")
    
    # Display inserted row
    query = "SELECT * FROM badDrivers WHERE state = 'NewState';"
    cursor.execute(query)
    output = cursor.fetchall()
    print(f'   Executing the Query: \n\t{query}\n   Output:')
    for row in output:
        print('\t', row)
    
    # Close connection
    connection.commit()
    connection.close()
    print()
    return output

def read_db():
    '''Reads the bad drivers db and shows those results'''
    # Create connection
    connection = sqlite3.connect('badDrivers.db')
    cursor = connection.cursor()

    # Read rows
    print('[READ] Reading the first 5 rows...')
    query = 'SELECT * FROM badDrivers ORDER BY state LIMIT 5;'
    cursor.execute(query)
    output = cursor.fetchall()

    # Display results
    print(f'   Query: \n\t{query}\n   Output:')
    for row in output:
        print('\t', row)
    
    # Close connection
    connection.close()
    print()
    return output

def update_ca():
    '''Updates bad drivers db California drivers_count value'''
    # Create connection
    connection = sqlite3.connect('badDrivers.db')
    cursor = connection.cursor()

    # Update Row
    print('[UPDATE] Updating Califonia data...')
    query1 = "UPDATE badDrivers SET drivers_count = 13 WHERE state = 'California'; "
    cursor.execute(query1)

    # Display Results
    query2 = "SELECT * FROM badDrivers WHERE state = 'California';"
    cursor.execute(query2)
    output = cursor.fetchall()
    print(f'   Queries: \n\t{query1}\n\t{query2}\n   Output:')
    for row in output:
        print('\t', row)
    
    # Close connection
    connection.commit()
    connection.close()
    print()
    return output

def delete_ca():
    '''Deletes rows from bad drivers db when the state value is California'''
    # Create connection
    connection = sqlite3.connect('badDrivers.db')
    cursor = connection.cursor()

    # Delete row
    print('[DLETE] Deleting a row...')
    query1 = "DELETE FROM badDrivers WHERE state = 'California'; "
    cursor.execute(query1)

    # Display results
    query2 = "SELECT * FROM badDrivers order by state LIMIT 5;"
    cursor.execute(query2)
    output = cursor.fetchall()
    print(f'   Queries: \n\t{query1}\n\t{query2}\n   Output:')
    for row in output:
        print('\t', row)

    # Close connection
    connection.commit()
    connection.close()
    return output
