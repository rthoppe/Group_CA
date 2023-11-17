import csv
import psycopg2

def manage_database():
    try:
        dbConnection = psycopg2.connect(user='dap',
                                        password='dap',
                                        host='192.168.56.30',
                                        port='5432',
                                        database='postgres')
        dbCursor = dbConnection.cursor()
        grantStatement = "GRANT ALL PRIVILEGES ON SCHEMA public TO dap;"
        dbCursor.execute(grantStatement)
        dbConnection.commit()

        print("Privileges granted successfully!")

    except (Exception, psycopg2.Error) as dbError:
        print('Error while connecting to PostgreSQL:', dbError)

    finally:
        # Close the cursor and connection
        if dbCursor:
            dbCursor.close()
        if dbConnection:
            dbConnection.close()

# Call the function to grant privileges
manage_database()

try:
    dbConnection = psycopg2.connect(user='dap',
                                    password='dap',
                                    host='192.168.56.30',
                                    port='5432',
                                    database='postgres')
    dbConnection.set_isolation_level(0) #AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute('CREATE DATABASE housing_prices;')
    dbCursor.close()
except (Exception, psycopg2.Error) as dbError:
    print('Error while connecting to PostgreSQL', dbError)
finally:
    if(dbConnection):
        dbConnection.close()
    
createString = '''
CREATE TABLE housing_prices
(
    date_of_sale DATE,
    address VARCHAR(255),
    county VARCHAR(255),
    eircode VARCHAR(20),
    "price(€)" NUMERIC,
    not_full_market_price VARCHAR(255),
    VAT_exclusive VARCHAR(255),
    description_of_property VARCHAR(255),
    Property_Size_Description VARCHAR(255)
);
'''
try:
    dbConnection = psycopg2.connect(
                                    user='dap',
                                    password='dap',
                                    host='192.168.56.30',
                                    port='5432',
                                    database='housing_prices')
    dbConnection.set_isolation_level(0) #AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute(createString)
    dbCursor.close()
except (Exception, psycopg2.Error) as dbError:
    print('Error while connecting to PostgreSQL', dbError)
finally:
    if(dbConnection):
        dbConnection.close()
try:
    dbConnection = psycopg2.connect(
        user='dap',
        password='dap',
        host='192.168.56.30',
        port='5432',
        database='housing_prices'
    )
    dbConnection.set_isolation_level(0)  # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    
#%s placeholders were employed for parameterization due to potential issues, mitigating the risk of SQL injection.
    insertString = '''
    INSERT INTO housing_prices
    VALUES (to_date(%s, 'DD/MM/YYYY'), %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    with open('housing_prices.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # skip the header
        for row in reader:
            # Clean the numeric value before inserting
            # need to fix this so that there is only 2 decimal places
            row[4] = float(row[4].replace('€', '').replace(',', '').strip())
            dbCursor.execute(insertString, row)

    dbCursor.close()

except (Exception, psycopg2.Error) as dbError:
    print('Error while connecting to PostgreSQL', dbError)

finally:
    if dbConnection:
        dbConnection.close()

import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
sql = """
    SELECT
    AVG("price(€)") as avg_price,
    description_of_property
    FROM
    housing_prices
    GROUP BY
    description_of_property;"""
try:
    dbConnection = psycopg2.connect(
    user = "dap",
    password = "dap",
    host = "192.168.56.30",
    port = "5432",
    database = "housing_prices")
    housing_df = sqlio.read_sql_query(sql, dbConnection)
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(dbConnection):
        dbConnection.close()
        
display(housing_df)