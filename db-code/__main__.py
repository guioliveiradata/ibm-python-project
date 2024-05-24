import os
import sqlite3
import pandas as pd

# Download the files 
project_path = os.path.dirname(__file__)
files_path = '{}/files'.format(project_path)
download_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv'
csv_path = '{}/files/INSTRUCTOR.csv'.format(project_path)

if os.path.isfile(csv_path) == False:
    os.system('wget -q -P {} {}'.format(files_path, download_url))

# Start the connection to the database
conn = sqlite3.connect('{}/STAFF.db'.format(files_path))

# Declare the table name and its attributes
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Create a dataframe with the .csv data
df = pd.read_csv(csv_path, names=attribute_list)

# Load the data to the 'INSTRUCTOR' table on the database
df.to_sql(table_name, conn, if_exists='replace', index=False)
print('Table is ready')

# Select all the data from the INSTRUCTOR table
query = '\nSELECT * FROM INSTRUCTOR'
output = pd.read_sql(query, conn)
print(query + '\n')
print(output)

# Select all the rows on the FNAME column
query = '\nSELECT FNAME FROM INSTRUCTOR'
output = pd.read_sql(query, conn)
print(query + '\n')
print(output)

# Count the number of rows on the INSTRUCTOR table
query = '\nSELECT COUNT(*) FROM INSTRUCTOR'
output = pd.read_sql(query, conn)
print(query + '\n')
print(output)

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists='append', index=False)
print('\nData appended successfully')

# Count the number of rows on the INSTRUCTOR table
query = '\nSELECT COUNT(*) FROM INSTRUCTOR'
output = pd.read_sql(query, conn)
print(query + '\n')
print(output)

conn.close()
