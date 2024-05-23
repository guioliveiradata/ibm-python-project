import os
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# Declare the variables
project_path = os.path.dirname(__file__)
base_url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = '{}/Movies.db'.format(project_path)
table_name = 'Top 50'
csv_path = '{}/top_50_films.csv'.format(project_path)
df = pd.DataFrame(columns=['Average Rank', 'Film', 'Year'])
count = 0

# Extract the data
html_page = requests.get(base_url).text
data = BeautifulSoup(html_page, 'html.parser')

# Extract a list with the rows
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

# Iterate over the rows, create a dictionary for each row with 3 key:value pairs and concat the dictionary to a dataframe
for row in rows:
    if count <= 50:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                'Average Rank':col[0].contents[0],
                'Film':col[1].contents[0],
                'Year':col[2].contents[0]
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
        count += 1
    else:
        break

print(df)

# Save the dataframe as a .csv file
df.to_csv(csv_path)

# Initialize the connection to the database
conn = sqlite3.connect(db_name)
# Save the dataframe as a table
df.to_sql(table_name, conn, if_exists='replace', index=False)
# Close the connection to the database
conn.close()
