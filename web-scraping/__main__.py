import os
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# Declaring the variables
project_path = os.path.dirname(__file__)
base_url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top 50'
csv_path = '{}/top_50_films.csv'.format(project_path)
df = pd.DataFrame(columns=['Average Rank', 'Film', 'Year'])
count = 0

# Getting the data
html_page = requests.get(base_url).text
data = BeautifulSoup(html_page, 'html.parser')


