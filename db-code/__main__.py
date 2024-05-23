import os
import sqlite3
import pandas as pd

project_path = os.path.dirname(__file__)
files_path = '{}/files'.format(project_path)
download_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv'
download_file_path = '{}/files/INSTRUCTOR.csv'.format(project_path)

if os.path.isfile(download_file_path) == False:
    os.system('wget -q -P {} {}'.format(files_path, download_url))

conn = sqlite3.connect('{}/STAFF.db'.format(files_path))


