import os
import glob
import pytz
import pandas as pd
from datetime import datetime

# Paths and URLs
base_path = os.path.dirname(__file__)
files_path = r'{}/raw-files'.format(base_path)
download_url = r'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip'
log_file = r'{}/log-files/log_file.txt'.format(base_path)
target_file = r'{}/transformed_data.csv'.format(base_path)

# Download and extract the files
if os.path.exists(files_path) == False:
    os.system(r'wget -q -P {} {}'.format(base_path, download_url))    
    os.system(r'unzip -q {}/source.zip -d {}'.format(base_path, files_path))
    os.system(r'rm -f {}/source.zip'.format(base_path))

# Extract data from files and format as a DataFrame
def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    for csv_file in glob.glob(r'{}/*.csv'.format(files_path)):
        extracted_data = pd.concat([extracted_data, pd.read_csv(csv_file)], ignore_index=True)

    for json_file in glob.glob(r'{}/*.json'.format(files_path)):
        extracted_data = pd.concat([extracted_data, pd.read_json(json_file, lines=True)], ignore_index=True)

    for xml_file in glob.glob(r'{}/*.xml'.format(files_path)):
        extracted_data = pd.concat([extracted_data, pd.read_xml(xml_file)], ignore_index=True)

    return extracted_data

# Convert inches to meters and pounds to kilograms
def transform(data):
    data['height'] = round(data.height * 0.0254, 2)
    data['weight'] = round(data.weight * 0.45359237, 2)
    return data

# Loads data to a csv file:
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

# Records the log timestamp and message
def log_progress(message):
    timestamp = datetime.now(pytz.timezone('Europe/London')).strftime(r'%Y-%m-%d %H:%M:%S.%f')
    
    with open(log_file, 'a') as f:
        f.write('{} UTC, {}\n'.format(timestamp, message))


# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 
