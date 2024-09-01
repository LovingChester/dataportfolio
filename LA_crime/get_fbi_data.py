'''
This script fetches data from FBI's API https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi
and outputs a csv file. Then, it uploads the file to AWS S3.
'''

from tqdm import tqdm
import requests
import pandas as pd
import numpy as np
import boto3

# Get all agencies in CA
url = "https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/CA"
params = {
    "API_KEY": "iiHnOKfno2Mgkt5AynpvPpUQTEyxE77jo1RU8PIv"
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    print("Response successful")
else:
    print(f"Request failed with status code: {response.status_code}")

# Get all unique agency identifier
oris = list(map(lambda x : x['ori'], data))

offense_types = ['motor-vehicle-theft', 'aggravated-assault', 'violent-crime', 'robbery', 'arson', 'human-trafficing', 'rape-legacy', 
                 'homicide', 'burglary', 'larceny', 'property-crime']

url = "https://api.usa.gov/crime/fbi/cde/summarized/agency/"
params = {
     "from": 2020,
     "to": 2022,
     "API_KEY": "iiHnOKfno2Mgkt5AynpvPpUQTEyxE77jo1RU8PIv"
}

FBI_data = []

for ori in tqdm(oris):
    for offense_type in offense_types:
        full_url = url + ori + '/' + offense_type
        response = requests.get(full_url, params=params)

        if response.status_code == 200:
            data = response.json()
            if len(data) == 0: continue
            row = list(map(lambda x : [x['data_year'], x['offense'], x['cleared'], x['actual']], data))
            FBI_data.append(row)
        else:
            print(f"Request failed with status code: {response.status_code}")

FBI_data = np.vstack(FBI_data)
FBI_df = pd.DataFrame(FBI_data, columns=['YEAR', 'FBI_OFFENSE', 'CLEARED', 'ACTUAL'])
FBI_df.to_csv('FBI_data.csv', index=False)

s3 = boto3.client('s3')
bucket = 'edward-s3-crime-v1'
s3.upload_file(Filename='./FBI_data.csv', Bucket=bucket, Key='fbi/FBI_data.csv')