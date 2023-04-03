import json
import requests
import boto3
import pandas as pd
from io import StringIO

def upload_files(s3, bucket_name, url_dict):
    for object_key, url in url_dict.items():
        # Download the file from the URL
        response = requests.get(url)
        # Convert the content to bytes
        content = bytes(response.text, 'utf-8')
        # Upload the file to S3
        s3.put_object(Body=content, Bucket=bucket_name, Key=object_key)
        
        print(f'CSV file {object_key} uploaded to s3://{bucket_name}')
    

def upload_income_table(s3, bucket_name):
    # scrape income data from wiki
    url = 'https://en.wikipedia.org/wiki/List_of_California_locations_by_income'
    object_key = 'income/ca-family-income-2014.csv'
    tables = pd.read_html(url)
    income = tables[1]
 
    # step 1 rename columns
    income.columns = ['County', 'Population', 'Population_density', 
                      'Per_captia_income', 'Median_household_income','Median_family_income']
    
    # step 2 remove currency symbol and comma
    cols = ['Per_captia_income', 'Median_household_income','Median_family_income']
    income[cols] = income[cols].replace({'\$': '', ',': ''}, regex=True)

    # convert the table data to CSV format
    csv_buffer = StringIO()
    income.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue().encode('utf-8')
    
    # upload the CSV file to S3
    s3.put_object(Body=csv_content, Bucket=bucket_name, Key=object_key)
    
    print(f'Income data uploaded to s3://{bucket_name}/{object_key}')
    

def lambda_handler(event, context):
    # destination bucket name
    bucket_name = 'california-data-test'
    # create the s3 client
    s3 = boto3.client('s3')
    
    # get population and hospitalization data from urls
    # in the dict, key is the destination file name in s3, value is the source url of the file
    url_dict = {
        'population/ca-county-2010-2020-population.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/asrh/CC-EST2020-AGESEX-06.csv',
        'hospitalization/ca-hospitalization-counts-adverse-events.csv': 'https://data.chhs.ca.gov/dataset/9638e316-763e-4f69-b827-e9aba51c1f33/resource/d08f328e-0cd9-4df4-92f2-99ae5261b50a/download/ca-oshpd-adveventhospitalizationspsi-county2005-2015q3.csv',
    }
    
    # upload hospitalization and population data to S3
    upload_files(s3, bucket_name, url_dict)
    # scrape and upload income data to s3
    upload_income_table(s3, bucket_name)

if __name__ == "__main__":
    lambda_handler(None, None)
