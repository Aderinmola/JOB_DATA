from extract import api_extract
from transform import transform
from load import load

from utils import s3_upload, download_from_s3
from dotenv import dotenv_values


# API credentials
config = dotenv_values()

BUCKET_NAME = config.get('BUCKET_NAME')
FILE_NAME = config.get('FILE_NAME')
OBJECT_NAME = config.get('OBJECT_NAME')

SECOND_BUCKET_NAME = config.get('SECOND_BUCKET_NAME')
SECOND_FILE_NAME = config.get('SECOND_FILE_NAME') 
SECOND_OBJECT_NAME = config.get('SECOND_OBJECT_NAME')

data_name = "job_data.json"

def main():
    # Extract the data
    # api_extract(data_name)

    # Upload and then download from s3
    s3_upload(FILE_NAME, BUCKET_NAME, OBJECT_NAME)
    download_from_s3(FILE_NAME, BUCKET_NAME, OBJECT_NAME)

    # Transform data
    transform(data_name)

    # Upload transormed data to the second bucket
    s3_upload(
      SECOND_FILE_NAME, 
      SECOND_BUCKET_NAME, 
      SECOND_OBJECT_NAME
    )

    # Copy data to Redshift
    load()

main()
