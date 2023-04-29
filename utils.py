import boto3
from dotenv import dotenv_values

# Database credentials
config = dotenv_values()

AWS_ACCESS_KEY = config.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY = config.get('AWS_SECRET_KEY')
AWS_REGION = config.get('AWS_REGION')


s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

def s3_upload(FILE_NAME, BUCKET_NAME, OBJECT_NAME): 
    with open(FILE_NAME, "rb") as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, OBJECT_NAME)
    print("File written into s3 successfully")

def download_from_s3(FILE_NAME, BUCKET_NAME, OBJECT_NAME):
    with open(FILE_NAME, "wb") as f:
        s3_client.download_fileobj(BUCKET_NAME, OBJECT_NAME, f)
    print("File downloaded from s3 successfully!!!")
