import pandas as pd
from utils import s3_upload, download_from_s3


BUCKET_NAME = "raw-jobs-data-derin"
FILE_NAME = "job_data.json"
OBJECT_NAME = "raw_job_data.json"

s3_upload(FILE_NAME, BUCKET_NAME, OBJECT_NAME)
download_from_s3(FILE_NAME, BUCKET_NAME, OBJECT_NAME)

def transform():
    list_of_items = [
                        'employer_website', 'job_id', 'job_employment_type', 'job_title', 
                        'job_apply_link', 'job_description', 'job_city', 'job_country', 
                        'job_posted_at_timestamp', 'employer_company_type'
                    ]
    
    df = pd.read_json("job_data.json")
    # print(df.loc[0, 'job_description'])

    new_df = df[list_of_items]
    new_df['job_posted_at_timestamp'] = pd.to_datetime(new_df['job_posted_at_timestamp'])

    new_df['job_description'] = new_df['job_description'].map(lambda row: "" + str(row) + "" )

    # saving the dataframe
    new_df.to_csv('transformed_job_data.csv', index=False)
    print("Data written into a file successfully")

transform()

BUCKET_NAME = "transformed-jobs-data-derin"
FILE_NAME = "transformed_job_data.csv" 
OBJECT_NAME = "transformed_jobs_data.csv"

s3_upload(FILE_NAME, BUCKET_NAME, OBJECT_NAME)

