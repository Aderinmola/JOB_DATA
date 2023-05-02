import pandas as pd
from utils import s3_upload, download_from_s3


def transform(file_name):
    list_of_items = [
                        'employer_website', 'job_id', 'job_employment_type', 'job_title', 
                        'job_apply_link', 'job_description', 'job_city', 'job_country', 
                        'job_posted_at_timestamp', 'employer_company_type'
                    ]
    
    df = pd.read_json(file_name)
    # print(df.loc[0, 'job_description'])

    new_df = df[list_of_items]
    new_df['job_posted_at_timestamp'] = pd.to_datetime(new_df['job_posted_at_timestamp'])

    new_df['job_description'] = new_df['job_description'].map(lambda row: "" + str(row) + "" )

    # saving the dataframe
    new_df.to_csv('transformed_job_data.csv', index=False)
    print("Data written into a file successfully")
