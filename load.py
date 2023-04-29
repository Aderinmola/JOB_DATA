import redshift_connector
from dotenv import dotenv_values

# Database credentials
config = dotenv_values()

host = config.get('host')
database = config.get('database')
user = config.get('user')
password = config.get('password')

conn = redshift_connector.connect(
    host=host,
    database=database,
    user=user,
    password=password
)

conn.autocommit = True
cursor: redshift_connector.Cursor = conn.cursor()

cursor.execute("""
CREATE TABLE "job_data" (
  "employer_website" TEXT,
  "job_id" TEXT,
  "job_employment_type" TEXT,
  "job_title" TEXT,
  "job_apply_link" TEXT,
  "job_description" VARCHAR(MAX),
  "job_city" TEXT,
  "job_country" TEXT,
  "job_posted_at_timestamp" TIMESTAMP,
  "employer_company_type" TEXT
)
""")

cursor.execute("""
copy job_data from 's3://transformed-jobs-data-derin/transformed_jobs_data.csv'
iam_role 'arn:aws:iam::271681604384:role/sayo-redshift-role'
delimiter ','
csv quote as '"'
region 'us-west-1'
IGNOREHEADER 1
""")

print ("Cpied data from s3 to redshift successfully!!!")
