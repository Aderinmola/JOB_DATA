import redshift_connector
from dotenv import dotenv_values

# Redshift credentials
config = dotenv_values()

host = config.get('host')
database = config.get('database')
user = config.get('user')
password = config.get('password')

# Redshift credentials
copy_path = config.get('copy_path')
role = config.get('role')

def load():
  conn = redshift_connector.connect(
      host=host,
      database=database,
      user=user,
      password=password
  )

  conn.autocommit = True
  cursor: redshift_connector.Cursor = conn.cursor()

  cursor.execute("""
  CREATE TABLE IF NOT EXISTS "job_data" (
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

  cursor.execute(f"""
  copy job_data from '{copy_path}'
  iam_role '{role}'
  delimiter ','
  csv quote as '"'
  region 'us-west-1'
  IGNOREHEADER 1
  """)

  print ("Copied data from s3 to redshift successfully!!!")
