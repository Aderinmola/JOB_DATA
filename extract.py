import requests
import json

from dotenv import dotenv_values


# API credentials
config = dotenv_values()

api_key = config.get('RapidAPI_Key')

def api_extract():
# Data Engineer and Data Analyst jobs posted in either UK, Cannada or the US.
    url = "https://jsearch.p.rapidapi.com/search"


    headers = {
        "X-RapidAPI-Key": {api_key},
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    job_titles_and_location = [
        "Data Engineer in USA",
        "Data Engineer in UK", 
        "Data Engineer in Canada",
        "Data Analyst in USA",
        "Data Analyst in UK",
        "Data Analyst in Canada"
    ]
    acc_data = []
    for query in job_titles_and_location:
        querystring = {
            "query":query,
            "page":"1",
            "num_pages":"1",
            "date_posted":"today"
            }
        response = requests.get(url, headers=headers, params=querystring)
        acc_data.extend(response.json().get("data"))

    # Serializing json
    json_object = json.dumps(acc_data, indent=4)

    with open("job_data.json", "w") as f:
        f.write(json_object)
    print("Data written into a file successfully")

api_extract()
