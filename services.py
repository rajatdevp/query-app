import json
import pandas as pd
import psycopg2

class QueryService:
    @staticmethod
    def get_all_users():
        query = """SELECT communication_code, communication_name, communication_category, template_name FROM public.campaign_metadata limit 5;"""
        return getData(query)

    @staticmethod
    def get_all_users_kafka():
        query = """SELECT communication_code, communication_name, communication_category, template_name FROM public.campaign_metadata limit 5;"""
        return getData(query)

def getData(queryString):
    conn = psycopg2.connect(
        host="54.236.21.177",
        database="communications",
        user="postgres",
        password="expediapostgres"
    )

    cur = conn.cursor()

    # Example query
    cur.execute(queryString)
    data = cur.fetchall()

    # Get column names
    colnames = [desc[0] for desc in cur.description]

    # Create DataFrame
    df = pd.DataFrame(data, columns=colnames)

    cur.close()
    conn.close()

    # Convert the DataFrame to a JSON string
    json_string = df.to_json(orient='records')

    # Convert the JSON string to a Python dictionary
    data = json.loads(json_string)

    # Print the JSON data
    jsondata = json.dumps(data, indent=4)
    return jsondata
