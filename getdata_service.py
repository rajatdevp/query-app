import json
import pysa

class QueryService:

    @staticmethod
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
