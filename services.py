import json
import pandas as pd
import psycopg2
import openai
import os
from flask import jsonify
import pandas as pd
import json
from scipy.stats import pearsonr
import matplotlib.pyplot as plt

try:
    test_api_key = os.getenv('API_KEY')
    print("test_api_key " + test_api_key)
except:
    print("")
finally:
    pass

openai.api_key  = "rajat"

class QueryService:
    @staticmethod
    def get_all_users():
        query = """SELECT communication_code, communication_name, communication_category, template_name FROM public.campaign_metadata limit 5;"""
        return getData(query)

    @staticmethod
    def get_all_users_kafka():
        query = """SELECT communication_code, communication_name, communication_category, template_name FROM public.campaign_metadata limit 5;"""
        return getData(query)

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
    return df
@staticmethod
def get_datasets():
    datasets={
        "database": [
            { "id": 1, "type": "database", "name": "email_campaign_uber" },
            { "id": 2, "type": "database", "name": "email_user_uber" },
            { "id": 3, "type": "database", "name": "push_campaign_uber" },
            { "id": 4, "type": "database", "name": "push_user_uber" },
            { "id": 5, "type": "database", "name": "campaign_metadata" },
            { "id": 6, "type": "database", "name": "email_user_table_stats" }
        ],
        "s3": [
            { "id": 1, "type": "s3", "name": "omp_email_user_master" },
            { "id": 2, "type": "s3", "name": "omp_audience_engagement_data_campaign_level_domain_event_v2" }
        ],
        "kafka": [
            { "id": 1, "type": "kafka", "name": "teal_segmentation_history_v3" },
            { "id": 2, "type": "kafka", "name": "customer_profile_enhanced_v4" }
        ]
    }
    return datasets


@staticmethod
def df_to_json(df):
    # Convert the DataFrame to a JSON string
    json_string = df.to_json(orient='records')

    # Convert the JSON string to a Python dictionary
    data = json.loads(json_string)

    # Print the JSON data
    jsondata = json.dumps(data, indent=4)
    return jsondata

def get_completion(system_prompt, user_prompt, model="gpt-4o", temperature : float = 0.0):
    messages = [{"role": "system",
                 "content": system_prompt},
                {"role": "user",
                 "content": user_prompt}]
    response = openai.chat.completions.create(
        model=model,
        messages=messages,
        temperature= temperature, # degree of randomness/ creativity of the model's output
    )
    # Extract the completion content
    completion_content = response.choices[0].message.content

    # Extract token usage information
    token_usage = response.usage

    return completion_content, token_usage


# Give system prompt here which explains the context
system_prompt = """ You are a Data Analyst, proficient in python and other languages. Read the data description and sample carefully from the tables to identify the correct data columns mentioned in the question from the correct tables to answer the question. If the question requires data from multiple tables based on the attributes available in each, join the tables on the relevant columns. 

### Metadata for Tables

#### email_campaign_uber
- **Table Description**: Reports metrics at campaign and sent_date grain for email channel. It is aggregated version of email_user_uber.
- **Columns**:
  - **brand**: String, Brand to which the communication caters
  - **campaign_code**: String, Communication code, non-obfuscated
  - **pos**: String, Point of sale (similar to Country)
  - **sent_date**: Date, Date of communication being sent to user (in UTC)
  - **room_night_count**: Double, Room nights booked (excluding cancellations), aggregated at campaign grain
  - **OKCC_approved**: Double, One key credit card approved events, TIA attributed, aggregated at campaign grain
  - **OKCC_applied**: Double, One key credit card applied events, TIA attributed, aggregated at campaign grain
  - **xft_orders**: Double, How many orders are attributed to a user based on the XFT model, aggregated at campaign grain
  - **xft_gp**: Double, How much GP is attributed to a user based on the XFT model, aggregated at campaign grain
  - **SSLT_orders**: Double, How many orders are attributed to a user based on the SSLT model, aggregated at campaign grain
  - **SSLT_gp**: Double, How much GP is attributed to a user based on the SSLT model, aggregated at campaign grain

#### email_user_uber
- **Table Description**: Reports metrics at recipient_id and sent_date grain for email channel. It is aggregated at campaign level to form email_campaign_uber.
- **Columns**:
  - **campaignname**: String, Communication code, non-obfuscated
  - **recipient_id**: String, User identifier, uniquely identifies user within a brand
  - **brand**: String, Brand to which the communication caters
  - **sent_date**: Date, Date of communication being sent to user (in UTC)
  - **lang_id**: String, Derived from email omni code, often referred to as locale
  - **teal_segment**: String, Segment name at the time of sent
  - **delivered**: Int, Distinct count of successfully delivered emails
  - **clicks**: Int, Count of recorded clicks events
  - **unsubscribes**: Int, Count of recorded unsubscribe events
  - **medium_influenced_gbv**: Double, How much GBV is attributed to a user based on the Influenced model
  - **medium_influenced_gp**: Double, How much GP is attributed to a user based on the Influenced model

#### push_campaign_uber
- **Table Description**: Reports metrics at campaign and sent_date grain for push channel. It is aggregated version of push_user_uber.
- **Columns**:
  - **brand**: String, Brand to which the communication caters
  - **campaign_code**: String, Communication code, non-obfuscated
  - **pos**: String, Point of sale (similar to Country)
  - **sent_date**: Date, Date of communication being sent to user (in UTC)
  - **room_night_count**: Double, Room nights booked (excluding cancellations), aggregated at campaign grain
  - **Medium_influenced_orders**: Double, How many orders are attributed to a user based on the Influenced model, aggregated at campaign grain
  - **Medium_influenced_NBV**: Double, How much NBV is attributed to a user based on the Influenced model, aggregated at campaign grain
  - **SSLT_orders**: Double, How many orders are attributed to a user based on the SSLT model, aggregated at campaign grain
  - **SSLT_NBV**: Double, How much NBV is attributed to a user based on the SSLT model, aggregated at campaign grain
  - **device_unsubscribe**: Double, Count of users who received the push communication and unsubscribed from the device, aggregated at campaign grain

#### push_user_uber
- **Table Description**: Reports metrics at recipient_id and sent_date grain for push channel. It is aggregated at campaign level to form push_campaign_uber. It is similar to email_user_uber but for push channel.
- **Columns**:
  - **campaignname**: String, Communication code, non-obfuscated
  - **recipient_id**: String, User identifier, uniquely identifies user within a brand
  - **brand**: String, Brand to which the communication caters
  - **sent_date**: Date, Date of communication being sent to user (in UTC)
  - **teal_segment**: String, Segment name at the time of sent
  - **delivered**: Int, Distinct count of successfully delivered push notifications
  - **opens**: Int, Count of notifications directly opened by the user
  - **partial_unsubscribe**: Int, User unsubscribing from some but not all available opted-in categories in 24 hours after receiving campaign
  - **total_unsubscribe**: Int, User unsubscribing from all available opted-in categories in 24 hours after receiving campaign
  - **segment_progression_tia**: Double, Segment changes that took place post the campaign was sent
  - **total_orders**: Double, Total orders/transactions placed

#### campaign_metadata
- **Table Description**: Reports metadata for a campaign at campaign name, sent_date, send_time, locale, and brand. It has data for both email and push channels. It can be associated with all uber tables using communication_code, brand, and send_date.
- **Columns**:
  - **communication_code**: String, Communication Code is part of the EMLCID and will be unique to each communication and will not be shared across multiple communications in addition to the Comm ID.
  - **communication_name**: String, Communication name is used for user to enter to create a user-friendly name for tracking and reporting purposes but will NOT be part of EMLCID. This is NOT the same as the Comm ID and Comm Code.
  - **communication_category**: String, Usually categorizes a communication into Transactional or Marketing
  - **template_name**: String, Name of the communication template used from Delivery Provider as defined in Communications Manager
  - **audience_segment_name**: String, Name of the Audience Segment used from EG Audience Builder as defined in Communications Manager
  - **sent_date**: Date, EG Standard Send date
  - **locale**: String, Locale: Language + POS
  - **communication_goal**: String, Communication Goal defined as:
    - 12m Active 1st booker progression
    - 12m Active Member cross brand earn/burn
    - 2nd Booker growth
    - Active member growth
    - At-risk member reactivation
    - Credit Card membership
    - Drive 1st Redemption Rate
    - Gift Card Activation
    - Increase NBV per member
    - Member App Users
    - Members attach/cross-sell
    - Members booking > trip elements
    - MOD transactions
    - Monthly active members
    - New active member sign up
    - Owned App Installs
    - Prospect member join
    - Prospect Shopping Activation
    - Travelers shopping cross Brand
    - Travelers shopping cross LOB
    - Winback
    - Generic
  - **send_time**: Timestamp, EG Standard Send Time for the communication
  - **send_date_local**: Date, Send date in the local POS timezone
  - **brand**: String, Brand to which the communication caters


#### email_user_table_stats
- **Table description** : Reports metrics at recipient_id and sent_date grain for email channel.
- **Columns**:
    - **recipient_id**: String, User identifier, uniquely identifies user within a brand
    - **brand**: String, Brand to which the comm caters
    - **sent_date**: Date, Date of communication being sent to user (in UTC)
    - **email_omni_code**: String, An email omni code having all the details of campaign/brand/subchannel/date/pos/locale/various id's to identify a communication uniquely. 
    - **medium_influenced_gp**: Double, How much GP i.e. gross profit ,is attributed to a user based on the Influenced model
    - **medium_influenced_nbv**: Double, How much NBV i.e. net booking value ,is attributed to a user based on the Influenced model
    - **room_night_count**: Double, Room nights booked (excluding cancellations)
    - **clicks**: Int, Count of recorded clicks events
    - **platform_identifier**: String, indicates platform using which campaign are sent. It can have 2 values - UMP and SF.


As the 1st part of the response, please provide a valid PostgreSQL query, to retrieve the required information from the tables in prostgres DB without any errors. Strictly DO NOT use Aliases for columns instead only use appropriate column names in the whole query. Ensure to use having clause when using group by instead of where clause. Additionally, remember to limit the number of records in the query to the most relevant number based on the question. If date conditions are required, use the `current_date` function to compare dates in the query.

As the 2nd part of the response, provide Python code to extract the data from the query result and return the JSON string named 'graph_json' to be used in a React-based chart.cjs module. Assume that the query result is available in a Pandas DataFrame named `result_df`. For statistical analysis, use the base data directlty instead of `result_df`. For statitical analysis, clean the data which should include dealing with all nulls to ensure statistical analysis work properly. The code should be structured based on the number of columns in the select query. If the DataFrame contains date columns, then please ensure that you first convert these date columns to a string type using appropriate functions. For cases where statitical analysis is to be done, provide python code for statiscal analysis as well. Perform this operation for all date columns in the result_df. Decide on the type of graph based on the question and provide appropriate code to gnereate the json for the specific graph types. If there are multiple data points that can be plotted, ONLY then provide the JSON generation code, if there is only one data point in result data, just return empty json string. The returned JSON string should follow the structure of the example below:

sampleGraphData = {
  labels: ['January', 'February', 'March'], // Graph labels
  datasets: [
    { 
      type: 'line',
      label: 'Line Dataset',
      data: [30, 70, 100], 
      borderColor: 'rgba(75,192,192,1)',
      backgroundColor: 'rgba(75,192,192,0.2)',
      fill: true
    },
    { 
      type: 'bar',
      label: 'Bar Dataset',
      data: [10, 20, 30], 
      backgroundColor: 'rgba(153,102,255,0.6)',
    }, 
    {
      type: 'scatter',
      label: 'Scatter Dataset',
      data: [
        { x: "January", y: 20 },
        { x: "February", y: 5 },
        { x: "February", y: 45 },
        { x: "January", y: 35 }
      ],
      backgroundColor: 'rgba(255,99,132,0.6)'
    }
  ]
};

As the 3rd part of the response, generate a descriptive text answer to the question, with placeholders '<>' in place to add the data from the query response. Insert newline characters wherever required to make the answer more appealing and engaging. Format the answer as a Python string and include the code to extract the data from the DataFrame and populate the text string. Make sure to list down the datasets used to find the answer at the end of the answer as a note. Provide the answer in a variable named 'answer_text'.

General Instructions: 
Only use small case characters for naming columns and variables. 
Make sure to import all the external libraries used so that there are no errors.
If this is a follow-up question based on the Follow-up Question Flag value, please consider the original query and the context provided. Use the original query as part of the prompt to ensure continuity and coherence in the response.

Provide the different code blocks for all three parts strictly within triple quotes (''') for easy extraction. Please use the following format:

'''
<1st part of the response>
'''

'''
<2nd part of the response>
'''

'''
<3rd part of the response>
'''

Expect the below Data points as part of the user prompt: 
"Follow-up Question Flag" : "Y or N"
"Question": "<Question in Natural Language>"
"Original Query": "<Original query if this is a follow-up question>"
"Dataset" : "<Datasets to be considered>"

"""
@staticmethod
def get_llm_response(question, follow_up_flag, original_query, dataset):
    user_prompt = f"Question: {question}\nFollow-up: {follow_up_flag}\nOriginal Query: {original_query}\nDataset: {dataset}"
    completion, tokens = get_completion(system_prompt, user_prompt)
    print("Completion:", completion)
    print("Tokens used:", tokens)
    return completion

@staticmethod
def extract_code_blocks(text):
    code_blocks = []
    start = 0
    while True:
        start = text.find("'''", start)
        if start == -1:
            break
        end = text.find("'''", start + 3)
        if end == -1:
            break
        code_blocks.append(text[start + 3:end].strip())
        start = end + 3
    return code_blocks

@staticmethod
def process_request(data):
    # Access elements from the JSON data
    question = data.get('question')
    original_query = data.get('original_query')
    dataset = data.get('dataset')
    if original_query:
        follow_up_flag = 'Y'
    else:
        follow_up_flag = 'N'
    # Assuming you have a method to create a new user
    response_text = get_llm_response(question, follow_up_flag, original_query, dataset)
    blocks = extract_code_blocks(response_text)
    result_df = getData(blocks[0])
    result_json = df_to_json(result_df)
    print("result_json 1: " + result_json)
    scope={'result_df': result_df}
    #graph_json = None
    exec(blocks[1], scope)
    print("block 1: " + blocks[1])
    print("graph_json"+scope['graph_json'])
    #answer_text = None
    exec(blocks[2], scope)
    print("block 2: " + blocks[2])
    print("answer_text"+scope['answer_text'])
    return jsonify({'query': blocks[0],'table_data': result_json, 'graph_json': scope['graph_json'], 'answer_text': scope['answer_text']}), 201
