# --------------------------------------------------------------------------------------------------
#
# Note:
# --------------------------------------------------------------------------------------------------


import urllib.request
import json
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os
from datetime import datetime
import sys


######## Load Environment Variables ########

load_dotenv()

API_KEY = os.getenv("API_KEY")
ELASTIC_URL = os.getenv("ELASTIC_URL")

############################################

class Bucket:
    def __init__(self, api, policy, referer, ip_address, request_per_second, request_time, last_update):
        self.api = api
        self.policy = policy
        self.referer = referer
        self.ip_address = ip_address
        self.request_per_second = request_per_second
        self.request_time = request_time
        self.last_update = last_update
    
# def get_records(query_data):
#     req = urllib.request.Request(ELASTIC_URL, data=query_data, method='GET')
#     req.add_header('kbn-xsrf', 'reporting')
#     req.add_header('Content-Type', 'application/json')
#     req.add_header('Authorization', f'ApiKey {API_KEY}')
#     print(query_data)

#     with urllib.request.urlopen(req) as response:
#         data = response.read()
#     json_data = json.loads(data.decode('utf-8'))
#     return json_data


def read_json_from(filename):
    query_file = open(filename, "r")
    query_body = json.load(query_file)
    query_file.close()
    return query_body


def main():
    
    client = Elasticsearch(ELASTIC_URL, api_key=API_KEY)
    
    
    # Can make it more reusable by making a template
    policy = 'referer'
    api = 'timeseries'
    query_body = read_json_from("query.json")
    aggregation = read_json_from("aggregation.json")
    json_data = client.search(index="filebeat-*", query=query_body, aggs=aggregation, size=0)
    # size=0 so that original records are not queried
    
    # Elasticsearch API Query Attributes
    AGGREGATIONS = "aggregations"
    BUCKETS = "buckets"
    
    buckets = json_data[AGGREGATIONS][policy][BUCKETS]
    
    # print(buckets)
    # return
    
    document_list = format_data(policy, api, buckets)

    print("Number of buckets: ", len(document_list))
    print("Size of list: ", sys.getsizeof(document_list), " bytes")
    
    # response = helpers.bulk(client, document_list, index="request-rate")
    # print("Response: ", response)


def format_data(policy, api, buckets):
    document_list = []
    for referer_bucket in buckets:
        referer = referer_bucket["key"]
        for ip_bucket in referer_bucket["ip_address"]["buckets"]:
            ip_address = ip_bucket["key"]
            for time_bucket in ip_bucket["requests_per_second"]["buckets"]:
                request_time = time_bucket["key_as_string"]
                rps = time_bucket["doc_count"]
                last_update = datetime.now().isoformat()
                request = Bucket(api, policy, referer, ip_address, rps, request_time, last_update)
                # json_request = json.dumps(request.__dict__) 
                document_list.append(request.__dict__)
    return document_list

    
main()
