# --------------------------------------------------------------------------------------------------
#
# Notes:
# - run_with_params(): runs the main logic; each run retrieves the logs from the entire day
#   1. Set the start and end dates of query, based on set_start_date() 
#   2. Loop through all specified APIs and policy types configured in main.py 
#       to generate corresponding query files with reference to query templates
#   3. For each generated query file
#       - combine with corresponding aggregation file (differs based on policy type)
#       - extract data from elasticsearch logs index
#       - format extracted data depending on policy type
#       - store processed data into elasticsearch storage index
#       - delete generated query files
#
# Other Functions:
# - set_start_date(): returns the date that script will be ran on, based on
#   1. latest date in index
#   2. input date in main.py
#   3. default date "2024-11-01T00:00:00.000Z"
# - round_utc_date_to_next_day(): takes in date in UTC format and round it to the next day
# - validate_date(): checks that the date is in UTC format
# - add_days_to_date(): add input number of days into input date
# - delete_file(): delete file from specified directory
# - format_data_with_referer(): format data buckets by referer, ip address then requests per second
# - format_data_without_referer(): format data buckets by ip address then requests per second
# - read_json_from_file(): read json data from file
# --------------------------------------------------------------------------------------------------


import json
from elasticsearch import Elasticsearch, helpers
import os
from datetime import datetime, timedelta, timezone
import re
import sys
import generate
import request
import logging
logger = logging.getLogger(__name__)

############# Constants ###################
## Elasticsearch API Response Attributes ##
AGGREGATIONS = "aggregations"
BUCKETS = "buckets"
REFERER = "referer"
IP_ADDRESS = "ip_address"

class Bucket:
    def __init__(self, api, policy, referer, ip_address, request_per_second, request_time, last_update):
        self.api = api
        self.policy = policy
        self.referer = referer
        self.ip_address = ip_address
        self.request_per_second = request_per_second
        self.request_time = request_time
        self.last_update = last_update


def read_json_from_file(filename):
    query_file = open(filename, "r")
    query_body = json.load(query_file)
    query_file.close()
    return query_body


def format_data_with_referer(policy, api, json_data):
    buckets = json_data[AGGREGATIONS][REFERER][BUCKETS]
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
                document_list.append(request.__dict__)      
    return document_list


def format_data_without_referer(policy, api, json_data):
    buckets = json_data[AGGREGATIONS][IP_ADDRESS][BUCKETS]
    document_list = []
    for ip_bucket in buckets:
        ip_address = ip_bucket["key"]
        for time_bucket in ip_bucket["requests_per_second"]["buckets"]:
            request_time = time_bucket["key_as_string"]
            rps = time_bucket["doc_count"]
            last_update = datetime.now().isoformat()
            request = Bucket(api, policy, None, ip_address, rps, request_time, last_update)
            document_list.append(request.__dict__)
    return document_list

    
def delete_file(directory, filename):
    file = f"{directory}/{filename}"
    if os.path.exists(file):
        os.remove(file)
        logger.info("file \"%s\" deleted", filename)
    else:
        logger.info("file \"%s\" does not exist", filename)


def validate_date(date):
    if date != "":
        pattern_str = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$'
        if not re.match(pattern_str, date):
            raise Exception("date not written in UTC format")
        

def round_utc_date_to_next_day(dt_str):
    try:
        validate_date(dt_str)
        dt, _, _ = dt_str.partition(".")
        datetime_object = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
        next_day_start = datetime_object.replace(hour=00, minute=00, second=00) + timedelta(days=1)
        rounded_date = next_day_start.isoformat() + ".000Z"
        return rounded_date
    except Exception as e:
        raise Exception("error in rounding date: ", e)


def set_start_date(date, storage_index, client):
    lastest_date = request.get_latest_record(client, storage_index)
    if lastest_date == None:
        if date == "":
            date = "2024-11-01T00:00:00.000Z"
        return date
    next_day = round_utc_date_to_next_day(lastest_date)
    if date != "":
        if date > next_day:
            return date
    return next_day


def add_days_to_date(input_days, dt_str):
    try:
        if dt_str[-1] == "Z":
            dt, _, _ = dt_str.partition(".")
            datetime_object = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
            next_day = datetime_object + timedelta(days=input_days)
            utc_date = next_day.isoformat() + ".000Z"
            return utc_date
    except Exception as e:
        raise Exception("error in adding days: ", e)
    
    
def validate_date_passed(date):
    current_datetime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    if current_datetime > date:
        return True
    return False
    

def run_with_params(date, logs_index, storage_index, apis, policies_with_referer, policies_without_referer, API_KEY, ELASTIC_URL):

    client = Elasticsearch(ELASTIC_URL, api_key=API_KEY)
    
    date_start = set_start_date(date, storage_index, client)
    date_end = add_days_to_date(1, date_start)
    
    if not validate_date_passed(date_end):
        raise Exception("end date is later than current date")

    policies = policies_with_referer + policies_without_referer

    for api in apis:
        for policy in policies:
            generate.query_file(date_start, date_end, policy, api)
    
    query_folder = "../generated-query-files"
    aggregation_folder = "../aggregation-files"
    generated_queries = os.fsencode(query_folder)
    
    for file in os.listdir(generated_queries): 
        query_filename = os.fsdecode(file)

        try:
            query_body = read_json_from_file(f"{query_folder}/{query_filename}")
            
            api, policy = query_filename.replace(".", "-").split("-")[-3:-1]
            policy = policy.replace("_", " ")
            
            if policy in policies_without_referer:
                agg_filename = "timeseries-content-without-referer.json"
                aggregation = read_json_from_file(f"{aggregation_folder}/{agg_filename}")
                logger.info("%s - %s - prepare query and aggregation without referer", api, policy)
            else:
                agg_filename = "timeseries-content-with-referer.json"
                aggregation = read_json_from_file(f"{aggregation_folder}/{agg_filename}")
                logger.info("%s - %s - prepare query and aggregation with referer", api, policy)
            
            response = request.extract_aggregated_data(client, query_body, aggregation, logs_index)
            if response == None:
                delete_file(query_folder, query_filename)
                logger.info("deleted json query file \"%s\"", query_filename)
                logger.info("continue to the next file")
                continue
                    
            if policy in policies_without_referer:
                document_list = format_data_without_referer(policy, api, response)
            else:
                document_list = format_data_with_referer(policy, api, response)
            
            number_of_buckets = len(document_list)
            logger.info("%s - %s - %d number of buckets extracted from query in %s", api, policy, number_of_buckets, query_filename)
            logger.info("size of document list: %s bytes", sys.getsizeof(document_list))
            
            # response = helpers.bulk(client, document_list, index=storage_index)
            # logger.info("saved into %s index with response: %s", storage_index, response)
            
            delete_file(query_folder, query_filename)
            logger.info("deleted json query file \"%s\"", query_filename)
        
        except Exception as e:
            logger.error("%s: %s", e, query_filename)


            
            