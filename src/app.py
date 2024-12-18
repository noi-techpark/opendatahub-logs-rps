# --------------------------------------------------------------------------------------------------
#
# Note:
# --------------------------------------------------------------------------------------------------


import json
from elasticsearch import Elasticsearch, helpers
import os
from datetime import datetime, timedelta
from dateutil.parser import isoparse
import time
import sys
import generate
import request
import logging
logger = logging.getLogger(__name__)

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


def read_json_from(filename):
    query_file = open(filename, "r")
    query_body = json.load(query_file)
    query_file.close()
    return query_body


def write_into_file(filename, content):
    f = open(filename, "w")
    f.write(content)
    f.close()


def round_to_next_day_range(dt_str):
    if dt_str[-1] == "Z":
        dt, _, _ = dt_str.partition(".")
        datetime_object = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
        
        next_day_start = datetime_object.replace(hour=00, minute=00, second=00) + timedelta(days=1)
        date_start = next_day_start.isoformat() + ".000Z"
        
        next_day_end = next_day_start + timedelta(days=1)
        date_end = next_day_end.isoformat() + ".000Z"
        return date_start, date_end

    # datetime_obj = isoparse(dt_str)
    # print(datetime_obj)
    # next_day_start = datetime_obj.replace(hour=00, minute=00, second=00) + timedelta(days=1)
    # date_start = next_day_start.isoformat()

    # next_day_end = next_day_start + timedelta(days=1)
    # date_end = next_day_end.isoformat()
    # print(date_start, date_end)
    # return date_start, date_end


def round_to_next_hour_range(dt_str):
    if dt_str[-1] == "Z":
        dt, _, _ = dt_str.partition(".")
        datetime_object = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
        
        next_day_start = datetime_object.replace(minute=00, second=00) + timedelta(hours=1)
        date_start = next_day_start.isoformat() + ".000Z"
        
        next_day_end = next_day_start + timedelta(hours=1)
        date_end = next_day_end.isoformat() + ".000Z"
        return date_start, date_end
    


def format_data_with_referer(policy, api, json_data):
    # Elasticsearch API Query Attributes
    AGGREGATIONS = "aggregations"
    BUCKETS = "buckets"
    REFERER = "referer"
    
    buckets = json_data[AGGREGATIONS][REFERER][BUCKETS]
    
    time_list = []
    document_list = []
    for referer_bucket in buckets:
        referer = referer_bucket["key"]
        for ip_bucket in referer_bucket["ip_address"]["buckets"]:
            ip_address = ip_bucket["key"]
            for time_bucket in ip_bucket["requests_per_second"]["buckets"]:
                request_time = time_bucket["key_as_string"]
                time_list.append([request_time, ip_address])
                rps = time_bucket["doc_count"]
                last_update = datetime.now().isoformat()
                request = Bucket(api, policy, referer, ip_address, rps, request_time, last_update)
                # json_request = json.dumps(request.__dict__) 
                document_list.append(request.__dict__)
    if api == "timeseries":
        write_into_file("tempfile-referer-timeseries.json", str(time_list))
                
    return document_list


def format_data_without_referer(policy, api, json_data):
    # For Anonymous Policy type with no referer
    # Elasticsearch API Query Attributes
    AGGREGATIONS = "aggregations"
    BUCKETS = "buckets"
    IP_ADDRESS = "ip_address"
    
    buckets = json_data[AGGREGATIONS][IP_ADDRESS][BUCKETS]
    
    document_list = []
    for ip_bucket in buckets:
        ip_address = ip_bucket["key"]
        for time_bucket in ip_bucket["requests_per_second"]["buckets"]:
            request_time = time_bucket["key_as_string"]
            rps = time_bucket["doc_count"]
            last_update = datetime.now().isoformat()
            request = Bucket(api, policy, None, ip_address, rps, request_time, last_update)
            # json_request = json.dumps(request.__dict__) 
            document_list.append(request.__dict__)
    return document_list

    
def delete_generated_query_file(directory, filename):
    file = f"{directory}/{filename}"
    if os.path.exists(file):
        os.remove(file)
        logger.info("file \"%s\" deleted", filename)
    else:
        logger.info("file \"%s\" does not exist", filename)

def round_utc_date_to_next_day(dt_str):
    try:
        if dt_str[-1] == "Z":
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


def run_with_params(date, apis, policies, policies_without_referer, storage_index, API_KEY, ELASTIC_URL):

    client = Elasticsearch(ELASTIC_URL, api_key=API_KEY)
    
    date_start = set_start_date(date, storage_index, client)
    
    date_end = add_days_to_date(1, date_start)
    
    # _, date_end = round_to_next_day_range(date)
    ## change to round_to_next_day_range if running once, at the end of the day

    for api in apis:
        for policy in policies:
            generate.query_file(date_start, date_end, policy, api)
    
    query_folder = "../generated-query-files"
    aggregation_folder = "../aggregation-files"
    generated_queries = os.fsencode(query_folder)
    
    for file in os.listdir(generated_queries): 
        query_filename = os.fsdecode(file)

        try:
            query_body = read_json_from(f"{query_folder}/{query_filename}")
            
            api, policy = query_filename.replace(".", "-").split("-")[-3:-1]
            policy = policy.replace("_", " ")
            
            if policy in policies_without_referer:
                agg_filename = "timeseries-content-without-referer.json"
                aggregation = read_json_from(f"{aggregation_folder}/{agg_filename}")
                logger.info("%s - %s - prepare query and aggregation without referer", api, policy)
            else:
                agg_filename = "timeseries-content-with-referer.json"
                aggregation = read_json_from(f"{aggregation_folder}/{agg_filename}")
                logger.info("%s - %s - prepare query and aggregation with referer", api, policy)
            
            response = request.extract_aggregated_data(client, query_body, aggregation)
            if response == None:
                delete_generated_query_file(query_folder, query_filename)
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
            
            # delete_generated_query_file(query_folder, query_filename)
            # logger.info("deleted json query file \"%s\"", query_filename)
        
        except Exception as e:
            print(e, query_filename)
            logger.error("%s: %s", e, query_filename)


            
            