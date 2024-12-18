# --------------------------------------------------------------------------------------------------
#
# Note: request.py handles all external request with Elasticsearch
#
# extract_aggregated_data:
#       - extract buckets from elastic search index based on query and aggregation types
#
# get_latest_record:
#       - retrieve the latest record if it exists
#       - if no reacord in index, return None
#
# --------------------------------------------------------------------------------------------------


import logging
logger = logging.getLogger(__name__)


############################################


def extract_aggregated_data(client, query_body, aggregation):
    
    filebeat = "filebeat-*"
    
    try:
        json_data = client.search(index=filebeat, query=query_body, aggs=aggregation, size=0)
        # size=0 so that original records are not queried
    except Exception as e:
        raise Exception("error in retrieving aggregated data", e)
    
    if json_data["hits"]["total"]["value"] == 0:
        logger.info("there is no matching data in %s index", filebeat)
        return None
    
    return json_data


def get_latest_record(client, index_name):
    
    try:
        json_data = client.search(
            index=index_name, 
            size=1, 
            query={"match_all": {}}, 
            sort={
                "request_time": {
                    "order": "desc"
                }
            }
        )
    except Exception as e:
        raise Exception("error in retrieving latest record")
    
    if json_data["hits"]["total"]["value"] == 0:
        logger.info("no existing record in %s index", index_name)
        return None
    
    latest_request_time = json_data["hits"]["hits"][0]["_source"]["request_time"]
    logger.info("latest request time in %s index is %s", index_name, latest_request_time)
    
    return latest_request_time