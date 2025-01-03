# --------------------------------------------------------------------------------------------------
#
# Note: 
# - generate.py contains functions that help to generate json query files
#      
# Functions:
# - query_file(): called by external functions to generate query file based on query template
#       - reads template query file based on `api`
#       - changes the attributes based on the input parameters (`date_start`, `date_end`, `policy`)
#       - takes into account the following fields
#          - start date
#          - end date
#          - policy type
#          - API type
# - generate_by_policy_type: adds policy type into json
# - read_json_from_file(): read json data from file
# --------------------------------------------------------------------------------------------------

import json
from pathlib import Path
import logging
logger = logging.getLogger(__name__)

############################################


def read_json_from(filename):
    query_file = open(filename, "r")
    query_body = json.load(query_file)
    query_file.close()
    return query_body


def query_file(date_start, date_end, policy, api):
    
    query_template_folder = "../query-templates"
    query_body = read_json_from(f"{query_template_folder}/{api}.json")
    
    query_body["bool"]["filter"]["range"]["@timestamp"]["gte"] = date_start
    query_body["bool"]["filter"]["range"]["@timestamp"]["lt"] = date_end
    
    generate_by_policy_type(query_body, policy)
        
    directory = "../generated-query-files/"
    filename = f"{date_start}-{api}-{policy.replace(" ", "_")}.json"
    f = open(f"{directory}{filename}", "w")
    f.write(json.dumps(query_body))
    f.close()
    
    logger.info("%s - %s - json query generated for date range from %s to %s in \"%s\"", api, policy, date_start, date_end, filename)


def generate_by_policy_type(query_body, policy):
    boolean_query = query_body["bool"]["must"]
    for query in boolean_query:
        if "match" in query:
            if "json.resp_headers.X-Rate-Limit-Policy" in query["match"]:
                query["match"]["json.resp_headers.X-Rate-Limit-Policy"] = policy.title()
                return
    