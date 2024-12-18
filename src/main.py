# --------------------------------------------------------------------------------------------------
#
# Note: 
# - main(): the main program that takes in configuration parameters
#
# Configuration Parameters:
# - start_date: UTC Date [String] 
#               If database date is later than start_date, database date will be taken
#               If start_date is empty string, database date is taken, otherwise default date is taken
# - logs_index: Elasticsearch index where logs are stored [String]
# - storage_index: Elasticsearch index where processed data are stored [String]
# - apis: types of api [List]
# - policies_with_referer: policy types that have referer field [List]
# - policies_without_referer: policy types that do not have referer field [List]
# --------------------------------------------------------------------------------------------------


from dotenv import load_dotenv
import os
import time
import re
import app
import logging
logger = logging.getLogger(__name__)


######## Load Environment Variables ########

load_dotenv()

API_KEY = os.getenv("API_KEY")
ELASTIC_URL = os.getenv("ELASTIC_URL")


############## Configurations ##############
    
logging.basicConfig(filename='main.log', format='%(levelname)s: %(asctime)s - %(name)s - %(message)s', level=logging.DEBUG)

start_date = ""
# start_date = "2024-12-17T00:00:00.000Z"

logs_index = "accesslog-*"
# logs_index = "filebeat-*"
storage_index = "request-rate"
apis = ["timeseries", "content"]
policies_with_referer = ["referer"]
policies_without_referer = ["anonymous", "premium", "authenticated basic", "no restriction", "basic"]

############################################

        
def main():
    
    try:
        app.validate_date(start_date)
    except Exception as e: 
        logger.error("%s", e)
        print("error: ", e)
        return
    
    # Set while loop logic
    count = 0
    while count < 1:
        count += 1

        try:
            t0 = time.time()
            logger.info("starting script")
            
            app.run_with_params(start_date, logs_index, storage_index, apis, policies_with_referer, policies_without_referer, API_KEY, ELASTIC_URL)

            t1 = time.time()
            logger.info("run completed in %.3f seconds", t1-t0)
            
        except Exception as e:
            logger.error("%s", e)
            print("error: ", e)
        

main()


