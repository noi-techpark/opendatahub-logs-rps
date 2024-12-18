# --------------------------------------------------------------------------------------------------
#
# Note: main.py is the main program that takes in configuration parameters
# --------------------------------------------------------------------------------------------------


from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import time
import app
import logging
logger = logging.getLogger(__name__)


######## Load Environment Variables ########

load_dotenv()

API_KEY = os.getenv("API_KEY")
ELASTIC_URL = os.getenv("ELASTIC_URL")


############## Configurations ##############
    
logging.basicConfig(filename='main.log', format='%(levelname)s: %(asctime)s - %(name)s - %(message)s', level=logging.DEBUG)

policies_without_referer = ["anonymous", "premium", "authenticated basic", "no restriction", "basic"]
# policies = policies_without_referer + ["referer"]
policies = ["referer"]
apis = ["timeseries", "content"]
storage_index = "request-rate"

############################################


def validate_date(date):
    if date != "":
        if len(date) != len("2024-11-01T00:00:00.000Z") or date[-1] != "Z":
            raise Exception("date not written in UTC format")
        
        
def main():
    
    count = 0
    date = ""
    # date = "2024-12-12T00:00:00.000Z"
    try:
        validate_date(date)
    except Exception as e: 
        logger.error("%s", e)
        print("error: ", e)

    
    while count < 1:
     
        t0 = time.time()
        
        logger.info("starting script")
        
        app.run_with_params(date, apis, policies, policies_without_referer, storage_index, API_KEY, ELASTIC_URL)
        # date, _ = app.round_to_next_day_range(date)
        # print("Ran ", date)

        t1 = time.time()
        logger.info("run completed in %.3f seconds", t1-t0)
        
        count += 1
        print("Ran round ", count)

main()


