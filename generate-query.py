import json

class And_Query:
    def __init__(self, must, filter):
        self.must = must
        self.filter = filter

class Or_Query:
    def __init__(self, should, filter):
        self.should = should
        self.filter = filter
        
class Match:
    def __init__(self, match):
        self.match = match

def main():
    query_details = read_json_from("query.json")
    # print(query_details['bool']['must'])
    # print(query_details['bool']['filter'])
    
    query = And_Query("must", "filter")
    
    
    match_fields = Match({"json.request.host": "mobility.api.opendatahub.bz.it"})
    complete_query = {"should": match_fields.__dict__}
    
    print(complete_query)
    

def read_json_from(filename):
    query_file = open(filename, "r")
    query_body = json.load(query_file)
    query_file.close()
    return query_body

main()