# open-data-hub-api-rps

## Setting up

1. `source .venv/bin/activate`
2. `python3 -m pip install -r requirements.txt`

## Running Application

1. Once in `.venv` virtual environment, run program using `python3 app.py`

## Installing new packages

1. `pip install <package>`
2. `python3 -m pip freeze > requirements.txt`


## Updates made

- October 2024 -> Referer, Timeseries
  

## Program Notes

### Get Data from Elasticsearch

1. Connect to Elasticsearch client
2. Read query body from file
3. Read aggregation from file
4. Get buckets from Elasticsearch based on query and aggregation from `filebeat-*` index

### Store Data back into Elasticsearch

1. Store each bucket in `Bucket` object
2. Convert into dictionary and append into document list
3. Bulk post document list into Elasticsearch `request-rate` index


## On Elasticsearch

1. Created index `request-rate`
2. Access has to be given to index

Anonymous has no referer -> need to use another method

## Running Tests

1. Go to `open-data-hub-api-rps`
2. Run tests `python -m unittest`
