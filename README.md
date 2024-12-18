# Processing Logs on Elasticsearch

## Project Brief

1. Get logs data from Elasticsearch logs index based on generated query and aggregation to time interval of 1 second
2. Process data into cleaner format using `Bucket` object
3. Bulk post data into Elasticsearch storage index for every api and policy type

## How to Install and Run

### Setting up

1. `source .venv/bin/activate`
2. `python3 -m pip install -r requirements.txt`

### Installing new packages

1. `pip install <package>`
2. `python3 -m pip freeze > requirements.txt`

### Running Application

1. Change `.env.example` to `.env` and include `ELASTIC_URL` and `API_KEY`
2. Once in `.venv` virtual environment, run program using `python3 main.py`

### Running Tests

1. Go to `open-data-hub-api-rps`
2. Run tests `python -m unittest`

