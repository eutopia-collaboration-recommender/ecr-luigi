# Configuration setup

**Author:** [@lukazontar](https://github.com/lukazontar)

### File: `config.yaml`

Here is an example of the configuration setup for the project. The configuration file should be located in the root project folder and should be named `config.yaml`.
```yaml
#############################
# Configuration setup
#############################
# Environment name: "dev" or "prod"
ENVIRONMENT: "prod"
# Minimum year for the publication date in the database
MIN_YEAR: 2000
# Batch size for the data processing
# TODO: Move BATCH_SIZE to `util` folder
BATCH_SIZE:
  EMBEDDING: 64
  SMALL: 100
  MEDIUM: 1000
  LARGE: 10000
# Ollama API configuration
OLLAMA:
  HOST: <OLLAMA_HOST>
  API_KEY: <OLLAMA_API_KEY>
  MODEL: "llama3.1:8b"
# Postgres configuration
POSTGRES:
  HOST: <POSTGRES_IP>
  PORT: 5000
  USERNAME: <POSTGRES_USERNAME>
  PASSWORD: <POSTGRES_PASSWORD>
  DATABASE: "lojze"
  SCHEMA: "jezero"
  DBT_SCHEMA: "analitik"
# ORCID configuration
ORCID:
  CLIENT_ID: <ORCID_CLIENT_ID>
  CLIENT_SECRET: <ORCID_CLIENT_SECRET>
  # Number of records to checkpoint. The task will checkpoint after processing this number of records.
  NUM_RECORDS_TO_CHECKPOINT: 100
# MongoDB configuration (containing the Elsevier data)
MONGODB:
  HOST: <MONGO_DB_IP>
  PORT: 27000
  USERNAME: <MONGO_DB_USERNAME>
  PASSWORD: <MONGO_DB_PASSWORD>
  DATABASE: "elsevier"
  # Number of records to checkpoint. The task will checkpoint after processing this number of records.
  NUM_RECORDS_TO_CHECKPOINT: 100
# Crossref configuration
CROSSREF:
  MAILTO: <MAILTO>
  # Number of records to checkpoint. The task will checkpoint after processing this number of records.
  NUM_RECORDS_TO_CHECKPOINT: 100
  # Number of top research area publications to fetch from the Crossref API (top by relevance)
  NUM_TOP_RESEARCH_AREA_PUBLICATIONS: 16
# Text embedding configuration
TEXT_EMBEDDING:
  # Number of records to checkpoint. The task will checkpoint after processing this number of records.
  NUM_RECORDS_TO_CHECKPOINT: 5
```

### File: `.env`

The system requires a `.env` file to be set up for `docker-compose` configuration. Here's an example of the `.env` file:
```dotenv
POSTGRES_USER=<POSTGRES_USERNAME>
POSTGRES_PASSWORD=<POSTGRES_PASSWORD>
MONGO_INITDB_ROOT_USERNAME=<MONGO_DB_USERNAME>
MONGO_INITDB_ROOT_PASSWORD=<MONGO_DB_PASSWORD>
```