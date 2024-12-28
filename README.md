# EUTOPIA Collaboration Recommender: Luigi Pipeline

**Author:** [@lukazontar](https://github.com/lukazontar)

<hr/>

## Project Introduction

This repository contains the code for the EUTOPIA collaboration recommender system.

In today's academic landscape, collaboration across disciplines and institutions is crucial due to complex scientific
papers. Yet, such collaboration is often underused, leading to isolated researchers and few connected hubs. This thesis
aims to create a system for proposing new partnerships based on research interests and trends, enhancing academic
cooperation. It focuses on building a network from scientific co-authorships and a recommender system for future
collaborations. Emphasis is on improving the EUTOPIA organization by fostering valuable, interdisciplinary academic
relationships.

The system consist of three main components:

1. Luigi pipeline for data ingestion and
   transformation: [ecr-luigi :octocat:](https://github.com/eutopia-collaboration-recommender/ecr-luigi).
2. Analytical layer for gaining a deeper understanding of the
   data: [ecr-analytics :octocat:](https://github.com/eutopia-collaboration-recommender/ecr-analytics).
3. Recommender system for proposing new
   collaborations: [ecr-recommender :octocat:](https://github.com/eutopia-collaboration-recommender/ecr-recommender).

<hr/>

## Setting up the environment

Environment stack:

- Python, SQL as main programming languages.
- [Luigi](https://luigi.readthedocs.io/en/latest/index.html): an open-source data orchestration framework. *Installed
  via pip*.
- [Postgres](https://www.postgresql.org/): as the data warehouse. *Installed via Docker*.
- [MongoDB](https://www.mongodb.com/): as the datalake. *Installed via Docker*.
- [dbt](https://www.getdbt.com/): as the main data transformation and modeling tool. *Installed via pip*.

### Prerequisites

- Docker
- Python 3.10 (using [pyenv](https://github.com/pyenv-win/pyenv-win)
  and [venv](https://docs.python.org/3/library/venv.html))

To run Python scripts, you need to install the requirements:

```bash
pip install -r requirements.txt
```

### Running Luigi

Before running Luigi tasks, you need to set up Postgres and MongoDB databases. We've provided a `docker-compose.yaml`
file for this purpose.
You can run the following command to start the databases:

```bash
# Note that if you are running this command for the first time and want to restore the MongoDB backup, you need to uncomment a line in mongorestore.sh.
docker-compose up
```

The system also requires a `.env` file and `config.yaml` file to be set up. Configuration setup instructions can be
found in: [Configuration setup](./docs/Configuration setup.md).

After that, you can run the Luigi tasks with the following command:

```bash
# Start the Luigi daemon (available at localhost:8082)
luigid --background --logdir logging --pidfile luigid.pid

# Set the PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Run a luigi task in the background, for example running:
# - RefreshTask for refreshing the data
# - Located in tasks/refresh.py
# - With a single parameter period set to "week", meaning that the task will refresh the data for the last 7 days
nohup luigi --module tasks.refresh RefreshTask --period "week" --scheduler-host localhost --scheduler-port 8082  > /dev/null 2>&1 &

# To check the status of the Luigi tasks, you can run the following command
pgrep -a luigi
```

### Transforming data with dbt

To run `dbt` you need to have the `dbt` CLI installed. We're assuming that you have already installed the requirements
for the project using `pip`.

After that, you need to set up the `profiles.yml` file in the `~/.dbt` directory. See dbt
documentation: [Postgres setup](https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup).

Having established that, you can execute the following command to run the models in the `dbt` folder:

```bash
# Run the dbt models
dbt run

# To run only the models with the tag "data_ingestion"
dbt run --select tag:data_ingestion

# To run only the models with the tag "analytics"
dbt run --select tag:analytics 
```

<hr/>

## In-depth documentation

Check additional documentation in the `docs` directory.

### Connecting to the production VM

All this code is deployed to the production VM. To connect to the VM, you need to have an SSH key pair. You can generate
a new SSH key pair by running the following command:

1. Generate a new SSH key pair:
    ```bash
    ssh-keygen -t ed25519 -f ssh-eucollab -C "Connection to eucollab VM."
    ```
2. Copy the public key to the VM.
3. **Connect to the VM**:
    - Open a terminal and connect to the VM using the following command:
    ```bash
    ssh -i ~/.ssh/ssh-eucollab <USERNAME>@<IP_ADDRESS>
    ```

### Backup the database

To backup database `lojze`:

```bash
sudo docker exec -t postgres_container pg_dump -U <USERNAME> lojze > pg_dump_lojze.sql
```

Backup can also be copied to the local machine by executing:

```bash
scp -i ~/.ssh/ssh-eucollab <USERNAME>@<IP_ADDRESS>:~/eutopia-colllaboration/ecr-luigi/pg_dump_lojze.sql <DESTINATION_PATH>
```

### Low-hanging fruits for improvement

- Improve Luigi pipelines by clarifying what is stored in the database by a given task so that the data is not
  duplicated.
- Dockerize the Luigi pipeline.
- Add tests and documentation to dbt models.