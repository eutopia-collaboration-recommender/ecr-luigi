# EUTOPIA Collaboration Recommender: Luigi Pipeline

1. Install Python

```
sudo apt install python3-pip
```

2. Create a virtualenv (either in PyCharm or in bash) and activate it.

3. Install requirements

```
pip install -r requirements.txt
```

```
docker-compose up
```

```bash
luigid --background --logdir logging --pidfile luigid.pid
export PYTHONPATH=$PYTHONPATH:$(pwd)
nohup luigi --module tasks.ingestion.crossref_update_publications CrossrefUpdatePublicationsTask --updated-date-start "2024-01-01" --updated-date-end "2024-11-30" --scheduler-host localhost --scheduler-port 8082  > /dev/null 2>&1 &
pgrep -a luigi
kill <pid>
```