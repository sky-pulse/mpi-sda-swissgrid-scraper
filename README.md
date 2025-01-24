# mpi-sda-swissgrid-scraper

## Description

This is a scraper repo that scrapes data from Swissgrid

## Requirements

- Python 3.11

### Run the container

```bash
docker build -t mpi-swissgrid-scraper .
```

```bash
docker run --rm \
    --name mpi-swissgrid-scraper \
    --net="host" \
    mpi-swissgrid-scraper
```

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Ensure all the required command lines argument are present in the bash file(run.sh)

You can run a demo using - 
```bash
./run.sh
```


