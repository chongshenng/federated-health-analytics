---
tags: [basic, tabular, federated analytics]
dataset: [artificial]
framework: [pandas]
---

# Federated Analytics with OMOP CDM using Flower

This example will show you how you can use Flower to run federated analytics workloads on distributed SQL databases.

## Set up the project

### Clone the project

After cloning the project, this will create a new directory called `federated-analytics` containing the following files:

```shell
federated-analytics
├── db_init.sh          # Defines an artificial OMOP CDM table
├── db_start.sh         # Generates and starts Postgres containers with OMOP CDM data
├── federated-analytics
│   ├── client_app.py   # Defines your ClientApp
│   ├── server_app.py   # Defines your ServerApp
│   └── task.py         # Defines your database connection and data loading
├── pyproject.toml      # Project metadata like dependencies and configs
└── README.md
```

### Install dependencies and project

Install the dependencies defined in `pyproject.toml` as well as the `federated-analytics` package.

```shell
# From a new python environment, run:
pip install -e .
```
