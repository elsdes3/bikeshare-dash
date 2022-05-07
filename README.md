# Toronto Bikeshare Dashboard

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/elsdes3/bikeshare-dash)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/elsdes3/bikeshare-dash/master/0_get_data.ipynb)
![CI](https://github.com/elsdes3/bikeshare-dash/workflows/CI/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/mit)
![OpenSource](https://badgen.net/badge/Open%20Source%20%3F/Yes%21/blue?icon=github)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
![prs-welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)
![pyup](https://pyup.io/repos/github/elsdes3/bikeshare-dash/shield.svg)

## [Table of Contents](#table-of-contents)
1. [About](#about)
2. [Notebooks](#notebooks)
   - [v1 Notebooks](#v1-notebooks)
   - [v2 Notebooks](#v2-notebooks)
3. [Notes](#notes)
4. [Project Organization](#project-organization)

## [About](#about)
Build a dashboard about Toronto Bikeshare usage during the months of January to October (inclusive) of the year 2021. See a screenshot of the dashboard (running locally) [here](https://github.com/elsdes3/bikeshare-dash/blob/main/reports/figures/screenshot.png).

## [Notebooks](#notebooks)
### [v1 Notebooks](#v1-notebooks)
1. `0_get_bikeshare_data.ipynb` ([view](https://nbviewer.org/github/elsdes3/bikeshare-dash/blob/main/0_get_bikeshare_data.ipynb))
   - download bikeshare ridership data and merge with supplementary city datasets
2. `1_transform_bikeshare_data.ipynb` ([view](https://nbviewer.org/github/elsdes3/bikeshare-dash/blob/main/1_transform_bikeshare_data.ipynb))
   - process merged raw data
2. `2_delete_data_v2.ipynb` ([view](https://nbviewer.org/github/elsdes3/bikeshare-dash/blob/main/2_delete_data.ipynb))
   - delete all raw and processed data and any charts saved during data analysis
### [v2 Notebooks](#v2-notebooks)
In progress.

## [Notes](#notes)
1. A notebook with a filename ending in `_v2.ipynb` contains analysis that is in progress.
2. The following neighbourhood profile columns in `aggregate_data.py` introduce missing values when called through the data pipeline notebook, but not when run through a standard Python function
   - `neigh_pop_2016`
   - `neigh_youth_15_24`
   - `neigh_work_age_25_54`

   If the neighbourhood profiles are useful for visualization, then future work should investigate the introduction of these missing values.

## [Project Organization](#project-organization)

    ├── .gitignore                    <- files and folders to be ignored by version control system
    ├── .pre-commit-config.yaml       <- configuration file for pre-commit hooks
    ├── .github
    │   ├── workflows
    │       └── main.yml              <- configuration file for CI build on Github Actions
    ├── *.ipynb                       <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                                    and a short `-` delimited description, e.g. `1.0-jqp-initial-data-exploration`.
    ├── app.py                        <- v2 dashboard application
    ├── configure_prefect.yml         <- Ansible playbook to automate setup of Python workflow orchestration tool
    ├── data
    │   ├── raw                       <- raw data downloaded from public sites
    |   └── processed                 <- transformed data
    ├── docker-compose.yml            <- to containerize v2 dashboard app
    ├── Dockerfile                    <- to containerize v2 dashboard app
    ├── executed_notebooks            <- folder to store notebooks after programmatic execution (with outputs)
    ├── hosts                         <- Ansible inventory
    ├── LICENSE
    ├── Makefile                      <- Makefile with commands like `make lint` or `make build`
    ├── README.md                     <- The top-level README for developers using this project.
    ├── nbconverter.py                <- programmatically convert *.ipynb files into *.html files
    ├── papermill_runner.py           <- control programmatic execution of notebooks
    ├── py_helpers.py                 <- Python helper functions v2 dashboard app
    ├── reports
    │   └── figures                   <- saved charts
    ├── requirements.txt              <- Python dependencies for v2 dashboard
    ├── src
    │   ├── *.py                      <- custom Python modules
    ├── tasks.py                      <- wrapper to inject env vars (if present) into Ansible playbook
    ├── requirements.txt              <- base packages required to run Dash application
    ├── tox.ini                       <- tox file with settings for running tox; see https://tox.readthedocs.io/en/latest/
    └── v1                            <- files from v1 dashboard
       ├── dash
       ├── Makefile
       ├── notebooks
       │   └── old-versions
       │       ├── *.ipynb
       ├── papermill_runner.py
       ├── pipe
       ├── requirements.txt
       └── tox.ini

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
