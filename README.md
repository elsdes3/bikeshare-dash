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
3. [Project Organization](#project-organization)

## [About](#about)
Build a dashboard about Toronto Bikeshare usage during the months of January to October (inclusive) of the year 2021. See a screenshot of the dashboard (running locally) [here](https://github.com/elsdes3/bikeshare-dash/blob/main/reports/figures/screenshot.png).

## [Notebooks](#notebooks)
1. `0_get_bikeshare_data.ipynb` ([view](https://nbviewer.org/github/elsdes3/bikeshare-dash/blob/main/0_get_bikeshare_data.ipynb))
   - download bikeshare ridership data and merge with supplementary city datasets
2. `1_process_bikeshare_data.ipynb` ([view](https://nbviewer.org/github/elsdes3/bikeshare-dash/blob/main/1_process_bikeshare_data.ipynb))
   - process merged raw data
2. `2_delete_data.ipynb` ([view](https://nbviewer.org/github/elsdes3/bikeshare-dash/blob/main/2_delete_data.ipynb))
   - delete all raw and processed data and any charts saved during data analysis

## [Project Organization](#project-organization)

    ├── LICENSE
    ├── .gitignore                    <- files and folders to be ignored by version control system
    ├── .pre-commit-config.yaml       <- configuration file for pre-commit hooks
    ├── .github
    │   ├── workflows
    │       └── main.yml              <- configuration file for CI build on Github Actions
    ├── Makefile                      <- Makefile with commands like `make lint` or `make build`
    ├── README.md                     <- The top-level README for developers using this project.
    ├── app.py                        <- PlotLy Dash application
    ├── Procfile                      <- Heroku deployment file
    ├── heroku_run.sh                 <- Heroku deployment script
    ├── runtime.txt                   <- Heroku Python file
    ├── py_helpers.py                 <- Python module with helper functions for PlotLy Dash application
    ├── assets
    │   └── *.*                       <- HTML-related files for PlotLy Dash application
    ├── data
    │   ├── raw                       <- Scripts to download or generate data
    |   └── processed                 <- merged and filtered data, sampled at daily frequency
    ├── *.ipynb                       <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                                    and a short `-` delimited description, e.g. `1.0-jqp-initial-data-exploration`.
    ├── requirements.txt              <- base packages required to run Dash application
    └── tox.ini                       <- tox file with settings for running tox; see https://tox.readthedocs.io/en/latest/

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
