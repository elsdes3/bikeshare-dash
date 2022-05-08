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
3. [Usage](#usage)
4. [Notes](#notes)
5. [Project Organization](#project-organization)

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

## [Usage](#usage)
1. Create a Prefect Cloud account account, by following steps 1. to 3. from the [Prefect documentation](https://orion-docs.prefect.io/ui/cloud/)
2. Set the following environment variables
   ```bash
   # (needed for OPTION 1 and 2 below) Configure Prefect storage
   $ export PREFECT_CLOUD_API_URL=<...>
   $ export PREFECT_CLOUD_API_KEY=<...>
   # (needed for OPTION 2 below) Set pre-existing storage as Prefect storage
   $ export PREFECT_CLOUD_STORAGE_ID=<...>
   ```

   See these two links from the Prefect documentation to get the API environment variables
   - [`PREFECT_CLOUD_API_URL`](https://orion-docs.prefect.io/ui/cloud/#create-a-workspace)
   - [`PREFECT_CLOUD_API_KEY`](https://orion-docs.prefect.io/ui/cloud/#create-an-api-key)
3. Perform the following one-time actions before notebooks can be used to run ETL jobs
   - [Create cloud storage for Prefect](https://orion-docs.prefect.io/concepts/storage/) (here, AWS S3 will be used)
     ```bash
     make aws-create
     ```
   - Do one of the following
     - (OPTION 1) Configure (a) [local environment to use Prefect Cloud](https://orion-docs.prefect.io/ui/cloud/#manually-configuring-cloud-settings), (b) [Prefect storage to use the cloud storage created](https://orion-docs.prefect.io/concepts/storage/#configure-storage) in step 1. above
       ```bash
       make build-configure
       ```

       This newly configured storage will be automatically set as the default Prefect storage. Running this command will also list all Prefect storage that has been configured to-date; from this list, the storage ID for the `PREFECT_CLOUD_STORAGE_ID` environment variable can be found. **If Prefect storage has not been previously configured, then this option must be followed.**

       When following this option
       - the numerical suffix at the end of the variable `STORAGE_NAME` in the `# GLOBALS` section in the `Makefile` must be incremented by 1
       - a new value, for this variable (`STORAGE_NAME`) must be set in the `Makefile`
     - (OPTION 2) (a) [Configure local environment to use Prefect Cloud](https://orion-docs.prefect.io/ui/cloud/#manually-configuring-cloud-settings), (b) [set a pre-existing storage as the default Prefect storage](https://orion-docs.prefect.io/concepts/storage/#setting-storage)
       ```bash
       make build-reuse
       ```

       This option requires that `PREFECT_CLOUD_STORAGE_ID` be set as an environment variable. This is the storage ID of pre-existing Prefect storage. All Prefect storage IDs are listed at the end of OPTION 1 above. **OPTION 2 assumes that OPTION 1 has been previously followed and so the ID of the storage to be used as the default Prefect storage is known.**

     Both of these options will also start an interactive Python environment (Jupyter Lab) to run ETL jobs (get datasets, merge, etc.) using [Prefect flows](https://orion-docs.prefect.io/concepts/flows/). When following OPTION 1, it is possible to replace the starting of this interactive Python environment by the programmatic execution of notebooks (and all ETL jobs, using Prefect flows, contained in those notebooks) by running
     ```bash
     make build-configure-auto
     ```

The CI run is similar OPTION 2 above. So, it assumes that the three environment variables for OPTION 2 above have been set as [secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets). As discussed for OPTION 2 above, `PREFECT_CLOUD_STORAGE_ID` must be set since Prefect storage will not be configured from scratch (as is the case in OPTION 1 above) during the CI run. Instead, pre-existing storage will be set as the default Prefect storage (OPTION 2) and then [used to store the outputs of tasks and flows](https://orion-docs.prefect.io/concepts/storage/). The CI run will not start an interactive Python environment (Jupyter Lab) to run ETL jobs using Prefect flows. Instead, it will programmatically run notebooks thereby running all Prefect flows contained in this notebooks.

To delete cloud storage (AWS S3), run
```bash
make aws-delete
```

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
