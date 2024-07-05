The library **poetry** is a modern tool for reproducibility and robust dependency management. 

# Using poetry

## Installation
curl -sSL https://install.python-poetry.org | python3 -

## Add poetry's bin directory to PATH environment variable

### Windows
1. Open Environment Variables
   1. Press 'Win + X' and select 'System'
   2. Click on 'Advanced system settings'
   3. In the Advanced window, click on 'Environment Variables'
2. Edit PATH Variable
   1. In the Environment Variables window, find the 'Path' variable in the 'User variables' section and select it.
   2. Click 'Edit'
3. Add Poetry Bin Directory
   1. Click 'New' and add the path to Poetry's bin directory:
      1. C:\Users\Jaspo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\Roaming\pypoetry\venv\Scripts\
4. Verify Installation
   1. Run 'poetry --version' in a command prompt to check if Poetry is accessible


## Initialize project
poetry init

## Add Dependencies
poetry add psycopg2-binary

## Disable packaging
In pyproject.toml, add "package-mode = false"

## Create poetry.lock file
poetry install

Note: Whenever significant changes are made to the toml file, run:
poetry lock --no-update
poetry install

## Activate the virtual environment
poetry shell

## Deactive the virtual environment
exit

# Steps to reproduce environment

## Clone github repository

## Install dependencies
poetry install
