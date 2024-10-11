# Assignment 2 Code
Contains code for Assignment 2, which processes and inserts the Geolife 
dataset into a MySQL database. The code includes code to handle the dataset, run 
database operations, and execute the assignment tasks as unit tests. 

**Note**: The dataset itself is not included in this repository.

## Files
- `main.py`: The main file uses the functions in `geolife_data_handler.py` to process the dataset 
and `geolife db.py` functions to insert the data into the database.
- `geolife db.py`: Contains all methods used to interact with the database.
- `DbConnector.py`: Methods to connect and disconnect from the database.
- `geolife_data_handler.py`: Contains functions to process the Geolife dataset.
- `consts.py`: Contains constants used in the code, e.i. table names, schemas, paths, etc.
- `requirements.txt`: Contains all the required packages to run the code.
- `tasks.py`: Contains the tasks for the assignment as unit tests.
- `docker-compose.yml`: Contains the docker-compose file to setup the MySQL database.
- `.env`: Contains the environment variables for the database connection.
- `file_paths.py`: Function for counting the number of files and lines in the dataset

## Setup Instructions
Before running the code, ensure that you have Python and Docker installed.
You can use the `docker-compose.yml` file to quickly set up a MySQL database instance.

### Steps:
1. **Create .env file**:
Create a `.env` file in the root directory with the following content:
- `MYSQL_HOST`: The host of the MySQL database.
- `MYSQL_ROOT_PASSWORD`: The root password for the MySQL database.
- `MYSQL_DATABASE`: The name of the database
- `MYSQL_USER`: The username for the database
- `MYSQL_PASSWORD`: The password for the database

Example:
```
MYSQL_HOST=localhost
MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_DATABASE=geolife
MYSQL_USER=assignment2
MYSQL_PASSWORD=assignment2password
```

2. **Set up a virtual environment and database**: 
 ```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
docker compose up -d
 ```

### Running the code
One can run the main file to process the dataset and insert it into the database, then run the tasks file to execute 
the assignment tasks.

Main file:
```bash
python main.py
```

Assignment tasks:
```bash
python tasks.py
```
### Teardown
Removes the docker container and deactivates the virtual environment.
```bash
docker-compose down 
deactivate
```