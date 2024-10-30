# Assignment 3 Code
This repository contains code for **Assignment 3**, which processes and inserts the Geolife dataset into a MongoDB 
database. The project includes modules to handle data processing, database operations, and execution of 
assignment tasks as unit tests.

**Note**: The dataset itself is not included in this repository.

## Table of Contents
- [Files](#files)
- [Setup Instructions](#setup-instructions)
  - [Prerequisites](#prerequisites)
  - [Steps](#steps)
- [Running the Code](#running-the-code)
- [Teardown](#teardown)
- [Additional Notes](#additional-notes)

## Files

- `main.py`: The main script that processes the dataset using functions from `geolife_data_handler.py` and inserts data into the database using `geolife_db.py`.
- `geolife_db.py`: Contains methods for interacting with the MongoDB database.
- `DbConnector.py`: Provides methods to connect to and disconnect from the database.
- `geolife_data_handler.py`: Includes functions to process the Geolife dataset.
- `const.py`: Holds constants used in the code, such as collection names, schemas, and file paths.
- `requirements.txt`: Lists all required Python packages.
- `tasks.py`: Contains the assignment tasks implemented as unit tests.
- `docker-compose.yml`: Used to set up the MongoDB database via Docker.
- `.env`: Stores environment variables for the database connection.

## Setup Instructions and Running the code

### Prerequisites

- **Python 3.8 or higher**
- **Docker**
- **Docker Compose**
- **Git** (optional, for cloning the repository)

### Setup:
1. **Create .env file**:
Create a `.env` file in the root directory with the following content:
- `DB_NAME`: The name of the database.
- `DB_HOST`: The host of the database.
- `DB_ROOT_USER`: Root username for the database
- `DB_ROOT_PASSWORD`: Root password for the database.

Example:
```
DB_NAME=geolife_db
DB_HOST=localhost
DB_ROOT_USER=assignment3
DB_ROOT_PASSWORD=assignment3
```

2. **Set up a virtual environment and database**: 
 ```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
docker compose up -d
 ```

3. **Create MongoDB user**`:

Connect to the MongoDB container and create a user with read and write permissions for the database.
```
docker exec -it mongo_db mongosh -u DB_ROOT_USER -p DB_ROOT_PASSWORD --authenticationDatabase "admin"
```
Example:
```bash
docker exec -it mongo_db mongosh -u assignment3 -p assignment3 --authenticationDatabase "admin"
```
Use database name from the `.env` file to switch to the database:
```
use {DB_NAME}
```
Example:
```bash
use geolife_db
```
Create the database user with values from the `.env` file, and assign read and write permissions to the database:
````
db.createUser({ user: {DB_ROOT_USER}, pwd: {DB_ROOT_PASSWORD}, roles: [{ role: "readWrite", db: "geolife_db" }] })
````
Example:
```bash
db.createUser({ user: "assignment3", pwd: "assignment3", roles: [{ role: "readWrite", db: "geolife_db" }] })
````

### Running the code
One can run the main file to process the dataset and insert it into the database, then run the tasks file to execute 
the assignment tasks. 

NB! The database needs to be populated before running the tasks.

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