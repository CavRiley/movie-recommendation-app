# Movie Recommendation App

Repository fulfilling the requirements for the Modern DB Project 

### Authors

1. Cavan Riley
2. Nate Schaefer


## Versioning
Python v3.13.15

Packages are in requirements.txt

## Setup

### Virtual Environment

To run the application locally you first need to create a virtual environment:

`
python3 -m venv venv
`

`
source venv/bin/activate
`

`
pip install -r requirements.txt
`

### Database Setup

>[!NOTE]
> This assumes you have the movies and ratings csv files in the root directory

Within the `db` directory in the project there is a python script to populate databases. It assumes your password for neo4j is "password".

>[!IMPORTANT]
> Your Neo4j instance needs the Graph Data Science (GDS) plugin

WARNING: It will erase your Neo4j database before running(you need to manually erase redis database, if needed), if you don't want that to happen, comment out line 325 in db/populate_dbs.py

run this to populate your databases ( make sure to have redis and Neo4j running before doing so)

`
python3 db/populate_dbs.py
`

### Run the application

Running the application can be done with

`
python3 app.py
`

To view the application, go to [http://127.0.0.1:5000](http://127.0.0.1:5000)


>[!CAUTION]
> The filters under the search bar are only applied when you hit search again
