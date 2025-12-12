# Movie Recommendation App

Repository fulfilling the requirements for the Modern DB Project 

### Authors

1. Cavan Riley
2. Nate Schaefer


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

WARNING: It will erase your database before running, if you don't want that to happen, comment out line 325 in db/populate_dbs.py
`
python3 db/populate_dbs.py
`

run this to populate your databases

The Redis database contains hashes for each movie formatted as such below 

```
movie:10

    title: GoldenEye (1995)
    genre: Action Adventure Thriller
    avg_rating: 3.5405405405405403
```

### Run the application

Running the application can be done with

`
python3 app.py
`

To view the application, go to [http://127.0.0.1:5000](http://127.0.0.1:5000)
