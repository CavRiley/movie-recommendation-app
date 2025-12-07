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

Within the `db` directory in the project there are python scripts to populate the Redis database as well as the _____(need to figure out... prob neo4j) db

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
