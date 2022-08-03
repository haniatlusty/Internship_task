# Test task for Data Engineering Internship

Implement the service which 
* periodically processes input data, aggregates it and stores in output location
* serves the requests to query the data via HTTP

Data should be stored in [Minio](https://min.io/), the example  [docker-compose](./01-docker-compose/docker-compose.yml) file already contains the definition of it.

## Input data description

In a provided archive, there's a `01-src-data` folder which contains some CSV files with information about users, and image files for some of the users. If the base CSV filename matches to the image base filename, then the image is a picture of that user. The base name of a CSV file / image is a user's ID.

E.g. 

* `01-src-data/1000.csv` - information about user #1000
* `01-src-data/1000.jpg` - user #1000's picture

You will have to load the source data to Minio for your service to process.

### Input CSV file format
It has following columns:
1. first_name - User first name
2. last_name - User last name
3. birthts - User birthdate timestamp in milliseconds UTC

**Example:**
```text
first_name, last_name, birthts
John, Doe, 946674000000
```

## Data transformation to be performed

1. Read CSV files
2. Match images to users
3. Aggregate data into a single CSV file (see format below)
4. Store the output data to [Minio](https://min.io/) (possibly overwriting the earlier version of the output)

This should happen periodically. so once source data is updated in Minio, the output should eventually be updated. The update of the output has to happen not later then in 1 hour from source update.

## Output data format
Update `processed_data/output.csv` CSV file and incorporate new data coming from input. Note: we can update data for previously processed user, in this case the record for a user should be overwritten. The output CSV file should not contain duplicate user records. 

Output CSV file format: `user_id, first_name, last_name, birthts, img_path`

## Serving the data
The service should implement web server with following endpoints:

* `GET /data` - get all records from DB in JSON format. Need to implement filtering by: is_image_exists = True/False, user min_age and max_age in years.
* `POST /data` - manually trigger reprocessing the source data in  `01-src_data`
* `GET /stats` - calculate and return the average age of users matching the filters (`is_image_exists=True/False`, `min_age`, `max_age`)

It should answer on `localhost:8080` when your service is running.

## Other requirements
* You can use a programming language/platform of your choice
  however resulting service should be a set of containers run with `docker-compose`.
* You can leverage any additional containers as you see necessary for your solution
* Put the script to build (if necessary) and start a service into `run.sh` shell script in the root of your repository.

## Turning in the completed task
* To turn in the completed test task, create a **private** GitHub repository and give us the access (GitHub accounts: HaykManukyanAvetiky, lon10, tigra, akhmetbekali)
* Be sure to include `README.md` file into your repository describing your approach
* You should've got this archive as a part of email inviting you to solve the test task. Reply to this email and include the URL of your  repository

## Evaluation criteria

We will take following into account when evaluating your solution:
* Correctness of implementation
* Scalability
* Effective use of resources
* Simplicity and elegance
* Code quality
