# datagen

Datagen generates all of the input JSON files needed by each Confluence module to execute in the AWS Batch job environment from the SWOT shapefiles hosted in an S3 bucket.

It generates one input file per continent for the following wher {c} equals the continent abbreviation:
- basin_{c}.json: Basin identifiers with associated reach identifiers, SWORD file, SoS file, and SWOT NetCDF file.
- cycle_passes_{c}.json: Cycle and pass numbers in the form of "c_p" with an associated number for unique identification as a time step or observation.
- hivdisets_{c}.json: HiVDI sets.
- metrosets_{c}.json: MetroMan sets.
- passes_{c}.json: The inverse of cycle_passes{c}.json where the unique identifier is the key to the cycle and pass numbers.
- reach_node_{c}.json: Reach identifiers with associated node identifiers.
- reaches_{c}.json: Reach identifiers with associated SWOTD file, SoS file, and SWOT NetCDF file.
- s3_list_{c}.json: List of S3 URIs for the SWOT shapefiles.
- sicsets_{c}.json: Sic4DVAR sets.

**Note:** `datagen` operations have been implemented for SWOT Lake shapefiles but they need to be tested.

# installation

Build a Docker image: `docker build -t datagen .`

# execution

**Command line arguments:**
- -i: index to locate continent in JSON file
- -c: context to generate data for: 'river' or 'lake'
- -s: short name of the collection
- -t: temporal range to retrieve S3 URIs
- -p: the collection provider name
- -d: where to locate and save JSON data
- -k : unique SSM encryption key identifier
- -o: indicate run on simulated data (optional)
- -l: indicates local run (optional)
- -j: name of continent JSON file (optional)
- -f: name of shapefile directory for local runs (optional)

**Execute a Docker container: **

AWS credentials will need to be passed as environment variables to the container so that `datagen` may access AWS infrastructure to generate JSON files.

```
# Credentials
export aws_key=XXXXXXXXXXXXXX
export aws_secret=XXXXXXXXXXXXXXXXXXXXXXXXXX

# Docker run command
docker run --rm --name datagen -e AWS_ACCESS_KEY_ID=$aws_key -e AWS_SECRET_ACCESS_KEY=$aws_secret -e AWS_DEFAULT_REGION=us-west-2 -e AWS_BATCH_JOB_ARRAY_INDEX=3 -v /mnt/datagen:/data datagen:latest -i -235 -c river -s SHORT_NAME -p PROVIDER -d /data -k XXXXX-XXXXXX-XXXXXX
```