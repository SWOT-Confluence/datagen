"""Script to generate: basin, reach, reach_node, cycle_pass JSON files.

Also generates a list of S3 URIs for SWOT shapefiles. Accesses PO.DAAC CMR to
generate a list.

Requires .netrc file to log into CMR API and the AWS CLI tool configured with 
credentials and region.

Local execution option available, if you run locally, please place shapefiles in
the --d or --directory referenced in command line arguments.

Command line arguments:
 -i: index to locate continent in JSON file
 -s: short name of the collection
 -t: temporal range to retrieve S3 URIs
 -p: the collection provider name
 -d: where to locate and save JSON data
 -l: indicates local run (optional)
 -j: name of JSON file (optional)
 -s: name of shapefile directory for local runs (optional)

Example: python3 generate_data.py -i 3 -p POCLOUD -s SWOT_SIMULATED_NA_CONTINENT_L2_HR_RIVERSP_V1 -t 2022-08-01T00:00:00Z,2022-08-22T23:59:59Z -d /home/useraccount/json_data
"""

# Standard imports
import argparse
import json
import os
from pathlib import Path
import zipfile

# Third-party imports
import boto3
import botocore
import fsspec
import requests
import shapefile

# Local imports
from conf import conf
from Basin import Basin
from S3List import S3List

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Retrieve a list of S3 URIs")
    arg_parser.add_argument("-i",
                            "--index",
                            type=str,
                            help="Index value to select continent to run on")
    arg_parser.add_argument("-p",
                            "--provider",
                            type=str,
                            help="The dataset or collection provider")
    arg_parser.add_argument("-s",
                            "--shortname",
                            type=str,
                            help="The collection shortname")
    arg_parser.add_argument("-t",
                            "--temporalrange",
                            type=str,
                            help="Temporal range to retrieve URIs for")
    arg_parser.add_argument("-d",
                            "--directory",
                            type=str,
                            help="Directory to save JSON data to")
    arg_parser.add_argument("-j",
                            "--jsonfile",
                            type=str,
                            help="Name of continent JSON file",
                            default="continent.json")
    arg_parser.add_argument("-l",
                            "--local",
                            help="Indicate local run",
                            action="store_true")
    arg_parser.add_argument("-f",
                            "--shapefiledir",
                            type=str,
                            help="Directory of local shapefiles")
    return arg_parser

def get_continent(index, json_file):
    """Retrieve continent to run datagen operations for."""
    
    i = int(index) if index != "-235" else os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
    with open(json_file) as jf:
        data = json.load(jf)
    return list(data[i].keys())[0].upper()

def get_s3_creds(provider):
    """Retreive S3 credentials from endpoint, write to SSM parameter store
    and return them."""

    s3_creds = requests.get(conf["s3_cred_endpoints"][provider.lower()]).json()

    ssm_client = boto3.client('ssm')
    try:
        response = ssm_client.put_parameter(
            Name="s3_creds_key",
            Description="Temporary SWOT S3 bucket key",
            Value=s3_creds["accessKeyId"],
            Type="SecureString",
            KeyId="1416df6c-7a20-46a1-949d-d26975acfdd0",
            Overwrite=True,
            Tier="Standard"
        )
        response = ssm_client.put_parameter(
            Name="s3_creds_secret",
            Description="Temporary SWOT S3 bucket secret",
            Value=s3_creds["secretAccessKey"],
            Type="SecureString",
            KeyId="1416df6c-7a20-46a1-949d-d26975acfdd0",
            Overwrite=True,
            Tier="Standard"
        )
        response = ssm_client.put_parameter(
            Name="s3_creds_token",
            Description="Temporary SWOT S3 bucket token",
            Value=s3_creds["sessionToken"],
            Type="SecureString",
            KeyId="1416df6c-7a20-46a1-949d-d26975acfdd0",
            Overwrite=True,
            Tier="Standard"
        )
        response = ssm_client.put_parameter(
            Name="s3_creds_expiration",
            Description="Temporary SWOT S3 bucket expiration",
            Value=s3_creds["expiration"],
            Type="SecureString",
            KeyId="1416df6c-7a20-46a1-949d-d26975acfdd0",
            Overwrite=True,
            Tier="Standard"
        )
    except botocore.exceptions.ClientError:
        raise
    else:
        return s3_creds

def extract_reach_ids(shpfiles, creds):
    """Extract reach identifiers from shapefile names and return a list.
    
    Parameters
    ----------
    shapefiles: list
        list of shapefile names
    """

    reach_ids = []
    for shpfile in shpfiles:
        # Open S3 zip file
        with fsspec.open(f"{shpfile}", mode="rb", anon=False, 
            key=creds["accessKeyId"], secret=creds["secretAccessKey"], 
            token=creds["sessionToken"]) as shpfh:

            # Locate and open DBF file
            dbf_file = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"            
            zip_file = zipfile.ZipFile(shpfh, 'r')
            with zip_file.open(dbf_file) as dbf:
                sf = shapefile.Reader(dbf=dbf)
                records = sf.records()
                reach_id = {rec["reach_id"] for rec in records}                
            reach_ids.extend(list(reach_id))
    # Remove duplicates from mulitple files
    reach_ids = list(set(reach_ids))   
    return reach_ids           

def write_json(json_object, filename):
    """Write JSON object as a JSON file to the specified filename."""

    with open(filename, 'w') as jf:
        json.dump(json_object, jf, indent=2)

def run_aws(args, cont):
    """Executes operations to retrieve reach identifiers from shapefiles hosted
    in AWS S3 bucket."""

    # Retrieve a list of S3 files
    print("Retrieving and storing list of S3 URIs.")
    s3_list = S3List()
    try:
        s3_uris = s3_list.login_and_run_query(args.shortname, args.provider, args.temporalrange)
        s3_uris = list(filter(lambda uri, cont=cont: cont in uri, s3_uris))    # Filter for continent
        write_json(s3_uris, Path(args.directory).joinpath(conf["sf_list"]))
    except Exception as e:
        print(e)
        print("Error encountered. Exiting program.")
        exit(1)

    # Retrieve SWOT S3 bucket credentials
    print("Retrieving and storing credentials.")
    try:
        s3_creds = get_s3_creds(args.provider)
    except botocore.exceptions.ClientError as e:
        print(e)
        print("Could not store S3 credentials and will not be able to run input module.")
        print("Program exiting.")
        exit(1)

    # Extract a list of reach identifiers
    print("Extracting reach identifiers from shapefiles.")
    reach_ids = extract_reach_ids(s3_uris, s3_creds)
    
    return reach_ids

def run_local(args, cont):
    """Load shapefiles in from local file system and return reach identifiers."""
    
    # Extract reach identifiers from local files
    print("Extracting reach identifiers from shapefiles.")
    reach_ids = []
    with os.scandir(Path(args.shapefiledir)) as shpfiles:
        for shpfile in shpfiles:
            if cont in shpfile.name:    # Filter by continent
                # Locate and open DBF file
                dbf_file = f"{shpfile.name.split('/')[-1].split('.')[0]}.dbf"            
                zip_file = zipfile.ZipFile(shpfile, 'r')
                with zip_file.open(dbf_file) as dbf:
                    sf = shapefile.Reader(dbf=dbf)
                    records = sf.records()
                    reach_id = {rec["reach_id"] for rec in records}                
                reach_ids.extend(list(reach_id))
    # Remove duplicates from mulitple files
    reach_ids = list(set(reach_ids))  
    return reach_ids    

def run():
    """Execute the operations needed to generate JSON data."""

    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    
    # Determine continent to run on
    cont = get_continent(args.index, Path(args.directory).joinpath(args.jsonfile))
    
    # Determine where run is taking place (local or aws)
    if args.local:
        reach_ids = run_local(args, cont)
    else:
        reach_ids = run_aws(args, cont)
    
    # Writing JSON file of reach identifiers
    json_file = Path(args.directory).joinpath(conf["reach_id_list"])
    print(f"Writing list of reach identifiers to: {json_file}")
    write_json(reach_ids, json_file)

    # Filenames
    sword_filename = f"{cont.lower()}_{conf['sword_suffix']}"
    sos_filename = f"{cont.lower()}_{conf['sos_suffix']}"
    
    # Create basin data
    print("Retrieving basin data.")
    basin = Basin(reach_ids, sword_filename, sos_filename)
    basin_data = basin.extract_data()
    json_file = Path(args.directory).joinpath(conf["basin"])
    print(f"Writing basin data to: {json_file}")
    write_json(basin_data, json_file)

if __name__ == "__main__":
    import datetime
    start = datetime.datetime.now()
    run()
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")