"""Script to generate: basin, reach, reach_node, cycle_pass JSON files.

Also generates a list of S3 URIs for SWOT shapefiles. Accesses PO.DAAC CMR to
generate a list.

Requires .netrc file to log into CMR API.

Command line arguments:
 -s: short name of the collection
 -t: temporal range to retrieve S3 URIs
 -p: the collection provider name

Example: python3 generate_data.py -p POCLOUD -s SWOT_SIMULATED_NA_CONTINENT_L2_HR_RIVERSP_V1 -t 2022-08-01T00:00:00Z,2022-08-22T23:59:59Z -d /home/useraccount/json_data
"""

# Standard imports
import argparse
import json
from pathlib import Path
from tkinter import E

# Third-party imports
import boto3
import botocore
import requests

# Local imports
from S3List import S3List

# Constants
S3_FILENAME = "s3_list.json"
S3_CRED_ENDPOINT = {
    'pocloud':'https://archive.podaac.earthdata.nasa.gov/s3credentials',
    'lpdaac':'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials',
    'ornldaac':'https://data.ornldaac.earthdata.nasa.gov/s3credentials',
    'gesdisc':'https://data.gesdisc.earthdata.nasa.gov/s3credentials'
}

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Retrieve a list of S3 URIs")
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
    return arg_parser

def get_s3_creds(provider):
    """Retreive S3 credentials from endpoint, write to SSM parameter store
    and return them."""

    s3_creds = requests.get(S3_CRED_ENDPOINT[provider.lower()]).json()

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
    except botocore.exceptions.ClientError:
        raise
    else:
        return s3_creds

def extract_reach_ids(shapefiles):
    """Extract reach identifiers from shapefile names and return a list.
    
    Parameters
    ----------
    shapefiles: list
        list of shapefile names
    """

    # for shapefile in shapefiles:
    #     print(shapefile)

def write_json(json_object, filename):
    """Write JSON object as a JSON file to the specified filename."""

    with open(filename, 'w') as jf:
        json.dump(json_object, jf, indent=2)


def run():
    """Execute the operations needed to generate JSON data."""

    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()

    # Retrieve a list of S3 files
    print("Retrieving list of S3 URIs.")
    s3_list = S3List()
    try:
        s3_uris = s3_list.login_and_run_query(args.shortname, args.provider, args.temporalrange)
        write_json(s3_uris, Path(args.directory).joinpath(S3_FILENAME))
    except Exception as e:
        print(e)
        print("Error encountered. Exiting program.")
        exit(1)

    # Extract a list of reach identifiers
    reach_ids = extract_reach_ids(s3_uris)

    # Retrieve SWOT S3 bucket credentials
    print("Retrieving and storing credentials.")
    try:
        s3_creds = get_s3_creds(args.provider)
    except botocore.exceptions.ClientError as e:
        print(e)
        print("Could not store S3 credentials and will not be able to run input module.")
        print("Program exiting.")
        exit(1)

if __name__ == "__main__":
    run()