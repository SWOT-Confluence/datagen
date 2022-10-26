"""Script to generate: basin, reach, reach_node, cycle_pass JSON files.

Also generates a list of S3 URIs for SWOT shapefiles. Accesses PO.DAAC CMR to
generate a list.

Requires .netrc file to log into CMR API.

Command line arguments:
 -s: short name of the collection
 -t: temporal range to retrieve S3 URIs
 -p: the collection provider name

Example: python3 S3List_run.py -p POCLOUD -s SWOT_SIMULATED_NA_CONTINENT_L2_HR_RIVERSP_V1 -t 2022-08-01T00:00:00Z,2022-08-22T23:59:59Z -d /home/useraccount/json_data
"""

# Standard imports
import argparse
import json
from pathlib import Path

# Local imports
from S3List import S3List

# Constants
S3_FILENAME = "s3_list.json"

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

if __name__ == "__main__":
    run()