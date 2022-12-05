"""
Execute either river or lake input operations to create NetCDF files from
SWOT shapefiles.
"""
# Standard imports
import argparse
import datetime

# Local imports
from generate_data import run_river
from generate_data_lake import run_lake

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Retrieve a list of S3 URIs")
    arg_parser.add_argument("-i",
                            "--index",
                            type=str,
                            help="Index value to select continent to run on")
    arg_parser.add_argument("-c",
                            "--context",
                            type=str,
                            choices=["river", "lake"],
                            help="Context to generate data for: 'river' or 'lake'",
                            default="river")
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

def main():
    """Main function to execute input operations."""
    
    start = datetime.datetime.now()
    
    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    
    if args.context == "river":
        run_river(args)
    if args.context == "lake":
        run_lake(args)
        
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")
    
if __name__ == "__main__":
    main()