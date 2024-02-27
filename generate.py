"""
Execute either river or lake input operations to create NetCDF files from
SWOT shapefiles.

Local execution option available, if you run locally, please place shapefiles in
the --d or --directory referenced in command line arguments.

Command line arguments:
 -i: index to locate continent in JSON file
 -s: short name of the collection
 -t: temporal range to retrieve S3 URIs
 -p: the collection provider name
 -d: where to locate and save JSON data
 -k : unique SSM encryption key identifier
 -o: indicate run on simulated data (optional)
 -l: indicates local run (optional)
 -j: name of JSON file (optional)f
 -f: name of shapefile directory for local runs (optional)
 -u: Path to JSON file with list of reaches to subset
 -a: Path to JSON ifle with list of passes to subset

River Example: python3 generate.py -c river -i 3 -p POCLOUD -s SWOT_SIMULATED_NA_CONTINENT_L2_HR_RIVERSP_V1 -t 2022-08-01T00:00:00Z,2022-08-22T23:59:59Z -d /home/useraccount/json_data
Lake Example: python3 generate.py -c lake -i 3 -p POCLOUD -s SWOT_SIMULATED_NA_CONTINENT_L2_HR_RIVERSP_V1 -t 2022-08-01T00:00:00Z,2022-08-22T23:59:59Z -d /home/useraccount/json_data
Local Docker Example: sudo docker run -v /mnt/external/data/reprocessed_data/tars:/data/ datagen -i 0 -c river -p POCLOUD -s SWOT_L2_HR_RiverSP_1.1 -t 2023-01-01T00:00:00Z,2023-10-15T23:59:59Z -d /data -k 1416df6c-7a20-46a1-949d-d26975acfdd0 -l -f /data/ -b
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
                            type=int,
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
    arg_parser.add_argument("-k",
                            "--ssmkey",
                            type=str,
                            help="Unique SSM encryption key identifier.")
    arg_parser.add_argument("-o",
                            "--simulated",
                            help="Indication to run on simulated data",
                            action="store_true")
    arg_parser.add_argument("-l",
                            "--local",
                            help="Indicate local run",
                            action="store_true")
    arg_parser.add_argument("-f",
                            "--shapefiledir",
                            type=str,
                            help="Directory of local shapefiles")
    arg_parser.add_argument("-u",
                            "--subsetfile",
                            help="Path to JSON file with list of reaches to subset",
                            type=str)
    arg_parser.add_argument("-a",
                            "--passlist",
                            help="Path to JSON file with list of passes to subset",
                            type=str)
    arg_parser.add_argument("-w",
                            "--swordpatch",
                            help="Path to JSON file that patches SWORD topology issues",
                            type=str)
    arg_parser.add_argument("-b",
                            "--hls",
                            help="indicate the generation of hls target files for ssc prediction",
                            action="store_true")
    arg_parser.add_argument("-hpc",
                            "--hpc",
                            help="Indicates running on an HPC",
                            action="store_true")
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