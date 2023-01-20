"""Script to generate: lake, cycle_pass, s3_list_lake JSON files."""

# Standard imports
import json
import os
from pathlib import Path
import re

# Local imports
from conf_lake import conf
from datagen.CyclePass import CyclePass
from datagen.Lake import Lake
from datagen.S3List import S3List

def get_continent(index, json_file):
    """Retrieve continent to run datagen operations for."""
    
    i = int(index) if index != "-235" else os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
    with open(json_file) as jf:
        data = json.load(jf)
    return list(data[i].keys())[0].upper()

def strtoi(text):
    return int(text) if text.isdigit() else text

def sort_shapefiles(shapefile):
    """Sort shapefiles so that they are in ascending order."""
    
    return [ strtoi(shp) for shp in re.split(r'(\d+)', shapefile) ]

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
        if args.simulated:
            s3_creds = s3_list.get_creds_sim(args.ssmkey)
            s3_uris = s3_list.get_s3_uris_sim(s3_creds)
        else:
            s3_endpoint = conf["s3_cred_endpoints"][args.provider.lower()]
            s3_uris, s3_creds = s3_list.login_and_run_query(args.shortname, args.provider, args.temporalrange, s3_endpoint, args.ssmkey)
            s3_uris = list(filter(lambda uri, cont=cont: cont in uri and 'Prior' in uri, s3_uris))    # Filter for continent
        s3_uris.sort(key=sort_shapefiles)
        s3_json = Path(args.directory).joinpath(conf["s3_list"])
        print(f"Writing lake shapefiles to: {s3_json}.")
        write_json(s3_uris, s3_json)
    except Exception as e:
        print(e)
        print("Error encountered. Exiting program.")
        exit(1)
        
    return s3_uris, s3_creds

def run_local(args, cont):
    """Load shapefiles in from local file system and return reach identifiers."""
    
    # Extract reach identifiers from local files
    s3_json = Path(args.directory).joinpath(conf["s3_list_local"])
    print(f"Writing lake shapefiles to: {s3_json}.")
    with os.scandir(Path(args.shapefiledir)) as shpfiles:
        shp_files = [ str(Path(shpfile)) for shpfile in shpfiles if cont in shpfile.name and 'Prior' in shpfile.name ]
    shp_files.sort(key=sort_shapefiles)
    shp_json = [ str(Path(args.shapefiledir).joinpath(shp)) for shp in shp_files ]
    write_json(shp_json, s3_json)
    return shp_files

def run_lake(args):
    """Execute the operations needed to generate JSON data."""
    
    # Determine continent to run on
    cont = get_continent(args.index, Path(args.directory).joinpath(args.jsonfile))
    
    # Determine where run is taking place (local or aws)
    if args.local:
        shp_files = run_local(args, cont)
    else:
        shp_files, s3_creds = run_aws(args, cont)
    
    # Create cycle pass data
    cycle_pass = CyclePass(shp_files)
    cycle_pass_data, pass_num = cycle_pass.get_cycle_pass_data()
    json_file = Path(args.directory).joinpath(conf["cycle_passes"])
    print(f"Writing cycle pass data to: {json_file}")
    write_json(cycle_pass_data, json_file)
    json_file = Path(args.directory).joinpath(conf["passes"])
    print(f"Writing pass number data to: {json_file}")
    write_json(pass_num, json_file)
    
    # Lake identifiers
    if args.local:
        lake = Lake(shp_files)
        lake_ids = lake.extract_local()
    else:
        lake = Lake(shp_files, s3_creds)
        lake_ids = lake.extract_aws()
    json_file = Path(args.directory).joinpath(conf["lake"])
    print(f"Writing lake identifiers to: {json_file}")
    write_json(lake_ids, json_file)

if __name__ == "__main__":
    import datetime
    start = datetime.datetime.now()
    run_lake()
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")