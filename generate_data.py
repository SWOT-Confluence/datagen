"""Script to generate: basin, reach, reach_node, cycle_pass JSON files.

Also generates a list of S3 URIs for SWOT shapefiles. Accesses PO.DAAC CMR to
generate a list.
"""

# Standard imports
import json
import os
from pathlib import Path
import re
import traceback
import zipfile

# Third-party imports
import fsspec
import shapefile

# Local imports
from conf import conf
from datagen.Basin import Basin
from datagen.CyclePass import CyclePass
from datagen.Reach import Reach
from datagen.ReachNode import ReachNode
from datagen.S3List import S3List
from sets.getAllSets import main

def extract_ids(shpfiles, creds):
    """Extract reach identifiers from shapefile names and return a list.
    
    Parameters
    ----------
    shapefiles: list
        list of shapefile names
    """

    reach_ids = []
    node_ids = []
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
                if "Reach" in shpfile:
                    reach_id = {rec["reach_id"] for rec in records}
                    reach_ids.extend(list(reach_id))

                if "Node" in shpfile: 
                    node_id = {rec["node_id"] for rec in records}
                    node_ids.extend(list(node_id))            
            
    # Remove duplicates from multiple files
    reach_ids = list(set(reach_ids))
    reach_ids.sort()
    node_ids = list(set(node_ids))
    node_ids.sort()
    return reach_ids, node_ids

def extract_ids_local(shapefiledir, cont, outdir):
    
    # Extract reach identifiers from local files
    print("Extracting reach and node identifiers from shapefiles.")
    reach_ids = []
    node_ids = []
    shp_files = []
    with os.scandir(Path(shapefiledir)) as shpfiles:
        for shpfile in shpfiles:
            if cont in shpfile.name:    # Filter by continent
                shp_files.append(shpfile.name)
                # Locate and open DBF file
                dbf_file = f"{shpfile.name.split('/')[-1].split('.')[0]}.dbf"            
                zip_file = zipfile.ZipFile(shpfile, 'r')
                with zip_file.open(dbf_file) as dbf:
                    sf = shapefile.Reader(dbf=dbf)
                    records = sf.records()
                    if "Reach" in shpfile.name:
                        reach_id = {rec["reach_id"] for rec in records}
                        reach_ids.extend(list(reach_id))
                    if "Node" in shpfile.name:
                        node_id = {rec["node_id"] for rec in records}
                        node_ids.extend(list(node_id))
          
    # Remove duplicates from multiple files and sort
    reach_ids = list(set(reach_ids))
    reach_ids.sort()
    node_ids = list(set(node_ids))
    node_ids.sort()
    shp_files.sort(key=sort_shapefiles)
    shp_json = [ str(Path(shapefiledir).joinpath(shp)) for shp in shp_files ]
    json_file = Path(outdir).joinpath(update_json_filename(conf["s3_list_local"], cont))
    write_json(shp_json, json_file)
    return shp_files, reach_ids, node_ids

def extract_s3_uris(s3_uris, s3_creds, reach_list):
    """Extract S3 URIs from reach file subset."""
    
    # Open shapefiles and locate reach and node identifiers
    reach_ids = []
    node_ids = []
    shp_files = []
    for shpfile in s3_uris:
        # Open S3 zip file
        with fsspec.open(f"{shpfile}", mode="rb", anon=False, 
            key=s3_creds["accessKeyId"], secret=s3_creds["secretAccessKey"], 
            token=s3_creds["sessionToken"]) as shpfh:
            
                # Locate and open DBF file
                dbf_file = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"            
                zip_file = zipfile.ZipFile(shpfh, 'r')
                with zip_file.open(dbf_file) as dbf:
                    sf = shapefile.Reader(dbf=dbf)
                    records = sf.records()
                    if "Reach" in shpfile:
                        shp_reaches = {rec["reach_id"] for rec in records}
                        reach_intersection = [ value for value in shp_reaches if value in reach_list ]
                        if len(reach_intersection) > 0:
                            shp_files.append(shpfile)
                            reach_ids.extend(reach_intersection)
                    if "Node" in shpfile:
                        node_id = {rec["node_id"] for rec in records}
                        for reach_id in reach_list:
                            reach_r = re.compile(f"^{reach_id[:10]}.*")
                            node_ids.extend(list(filter(reach_r.match, node_id)))
    
    # Sort and remove duplicates
    reach_ids = list(set(reach_ids))
    reach_ids.sort()
    node_ids = list(set(node_ids))
    node_ids.sort()
    shp_files.sort(key=sort_shapefiles)
        
    return shp_files, reach_ids, node_ids

def extract_s3_uris_local(shapefiledir, cont, outdir, reach_list):
    """Extract S3 URIs from reach file subset."""
    
    print("Extracting shapefiles and node identifiers from subset.")
    # Open shapefiles and locate reach and node identifiers
    reach_ids = []
    node_ids = []
    shp_files = []
    with os.scandir(Path(shapefiledir)) as shpfiles:
        for shpfile in shpfiles:
            if cont in shpfile.name:    # Filter by continent
                # Locate and open DBF file
                dbf_file = f"{shpfile.name.split('/')[-1].split('.')[0]}.dbf"            
                zip_file = zipfile.ZipFile(shpfile, 'r')
                with zip_file.open(dbf_file) as dbf:
                    sf = shapefile.Reader(dbf=dbf)
                    records = sf.records()
                    if "Reach" in shpfile.name:
                        shp_reaches = {rec["reach_id"] for rec in records}
                        reach_intersection = [ value for value in shp_reaches if value in reach_list ]
                        if len(reach_intersection) > 0:
                            shp_files.append(shpfile.name)
                            reach_ids.extend(reach_intersection)
                    if "Node" in shpfile.name:
                        node_id = {rec["node_id"] for rec in records}
                        for reach_id in reach_list:
                            reach_r = re.compile(f"^{reach_id[:10]}.*")
                            node_ids.extend(list(filter(reach_r.match, node_id)))
    
    # Sort and remove duplicates
    reach_ids = list(set(reach_ids))
    reach_ids.sort()
    node_ids = list(set(node_ids))
    node_ids.sort()
    shp_files.sort(key=sort_shapefiles)
    
    # Write JSON file
    shp_json = [ str(Path(shapefiledir).joinpath(shp)) for shp in shp_files ]
    json_file = Path(outdir).joinpath(update_json_filename(conf["s3_list_local"], cont))
    write_json(shp_json, json_file)
    
    return shp_files, reach_ids, node_ids

def get_continent(index, json_file):
    """Retrieve continent to run datagen operations for."""
    
    i = int(index) if index != -235 else int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    with open(json_file) as jf:
        data = json.load(jf)
    return list(data[i].keys())[0].upper()

def get_subset(json_file):
    """Retrieve subset data to run datagen operations for."""
    
    with open(json_file) as jf:
        data = json.load(jf)
    return data
   
def strtoi(text):
    return int(text) if text.isdigit() else text

def sort_shapefiles(shapefile):
    """Sort shapefiles so that they are in ascending order."""
    
    return [ strtoi(shp) for shp in re.split(r'(\d+)', shapefile) ]

def write_json(json_object, filename):
    """Write JSON object as a JSON file to the specified filename."""

    with open(filename, 'w') as jf:
        json.dump(json_object, jf, indent=2)

def run_aws(args, cont, subset, reach_list = None):
    """Executes operations to retrieve reach identifiers from shapefiles hosted
    in AWS S3 bucket."""

    # Retrieve a list of S3 files
    print("Retrieving and storing list of S3 URIs.")
    s3_list = S3List()
    try:
        if args.simulated:
            s3_uris, s3_creds = s3_list.get_s3_uris_sim()
        else:
            s3_endpoint = conf["s3_cred_endpoints"][args.provider.lower()]
            s3_uris, s3_creds = s3_list.login_and_run_query(args.shortname, args.provider, args.temporalrange, s3_endpoint, args.ssmkey)
            s3_uris = list(filter(lambda uri, cont=cont: cont in uri, s3_uris))    # Filter for continent
        s3_uris.sort(key=sort_shapefiles)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        print("Error encountered. Exiting program.")
        exit(1)

    # Extract a list of reach identifiers
    if subset == False:
        print("Extracting reach and node identifiers from shapefiles.")
        reach_ids, node_ids = extract_ids(s3_uris, s3_creds)
        
    # Extract shapefiles and node identifiers for reach identifier subset
    else:
        print("Extracting shapefiles and node identifiers from subset.")
        s3_uris, reach_ids, node_ids = extract_s3_uris(s3_uris, s3_creds, reach_list)
    
    # Write shapefile json
    json_file = Path(args.directory).joinpath(update_json_filename(conf["s3_list"], cont))
    write_json(s3_uris, json_file)
    
    # Creat a list of only shapfile names
    shp_files = [shp.split('/')[-1].split('.')[0] for shp in s3_uris]
    return shp_files, reach_ids, node_ids

def update_json_filename(json_file, continent):
    """Update JSON file name to include continent."""
    
    filename_pieces = json_file.split('.')
    cont_name = f"{filename_pieces[0]}_{continent.lower()}.{filename_pieces[1]}"
    return cont_name

def run_local(args, cont, subset, reach_list=None):
    """Load shapefiles in from local file system and return reach identifiers."""
    
    # Extract reach identifiers
    if subset == False:
        shp_files, reach_ids, node_ids = extract_ids_local(args.shapefiledir, cont, args.directory)
    
    # Extract shapefiles and node identifiers for reach identifier subset
    else:
        shp_files, reach_ids, node_ids = extract_s3_uris_local(args.shapefiledir, cont, args.directory, reach_list)
    
    return shp_files, reach_ids, node_ids

def run_river(args):
    """Execute the operations needed to generate JSON data."""
    
    # Determine continent to run on
    cont = get_continent(args.index, Path(args.directory).joinpath(args.jsonfile))
    
    # Determine if global or subset run
    if args.subsetfile:
        reach_list = get_subset(args.subsetfile)
        subset = True
    else:
        reach_list = []
        subset = False
    
    # Determine where run is taking place (local or aws)
    if args.local:
        shp_files, reach_ids, node_ids = run_local(args, cont, subset, reach_list)
    else:
        shp_files, reach_ids, node_ids = run_aws(args, cont, subset, reach_list)
    
    # Create cycle pass data
    cycle_pass = CyclePass(shp_files)
    cycle_pass_data, pass_num = cycle_pass.get_cycle_pass_data()
    json_file = Path(args.directory).joinpath(update_json_filename(conf["cycle_passes"], cont))
    print(f"Writing cycle pass data to: {json_file}")
    write_json(cycle_pass_data, json_file)
    json_file = Path(args.directory).joinpath(update_json_filename(conf["passes"], cont))
    print(f"Writing pass number data to: {json_file}")
    write_json(pass_num, json_file)
    
    # Filenames
    sword_filename = f"{cont.lower()}_{conf['sword_suffix']}"
    sos_filename = f"{cont.lower()}_{conf['sos_suffix']}"
    
    # Create basin data
    print("Retrieving basin data.")
    basin = Basin(reach_ids, sword_filename, sos_filename)
    basin_data = basin.extract_data()
    json_file = Path(args.directory).joinpath(update_json_filename(conf["basin"], cont))
    print(f"Writing basin data to: {json_file}")
    write_json(basin_data, json_file)
    
    # Create reach data
    print("Retrieving reach data.")
    reach = Reach(reach_ids, sword_filename, sos_filename)
    reach_data = reach.extract_data()
    json_file = Path(args.directory).joinpath(update_json_filename(conf["reach"], cont))
    print(f"Writing reach data to: {json_file}")
    write_json(reach_data, json_file)
    
    # Create reach node data
    print("Retrieving reach node data.")
    reach_node = ReachNode(reach_ids, node_ids)
    reach_node_data = reach_node.extract_data()
    json_file = Path(args.directory).joinpath(update_json_filename(conf["reach_node"], cont))
    print(f"Writing reach node data to: {json_file}")
    write_json(reach_node_data, json_file)   
    
    # Create sets 
    print("Retrieving set data.")
    main(args, cont)

if __name__ == "__main__":
    import datetime
    start = datetime.datetime.now()
    run_river()
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")