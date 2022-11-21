# Standard imports
import zipfile

# Third-party imports
import fsspec
import requests
import pandas as pd
import shapefile

# Local imports
from conf_lake import conf

class Lake:
    """
    A class that maps lake identifiers from the 'Priors' SWOT shapefiles.
    
    Assumes use of 'Priors' shapefiles and not 'Obs' or 'Unassigned'.
    

    Attributes
    ----------
    lake_ids: list
        list of lake identifiers
    
    Methods
    -------
    extract_data()
        extracts reach identifier and maps to file name 
    """

    def __init__(self, shapefiles, provider):
        """
        Parameters
        ----------
        reach_ids: List
            List of reach identifierse
        """
        
        self.creds = requests.get(conf["s3_cred_endpoints"][provider.lower()]).json()
        self.lake_ids = []
        self.shapefiles = shapefiles

    def extract_aws(self):
        """Extracts lake identifier from shapefiles stored in AWS S3 bucket.
        
        Populates lake_id attribute.
        """

        for shpfile in self.shapefiles:
            # Open S3 zip file
            with fsspec.open(f"{shpfile}", mode="rb", anon=False, 
                key=self.creds["accessKeyId"], secret=self.creds["secretAccessKey"], 
                token=self.creds["sessionToken"]) as shpfh:

                # Locate and open DBF file
                dbf_file = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"            
                zip_file = zipfile.ZipFile(shpfh, 'r')
                with zip_file.open(dbf_file) as dbf:
                    sf = shapefile.Reader(dbf=dbf)
                    fieldnames = [f[0] for f in sf.fields[1:]]
                    records = sf.records()
                    df = pd.DataFrame(columns=fieldnames, data=records)
                    self.lake_ids.extend(df["lake_id"].tolist())        
                
        # Remove duplicates from multiple files
        self.lake_ids = list(set(self.lake_ids))
        self.lake_ids.sort()
        return self.lake_ids
    
    def extract_local(self):
        """Extracts lake identifier from shapefiles on local file system.
        
        Populates lake_id attribute.
        """
        
        for shpfile in self.shapefiles:

            # Locate and open DBF file
            dbf_file = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"            
            zip_file = zipfile.ZipFile(shpfile, 'r')
            with zip_file.open(dbf_file) as dbf:
                sf = shapefile.Reader(dbf=dbf)
                fieldnames = [f[0] for f in sf.fields[1:]]
                records = sf.records()
                df = pd.DataFrame(columns=fieldnames, data=records)
                self.lake_ids.extend(df["lake_id"].tolist())        
                
        # Remove duplicates from multiple files
        self.lake_ids = list(set(self.lake_ids))
        self.lake_ids.sort()
        return self.lake_ids