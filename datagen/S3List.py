# Standard imports
import base64
from http.cookiejar import CookieJar
import json
from socket import gethostname, gethostbyname
from urllib import request

# Third-party imports
import boto3
import botocore
import requests

class S3List:
    """Class used to query and download from PO.DAAC's CMR API."""

    CMR = "cmr.earthdata.nasa.gov"
    URS = "urs.earthdata.nasa.gov"

    def __init__(self):
        self._token = None
        
    def get_creds(self, s3_endpoint, edl_username, edl_password):
        """Request and return temporary S3 credentials.
        
        Taken from: https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME
        """
        
        login = requests.get(
            s3_endpoint, allow_redirects=False
        )
        login.raise_for_status()

        auth = f"{edl_username}:{edl_password}"
        encoded_auth  = base64.b64encode(auth.encode('ascii'))

        auth_redirect = requests.post(
            login.headers['location'],
            data = {"credentials": encoded_auth},
            headers= { "Origin": s3_endpoint },
            allow_redirects=False
        )
        auth_redirect.raise_for_status()
        final = requests.get(auth_redirect.headers['location'], allow_redirects=False)
        results = requests.get(s3_endpoint, cookies={'accessToken': final.cookies['accessToken']})
        results.raise_for_status()
        return json.loads(results.content)       
        
    def get_s3_creds(self, s3_endpoint, edl_username, edl_password, key):
        """Retreive S3 credentials from endpoint, write to SSM parameter store
        and return them."""
        
        s3_creds = self.get_creds(s3_endpoint, edl_username, edl_password)

        ssm_client = boto3.client('ssm', region_name="us-west-2")

        retry_cnt = 5
        while retry_cnt != 0:
            try:
                response = ssm_client.put_parameter(
                    Name="s3_creds_key",
                    Description="Temporary SWOT S3 bucket key",
                    Value=s3_creds["accessKeyId"],
                    Type="SecureString",
                    KeyId=key,
                    Overwrite=True,
                    Tier="Standard"
                )
                response = ssm_client.put_parameter(
                    Name="s3_creds_secret",
                    Description="Temporary SWOT S3 bucket secret",
                    Value=s3_creds["secretAccessKey"],
                    Type="SecureString",
                    KeyId=key,
                    Overwrite=True,
                    Tier="Standard"
                )
                response = ssm_client.put_parameter(
                    Name="s3_creds_token",
                    Description="Temporary SWOT S3 bucket token",
                    Value=s3_creds["sessionToken"],
                    Type="SecureString",
                    KeyId=key,
                    Overwrite=True,
                    Tier="Standard"
                )
                response = ssm_client.put_parameter(
                    Name="s3_creds_expiration",
                    Description="Temporary SWOT S3 bucket expiration",
                    Value=s3_creds["expiration"],
                    Type="SecureString",
                    KeyId=key,
                    Overwrite=True,
                    Tier="Standard"
                )
                retry_cnt = 0
            except:
                retry_cnt -= 1


        return s3_creds

    def login(self):
        """Log into Earthdata and set up request library to track cookies.
        
        Raises an exception if can't access SSM client.
        """
        
        try:
            ssm_client = boto3.client('ssm', region_name="us-west-2")
            username = ssm_client.get_parameter(Name="edl_username", WithDecryption=True)["Parameter"]["Value"]
            password = ssm_client.get_parameter(Name="edl_password", WithDecryption=True)["Parameter"]["Value"]
        except botocore.exceptions.ClientError as error:
            raise error
        
        # Create Earthdata authentication request
        manager = request.HTTPPasswordMgrWithDefaultRealm()
        manager.add_password(None, self.URS, username, password)
        auth = request.HTTPBasicAuthHandler(manager)

        # Set up the storage of cookies
        jar = CookieJar()
        processor = request.HTTPCookieProcessor(jar)

        # Define an opener to handle fetching auth request
        opener = request.build_opener(auth, processor)
        request.install_opener(opener)
        
        return username, password

    def get_token(self):
        """Get CMR authentication token for searching records.
        
        Parameters
        ----------
        client_id: str
            client identifier to obtain token
        ip_address: str
            client's IP address
        """
        
        try:
            ssm_client = boto3.client('ssm', region_name="us-west-2")
            self._token = ssm_client.get_parameter(Name="bearer--edl--token", WithDecryption=True)["Parameter"]["Value"]
        except botocore.exceptions.ClientError as error:
            raise error

    def run_query(self, shortname, provider, temporal_range, continent):
        """Run query on collection referenced by shortname from provider."""

        url = f"https://{self.CMR}/search/granules.umm_json"
        params = {
                    "provider" : provider, 
                    "ShortName" : shortname, 
                    "token" : self._token,
                    "scroll" : "true",
                    "page_size" : 2000,
                    "sort_key" : "start_date",
                    "temporal" : temporal_range
                }
        res = requests.get(url=url, params=params)      
        coll = res.json()
        all_urls = [url["URL"] for res in coll["items"] for url in res["umm"]["RelatedUrls"] if url["Type"] == "GET DATA VIA DIRECT ACCESS"]
        s3_urls = list(filter(lambda url: self.continent_filter(url, continent), all_urls))
        return s3_urls
    
    def continent_filter(self, url, continent):
        """Filter by continent and for zip files."""
        
        if continent in url and url[-3:] == 'zip':
            return True
        else:
            return False
    
    def login_and_run_query(self, short_name, provider, temporal_range, continent, s3_endpoint, key):
        """Log into CMR and run query to retrieve a list of S3 URLs."""

        try:
            # Login and retrieve token
            username, password = self.login()
            s3_creds = self.get_s3_creds(s3_endpoint, username, password, key)
            client_id = "podaac_cmr_client"
            hostname = gethostname()
            ip_addr = gethostbyname(hostname)
            self.get_token()

            # Run query
            s3_urls = self.run_query(short_name, provider, temporal_range, continent)
                        
        except Exception as error:
            raise error
        else:
            # Return list and s3 endpoint credentials
            return s3_urls, s3_creds
        
    def get_s3_uris_sim(self):
        """Get a list of S3 URIs for S3-hosted simulated data."""
        
        # Get S3 credentials
        ssm_client = boto3.client('ssm', region_name="us-west-2")
        creds = {}
        try:
            creds["accessKeyId"] = ssm_client.get_parameter(Name="s3_creds_key", WithDecryption=True)["Parameter"]["Value"]
            creds["secretAccessKey"] = ssm_client.get_parameter(Name="s3_creds_secret", WithDecryption=True)["Parameter"]["Value"]
            creds["sessionToken"] = ssm_client.get_parameter(Name="s3_creds_token", WithDecryption=True)["Parameter"]["Value"]
        except botocore.exceptions.ClientError as e:
            raise e
    
        session = boto3.Session(
            aws_access_key_id=creds["accessKeyId"],
            aws_secret_access_key=creds["secretAccessKey"],
            )
        s3 = session.resource('s3')

        try:
            response = s3.list_objects(
                Bucket="confluence-swot",
                MaxKeys=1000
            )
        except botocore.exceptions.ClientError:
            raise
        
        return [ f"s3://confluence-swot/{shapefile['Key']}" for shapefile in response["Contents"] ], creds
