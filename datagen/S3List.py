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
        except botocore.exceptions.ClientError as error:
            raise error
        else:
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

    def get_token(self, client_id, ip_address, username, password):
        """Get CMR authentication token for searching records.
        
        Parameters
        ----------
        client_id: str
            client identifier to obtain token
        ip_address: str
            client's IP address
        """

        # Post a token request and return resonse
        token_url = f"https://{self.CMR}/legacy-services/rest/tokens"
        token_xml = (f"<token>"
                        f"<username>{username}</username>"
                        f"<password>{password}</password>"
                        f"<client_id>{client_id}</client_id>"
                        f"<user_ip_address>{ip_address}</user_ip_address>"
                    f"</token>")
        headers = {"Content-Type" : "application/xml", "Accept" : "application/json"}
        self._token = requests.post(url=token_url, data=token_xml, headers=headers) \
            .json()["token"]["id"]

    def delete_token(self):
        """Delete CMR authentication token."""

        token_url = f"https://{self.CMR}/legacy-services/rest/tokens"
        headers = {"Content-Type" : "application/xml", "Accept" : "application/json"}
        try:
            res = requests.request("DELETE", f"{token_url}/{self._token}", headers=headers)
            return res.status_code
        except Exception as e:
            raise Exception(f"Failed to delete token: {e}.")

    def run_query(self, shortname, provider, temporal_range):
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
        return [url["URL"] for res in coll["items"] for url in res["umm"]["RelatedUrls"] if url["Type"] == "GET DATA VIA DIRECT ACCESS"]
    
    def login_and_run_query(self, short_name, provider, temporal_range, s3_endpoint, key):
        """Log into CMR and run query to retrieve a list of S3 URLs."""

        try:
            # Login and retrieve token
            username, password = self.login()
            s3_creds = self.get_s3_creds(s3_endpoint, username, password, key)
            client_id = "podaac_cmr_client"
            hostname = gethostname()
            ip_addr = gethostbyname(hostname)
            self.get_token(client_id, ip_addr, username, password)

            # Run query
            s3_urls = self.run_query(short_name, provider, temporal_range)

            # Clean up and delete token
            self.delete_token()
                        
        except Exception as error:
            raise error
        else:
            # Return list and s3 endpoint credentials
            return s3_urls, s3_creds
        
    def get_creds_sim(self, key):
        """Return AWS credentials for environment that hosts simulated data."""
        
        # Retrieve temporary credentials
        client = boto3.client('sts')
        response = client.get_session_token()
        creds = {
            "accessKeyId": response["Credentials"]["AccessKeyId"],
            "secretAccessKey": response["Credentials"]["SecretAccessKey"],
            "sessionToken": response["Credentials"]["SessionToken"],
            "expiration": response["Credentials"]["Expiration"].strftime("%Y-%m-%d %H:%M:%S+00:00")
        }
        
        # Store temporary credentials in parameter store    
        ssm_client = boto3.client('ssm', region_name="us-west-2")
        try:
            response = ssm_client.put_parameter(
                Name="s3_creds_key",
                Description="Temporary SWOT S3 bucket key",
                Value=creds["accessKeyId"],
                Type="SecureString",
                KeyId=key,
                Overwrite=True,
                Tier="Standard"
            )
            response = ssm_client.put_parameter(
                Name="s3_creds_secret",
                Description="Temporary SWOT S3 bucket secret",
                Value=creds["secretAccessKey"],
                Type="SecureString",
                KeyId=key,
                Overwrite=True,
                Tier="Standard"
            )
            response = ssm_client.put_parameter(
                Name="s3_creds_token",
                Description="Temporary SWOT S3 bucket token",
                Value=creds["sessionToken"],
                Type="SecureString",
                KeyId=key,
                Overwrite=True,
                Tier="Standard"
            )
            response = ssm_client.put_parameter(
                Name="s3_creds_expiration",
                Description="Temporary SWOT S3 bucket expiration",
                Value=creds["expiration"],
                Type="SecureString",
                KeyId=key,
                Overwrite=True,
                Tier="Standard"
            )
        except botocore.exceptions.ClientError:
            raise
        else:
            return creds
        
    def get_s3_uris_sim(self, s3_creds):
        """Get a list of S3 URIs for S3-hosted simulated data."""
        
        s3 = boto3.client("s3",
                          aws_access_key_id=s3_creds["accessKeyId"],
                          aws_secret_access_key=s3_creds["secretAccessKey"],
                          aws_session_token=s3_creds["sessionToken"])
        try:
            response = s3.list_objects(
                Bucket="confluence-swot",
                MaxKeys=1000
            )
        except botocore.exceptions.ClientError:
            raise
        
        return [ f"s3://confluence-swot/{shapefile['Key']}" for shapefile in response["Contents"] ]