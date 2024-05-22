# Standard imports
import base64
from http.cookiejar import CookieJar
import json
from urllib import request
import random
import os

# Third-party imports
import boto3
import botocore
import requests
import fnmatch
import datetime
from datetime import datetime, timedelta

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
        print('token results', results)
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
    def get_granule_links(granules):
        """Return list of granule links for either https or S3."""
        
        s3_granules = [ url["URL"] for item in granules["items"] for url in item["umm"]["RelatedUrls"] if url["Type"] == "GET DATA VIA DIRECT ACCESS" ]
            
        return s3_granules

    def generate_time_search(self, timekey):
        # timekey = "2024-01-01T00:00:00Z,2024-04-01T23:59:59Z"
        time1 = timekey.split(',')[0].split('T')[0]
        all_date = [int(i) for i in time1.split('-')]
        time2 = timekey.split(',')[1].split('T')[0]
        final_hours = [i for i in timekey.split(',')[1].split('T')[1].split(':')]
        all_date2 = [int(i) for i in time2.split('-')]


        start_date = datetime(all_date[0], all_date[1], all_date[2])
        end_date = datetime(all_date2[0], all_date2[1], all_date2[2])

        add_days = timedelta(days=30)
        add_ending_hours = timedelta(hours = int(final_hours[0]), minutes=int(final_hours[1]), seconds=int(final_hours[2][:-1]))


        start_dates = []
        ending_dates = []

        while start_date <= end_date:
            start_dates.append(start_date)
            start_date += add_days
            ending_dates.append(start_date)

        ending_dates[-1] = end_date + add_ending_hours

        parsed_dates = []

        for i in range(len(start_dates)):
            
            
            parsed_dates.append(','.join([start_dates[i].strftime('%Y-%m-%dT%H:%M:%SZ'), ending_dates[i].strftime('%Y-%m-%dT%H:%M:%SZ')]))
        return parsed_dates


    def run_query(self, shortname, provider, temporal_range):
        """Run query on collection referenced by shortname from provider."""

        # all_temporal_ranges = self.generate_time_search(temporal_range)
        # all_urls_out = []
        # for i in all_temporal_ranges:

        url = f"https://{self.CMR}/search/granules.umm_json"
        #     params = {
        #                 "provider" : provider, 
        #                 "ShortName" : shortname, 
        #                 "token" : self._token,
        #                 "scroll" : "true",
        #                 "page_size" : 2000,
        #                 "sort_key" : "start_date",
        #                 "temporal" : i
        #             }
        #     res = requests.get(url=url, params=params)  

    
        #     coll = res.json()
        #     all_urls = [url["URL"] for res in coll["items"] for url in res["umm"]["RelatedUrls"] if url["Type"] == "GET DATA VIA DIRECT ACCESS"]
        #     all_urls = [url for url in all_urls if url[-3:] == 'zip']
        #     all_urls_out.extend(all_urls)
        # return all_urls_out

        # temporal_range = f"{self.revision_start.strftime('%Y-%m-%dT%H:%M:%SZ')},{self.revision_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        params = {
            "short_name" : shortname,
            "revision_date": temporal_range,
            "page_size": 2000,
            "token" : self._token,
        }

        all_urls_out = []

        cmr_response = requests.get(url=url, params=params)
        hits = cmr_response.headers["CMR-Hits"]
        coll = cmr_response.json()
        # all_urls = self._get_granule_ur_list(cmr_response.json())
        all_urls = [url["URL"] for res in coll["items"] for url in res["umm"]["RelatedUrls"] if url["Type"] == "GET DATA VIA DIRECT ACCESS"]
        all_urls = [url for url in all_urls if url[-3:] == 'zip']
        all_urls_out.extend(all_urls)
        # total = len(self.cmr_granules.keys())
        
        if "CMR-Search-After" in cmr_response.headers.keys(): 
            search_after = cmr_response.headers["CMR-Search-After"]
        else:
            search_after = ""
        headers = {}
        while search_after:
            # print(f"Searching for more results...{total} out of {hits}")
            headers["CMR-Search-After"] = search_after
            cmr_response = requests.get(url=url, headers=headers, params=params)
            # self.cmr_granules.update(self._get_granule_ur_list(cmr_response.json()))
            coll = cmr_response.json()
            all_urls = [url["URL"] for res in coll["items"] for url in res["umm"]["RelatedUrls"] if url["Type"] == "GET DATA VIA DIRECT ACCESS"]
            all_urls = [url for url in all_urls if url[-3:] == 'zip']
            all_urls_out.extend(all_urls)
            # total = len(self.cmr_granules.keys())
            if "CMR-Search-After" in cmr_response.headers.keys(): 
                search_after = cmr_response.headers["CMR-Search-After"]
            else:
                search_after = ""
                
        # print(f"Located {len(self.cmr_granules.keys())} granules.")
        return all_urls_out
    
    def parse_duplicate_files(self, s3_urls:list):

        """
        In some cases, when shapefiles are processed more than once they leave both processings in the bucket, so we need to filter them.

        """
        parsed = []

        for i in s3_urls:
            # print(i[:-6])
            # mult_process_bool = False
            all_processings = fnmatch.filter(s3_urls, i[:-6]+'*')
            if len(all_processings) > 1:
                all_processings_nums = [int(i[-6:].replace('.zip', '')) for i in all_processings]
                padded_max = str("{:02d}".format(max(all_processings_nums)))
                max_path = fnmatch.filter(all_processings, f'*{padded_max}.zip')
                parsed.append(max_path[0])
                print('found a double', i)
            else:
                parsed.append(i)

        parsed = list(set(parsed))
        return parsed

    def login_and_run_query(self, short_name, provider, temporal_range, continent, s3_endpoint, key):
        """Log into CMR and run query to retrieve a list of S3 URLs."""

        try:
            # Login and retrieve token
            username, password = self.login()
            s3_creds = self.get_s3_creds(s3_endpoint, username, password, key)
            self.get_token()

            # Run query
            s3_urls = self.run_query(short_name, provider, temporal_range)

            # parse s3_urls 
            # s3_urls = self.parse_duplicate_files(s3_urls = s3_urls)

            # get_index = random.randrange(len(s3_urls))
    
            # print(s3_urls[get_index])
            
            # Filter by continent
            s3_urls = [s3 for s3 in s3_urls if continent in s3]

        except Exception as error:
            raise error
        else:
            # Return list and s3 endpoint credentials
            print('here are some sample urls that are returned...', s3_urls[:5])
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
