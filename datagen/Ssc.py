from multiprocessing import Pool
from pystac_client import Client  
# from collections import defaultdict    
# import json
# import geopandas
# from cartopy import crs
import geopandas as gpd
# import glob
# import netCDF4
import os
import numpy as np
import pandas as pd
from shapely.geometry import Point, LineString, shape
from itertools import repeat
import netCDF4 as ncf
from itertools import chain

class Ssc:
    """Class used to query and download from PO.DAAC's CMR API."""


    def __init__(self, args, sword_filename, sos_filename):
        """
        Parameters
        ----------
        reach_ids: List
            List of reach identifierse
        """
        
        self.reach_ids = reach_ids
        self.reach_data = []
        self.sos_filename = sos_filename
        self.sword_filename = sword_filename



    # find hls tiles given a point

    def find_hls_tiles(line_geo=False, band=False, limit=False, collections = ['HLSL30.v2.0', 'HLSS30.v2.0'], date_range = False):

        STAC_URL = 'https://cmr.earthdata.nasa.gov/stac'


        catalog = Client.open(f'{STAC_URL}/LPCLOUD/')



        # try:
        #     x, y = point[0], point[1]
        #     # print(x,y)
        # except TypeError:
        #     print("Point must be in the form of [lat,lon]")
        #     raise

        # point = geopandas.points_from_xy([x],[y])
        # point = point[0]

        # date_range = '2017-01-01T00:00:00Z/2018-12-31T23:59:59Z'

        if date_range:
    # ['2020-01-01:00:00:00Z', '..']
            search = catalog.search(
                collections=collections, intersects = line_geo, datetime=date_range, limit=limit)
        else:
            search = catalog.search(
                collections=collections, intersects = line_geo)


        # print(f'{search.matched()} Tiles Found...')


        item_collection = search.get_all_items()

        if limit:
            item_collection = item_collection[:limit]

        if band:
            links = []
            if type(band) == list:
                for i in item_collection:
                    for b in band:
                        link = i.assets[b].href
                        # print(link)
                        links.append(link)
            
            else:
                for i in item_collection:
                    link = i.assets[band].href
                    links.append(link)
        
        else:
            links =[]
            for i in item_collection:
                # print(i.assets)
                for key in i.assets:
                    if key.startswith('B'):
                        # link = i.assets[key].href.replace('https://data.lpdaac.earthdatacloud.nasa.gov/', 's3://')
                        link = i.assets[key].href

                        # print(link)
                        links.append(link)

        return links

    def find_download_links_for_reach_tiles(self, data_dir, reach_id, cont):
        try:
            lat_list, lon_list = self.get_reach_node_cords(data_dir,reach_id, cont)
            

            df = pd.DataFrame(columns=['x', 'y'])
            df['x'] = lat_list[:5]
            df['y'] = lon_list[:5]
            df['ID'] = reach_id
            geometry = [Point(xy) for xy in zip(df.x, df.y)]

            geo_df = gpd.GeoDataFrame(df, geometry=geometry)

            geo_df2 = geo_df.groupby(['ID'])['geometry'].apply(lambda x: LineString(x.tolist()))
            geo_df2 = gpd.GeoDataFrame(geo_df2, geometry='geometry')
            line_geo = list(geo_df2.geometry.unique())[0]
            links = find_hls_tiles(line_geo)

        except Exception as e:
            links = ['foo']
            print('error on ', reach_id)
            print(e)

        return list(set(links))





    def get_reach_node_cords(self, data_dir, reach_id, cont):

        lat_list, lon_list = [], []

        sword_fp = os.path.join(data_dir, f'{cont}_sword_v15.nc')
        # print(f'Searching across {len(files)} continents for nodes...')


        rootgrp = ncf.Dataset(sword_fp, "r", format="NETCDF4")

        node_ids_indexes = np.where(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U') == str(reach_id))

        if len(node_ids_indexes[0])!=0:
            for y in node_ids_indexes[0]:

                lat = float(rootgrp.groups['nodes'].variables['x'][y].data.astype('U'))
                lon = float(rootgrp.groups['nodes'].variables['y'][y].data.astype('U'))
                # all_nodes.append([lat,lon])
                lat_list.append(lat)
                lon_list.append(lon)


            rootgrp.close()

        # print(f'Found {len(all_nodes)} nodes...')
        return lat_list, lon_list












    # all_links = find_download_links_for_reach_tiles('/home/confluence/data/mnt/input/sword',23216000521)
    # print(len(all_links))



    def ssc_process_continent(self, reach_ids, cont)


        pool = Pool(processes=4)              # start 4 worker processes
        result = pool.starmap(self.find_download_links_for_reach_tiles, zip(repeat(data_dir), reach_ids, repeat(cont)))
        # cnt = 0
        # for i in result:
        #     for x in i:
        #         cnt += 1

        pool.close()

        flatten_list = list(chain.from_iterable(result))
        flatten_list = list(set(flatten_list))
        return flatten_list



    def __init__(self):
        self._token = None
        
 