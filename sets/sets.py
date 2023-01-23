# Standard imports
import itertools
import json

# Third-party imports
import numpy as np
import matplotlib.pyplot as plt

#maps
import folium
from folium.plugins import BeautifyIcon as BI
import pandas as pd
import random
import webbrowser

class sets:
    """ Divide a list of reaches into inversion sets.

    Attributes
    ----------
    params: dict
        dictionary of parameters to control how sets get defined   

    
    Methods
    -------

    """
    def __init__(self,params,reaches,sword_dataset):

        self.params=params
        self.reaches=reaches
        self.sword_dataset=sword_dataset

    def extract_data_sword_continent_file(self):
        swordreachids=self.sword_dataset["reaches/reach_id"][:]

        sword_data_continent={}

        # grab sizes of the data
        dimfields=['orbits','num_domains','num_reaches']
        for field in dimfields:
            sword_data_continent[field]=self.sword_dataset['reaches'].dimensions[field].size

        # grab data    
        reachfields=['reach_id','facc','n_rch_up','n_rch_down','rch_id_up','rch_id_dn','swot_obs','swot_orbits']
        for field in reachfields:
            sword_data_continent[field]=self.sword_dataset['reaches/' + field][:]

        return swordreachids,sword_data_continent

    def extract_inversion_sets_by_reach(self,sword_data_continent,swordreachids):
        # loop over all reaches and create a set for each
        InversionSets={}
        for reach in self.reaches:
             print('finding set for reach',reach['reach_id'])
             k=np.argwhere(swordreachids == reach['reach_id'])
             k=k[0,0] # not sure why argwhere is returning this as a 2-d array. this seems inelegant
             sword_data_reach=self.pull_sword_attributes_for_reach(sword_data_continent,k)
             InversionSet=self.find_set_for_reach(sword_data_reach,swordreachids,sword_data_continent)
             InversionSet['ReachList'],InversionSet['numReaches']=self.get_reach_list(InversionSet)

             InversionSets[reach['reach_id']]=InversionSet

        return InversionSets

    def pull_sword_attributes_for_reach(self,sword_data_continent,k):
        """
        Pull out needed SWORD data from the continent dataset arrays for a particular reach    
        """

        sword_data_reach={}
        # extract all single-dimension variables, including number of orbits and reach ids needed for multi-dim vars
        for key in sword_data_continent:
            if np.shape(sword_data_continent[key]) == (sword_data_continent['num_reaches'],):
                sword_data_reach[key]=sword_data_continent[key][k]

        # extract multi-dim vars
        for key in sword_data_continent:
            if key == 'rch_id_up':
                sword_data_reach[key]=sword_data_continent[key][0:sword_data_reach['n_rch_up'],k]
            elif key == 'rch_id_dn':
                sword_data_reach[key]=sword_data_continent[key][0:sword_data_reach['n_rch_down'],k]
            elif key == 'swot_orbits':
                sword_data_reach[key]=sword_data_continent[key][0:sword_data_reach['swot_obs'],k]

        return sword_data_reach

    def find_set_for_reach(self,sword_data_reach,swordreachids,sword_data_continent):
        # ok so lets define a set:
        CheckVerbosity=False

        # 1. initialize
        InversionSet={}
        InversionSet['OriginReach']=sword_data_reach
        InversionSet['Reaches']={}
        InversionSet['Reaches'][sword_data_reach['reach_id']]=sword_data_reach
        # initially, the upstream and downstream reaches are both set to the origin reach
        InversionSet['UpstreamReach']=sword_data_reach
        InversionSet['DownstreamReach']=sword_data_reach
        # 2. check whether we can expand upstream. keep going upstream until we hit an invalid reach
        UpstreamReachIsValid=True
        n_up_add=0
        while UpstreamReachIsValid:
            kup=np.argwhere(swordreachids == InversionSet['UpstreamReach']['rch_id_up'])

            if len(kup)==0:
                  UpstreamReachIsValid=False
            else:
                  kup=kup[0,0]
                  sword_data_reach_up=self.pull_sword_attributes_for_reach(sword_data_continent,kup)
                  UpstreamReachIsValid=self.CheckReaches(sword_data_reach,sword_data_reach_up,'up',CheckVerbosity)

            if UpstreamReachIsValid:
                InversionSet['Reaches'][sword_data_reach_up['reach_id']]=sword_data_reach_up
                InversionSet['UpstreamReach']=sword_data_reach_up
                n_up_add+=1
                if n_up_add > self.params['MaximumReachesEachDirection']:
                    UpstreamReachIsValid=False

        # 3. check whether we can expand downstream. keep going downstream until we hit an invalid reach
        DownstreamReachIsValid=True
        n_dn_add=0
        while DownstreamReachIsValid:
            kdn=np.argwhere(swordreachids == InversionSet['DownstreamReach']['rch_id_dn'])
            kdn=kdn[0,0]
            sword_data_reach_dn=self.pull_sword_attributes_for_reach(sword_data_continent,kdn)
            DownstreamReachIsValid=self.CheckReaches(sword_data_reach,sword_data_reach_dn,'down',CheckVerbosity)
            if DownstreamReachIsValid:
                InversionSet['Reaches'][sword_data_reach_dn['reach_id']]=sword_data_reach_dn
                InversionSet['DownstreamReach']=sword_data_reach_dn
                n_dn_add+=1
                if n_dn_add > self.params['MaximumReachesEachDirection']:
                    DownstreamReachIsValid=False

        return InversionSet

    def CheckReaches(self,sword_data_reach,sword_data_reach_adjacent,direction,verbose):

        reach_ids=[]
        for reach in self.reaches:
             reach_ids.append(reach['reach_id'])

        AdjacentReachInReaches=sword_data_reach_adjacent['reach_id'] in reach_ids

        OrbitsAreIdentical=False
        if sword_data_reach['swot_obs']==sword_data_reach_adjacent['swot_obs']:
            OrbitsAreIdentical=list(sword_data_reach['swot_orbits'])==list(sword_data_reach_adjacent['swot_orbits'])
        AccumulationAreaDifferencePct=(sword_data_reach_adjacent['facc']-sword_data_reach['facc'])/sword_data_reach['facc']*100

        if direction == 'up':
            RiverJunctionPresent=sword_data_reach['n_rch_up']>1
        else:
            RiverJunctionPresent=False

        if direction == 'down':
            RiverJunctionPresent=sword_data_reach['n_rch_down']>1
        else:
            RiverJunctionPresent=False

        ReachesMakeAValidSet=True
        if self.params['RequireIdenticalOrbits'] and not OrbitsAreIdentical:
            ReachesMakeAValidSet=False
        if AccumulationAreaDifferencePct > self.params['DrainageAreaPctCutoff']:
            ReachesMakeAValidSet=False
        if not self.params['AllowRiverJunction'] and RiverJunctionPresent:
            ReachesMakeAValidSet=False
        if not AdjacentReachInReaches:
            ReachesMakeAValidSet=False
     
        if verbose:
            print('reach:',sword_data_reach)
            print('adjacent reach:',sword_data_reach_adjacent)
            print('drainage area pct diff:',AccumulationAreaDifferencePct)
            print('same swot coverage as adjacent reach',OrbitsAreIdentical)
            print('there is a river junction present:',RiverJunctionPresent)
            print('These reaches are a valid set:',ReachesMakeAValidSet)

        return ReachesMakeAValidSet

    def get_reach_list(self,InversionSet):

        if len(InversionSet['Reaches'].keys()) == 1:
             #then the reach list is just one reach
             ReachList=[InversionSet['OriginReach']['reach_id']]
             numReaches=1
        else:
             # make a list of the reaches in the set, in order from upstream to downstream
             ReachList=[]
             ReachList.append(InversionSet['UpstreamReach']['reach_id'])
             EndOfSetReached=ReachList[-1]==InversionSet['DownstreamReach']

             # sort the list of reach ids
             while not EndOfSetReached:
                 CurrentEndOfSet=ReachList[-1]
                 next_reach_id_downstream=InversionSet['Reaches'][CurrentEndOfSet]['rch_id_dn'][0]
                 ReachList.append(next_reach_id_downstream)
                 EndOfSetReached=ReachList[-1]==InversionSet['DownstreamReach']['reach_id']

             numReaches=len(ReachList)

        return ReachList,numReaches

    def remove_duplicate_sets(self,InversionSets):

       FoundDuplicate=True

       while FoundDuplicate:

           reaches=list(InversionSets.keys())
           reach_combos=list(itertools.combinations(reaches, 2))

           for combo in reach_combos:
                if InversionSets[combo[0]]['ReachList']==InversionSets[combo[1]]['ReachList']:
                     del InversionSets[combo[1]]
                     break
                if combo==reach_combos[-1]:
                     FoundDuplicate=False

       return InversionSets

    def remove_sets_with_non_river_reaches(self,InversionSets):

       nsets=len(InversionSets)
       SetIsBad={}

       for IS in InversionSets:
            ContainsNonRiverReach=False
            for reach in InversionSets[IS]['Reaches']:
                reachstr=str(reach)
                reachtype=reachstr[-1]
                if reachtype != '1':
                    ContainsNonRiverReach=True
            if ContainsNonRiverReach:
                SetIsBad[IS]=True
                #print(InversionSets[IS]['Reaches'])
            else:
                SetIsBad[IS]=False

       for IS in SetIsBad:
            if SetIsBad[IS]:
                del InversionSets[IS]

       return InversionSets

    def remove_small_sets(self,InversionSets):

       reaches=list(InversionSets.keys())

       # first simply remove any reach that has fewer than the minimum
       for reach in reaches:
           if InversionSets[reach]['numReaches'] < self.params['MinimumReaches']:
               del InversionSets[reach]

       # second, if it's a one-reach-set, remove if the reach exists in another set
       reaches=list(InversionSets.keys())
       SetsToRemove=[]
       for reach in reaches:
          if InversionSets[reach]['numReaches'] == 1:
              for otherreach in reaches:
                  if otherreach != reach and reach in InversionSets[otherreach]['ReachList']: 
                      SetsToRemove.append(reach)

       for reach in SetsToRemove:
          del InversionSets[reach] 

       return InversionSets

    def MKmap(self,IS):
        #firt pull centerline XY from SWORD based on IS
        CL_RID=self.sword_dataset['nodes/reach_id'][:].filled(np.nan)
        px=self.sword_dataset['nodes/x'][:].filled(np.nan)
        py=self.sword_dataset['nodes/y'][:].filled(np.nan)
 
        df=pd.DataFrame(columns=["ID", "x",'y'])
        for key in IS:
            TS=IS[key]
            RIDS=TS['ReachList']
            IS[key]['x']=[]
            IS[key]['y']=[]

            for Rs in RIDS:
                IS[key]['x'].extend(px[np.where(CL_RID == Rs)].tolist())
                IS[key]['y'].extend(py[np.where(CL_RID == Rs)].tolist())
        #package in dataframe
        for key in IS:
            for p in range(len(IS[key]['x'])):
                entry = pd.DataFrame.from_dict({
                    "ID": [key],
                    "x":  [IS[key]['x'][p]],
                    "y":  [IS[key]['y'][p]]
                    })
                df = pd.concat([df, entry], ignore_index=True)

       #plot with folium
        mm = folium.Map(
            location=[df.y.mean(), df.x.mean()],
            tiles='Stamen Toner',
            zoom_start=5
            )
        colors = [ 'red', 'blue', 'gray', 'darkred', 'lightred', 'orange', 'beige', 'green', 'darkgreen',
        'lightgreen', 'darkblue', 'lightblue', 'purple', 'darkpurple', 'pink', 'cadetblue', 'lightgray', 'black' ]
        cc=0
        for pt in range(len(df)):
            #change color with group change
            if pt>0:
                if df.ID[pt] != df.ID[pt-1]:
                    cc=random.randrange(0,len(colors),1)

            folium.CircleMarker(([df["y"][pt],df["x"][pt]]), radius=3, weight=2, color=colors[cc], fill_color=colors[cc], fill_opacity=.5).add_to(mm)

        #save the map as an html    
        fname=self.params['algo'] + '.html'
        mm.save(fname)
        webbrowser.open(fname)

    def write_inversion_set_data(self,InversionSets,OutputDir):
        #out_json = OutputDir / "sets.json"
        out_json = OutputDir / self.params['Filename']

        InversionSetsWrite=[]
        for IS in InversionSets:
             InversionSetWrite=[]
             for reach in InversionSets[IS]['ReachList']:
                 reachdict={}
                 reachdict['reach_id']=int(reach)
                 reachdict['sword']='na_sword_v11.nc'
                 reachdict['swot']=str(reach) + '_SWOT.nc'
                 reachdict['sos']='na_sword_v11_SOS.nc'
                 InversionSetWrite.append(reachdict)
             InversionSetsWrite.append(InversionSetWrite)

        with open(out_json, 'w') as json_file:
            json.dump(InversionSetsWrite, json_file, indent=2)

    def print_stats(self,InversionSets):
        # output some stats
        numReaches=[]
        for set in InversionSets:
            #print(InversionSets[set]['ReachList'])
            numReaches.append(InversionSets[set]['numReaches'])
        print('histogram of number of reaches in set')
        plt.hist(numReaches)
        plt.show()

        print('total number of reaches:',len(self.reaches))
        print('A total of', len(InversionSets.keys()),'sets were identified.')
        print('Total reaches included in sets:',sum(numReaches))

    def getsets(self):
        # extract continent data into dict
        swordreachids,sword_data_continent=self.extract_data_sword_continent_file()

        # get an inversion set for each reach
        InversionSets=self.extract_inversion_sets_by_reach(sword_data_continent,swordreachids)

        # remove duplicate sets
        InversionSets=self.remove_duplicate_sets(InversionSets)

        # remove sets with non-river reaches
        InversionSets=self.remove_sets_with_non_river_reaches(InversionSets)

        # remove sets with too few reaches
        InversionSets=self.remove_small_sets(InversionSets)

        # stats
        self.print_stats(InversionSets)

        # map
        self.MKmap(InversionSets)
    
        return InversionSets


