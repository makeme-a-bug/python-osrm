"""

this files contains functions to send orders to the osrm exceeding its limit
for now the data needs to be dictionary 

"""
from . import RequestConfig
import math
from .core import table
import time
import pandas as pd


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


#only supported for pandas dataframe
def tableX(coords_src, coords_dest=None,
          ids_origin=None, ids_dest=None,
          minutes=False, annotations='duration',
          url_config=RequestConfig, send_as_polyline=True,limit=100):

    ids_origin = ids_origin if ids_origin is not None else list(range(len(coords_src)))
    dest_origin = ids_dest if ids_dest is not None else ids_origin
    coords_src_division = None
    ids_origin_division = None
    coords_dest_division = None
    ids_dest_division = None

    if len(coords_src) > 100:
        coords_src_division = chunks(coords_src, 50)
        ids_origin_division = chunks(ids_origin, 50)

    
    if coords_dest is not None and coords_dest > 100:
        coords_dest_division = chunks(coords_dest, 50)
        ids_dest_division = chunks(dest_origin, 50)

    df_list = []
    if coords_dest is None:
        for i , src in enumerate(coords_src_division):
            for j , src2 in enumerate(coords_src_division[i:]):
                tt = 0
                while True:
                    try:
                        temp_list_coord = src + src2
                        temp_list_id = ids_origin_division[i]  + ids_origin_division[j]
                        time_matrix= table(temp_list_coord,ids_origin=temp_list_id,output='dataframe',annotations = annotations)
                        df_list.append(time_matrix[0])
                        break
                    except:
                        tt += 10
                        print(tt,'went to Sleep for this time')
                        time.sleep(tt)
                        pass
        
        print('Merging Data')
        main_def = pd.DataFrame(index=ids_origin,columns=ids_origin,dtype=float)
        print('totalDfs to merge',len(df_list),annotations)
        if len(df_list) > 0:
            for i in range(len(df_list)):
                cols = list(df_list[i].columns) 
                main_def.loc[(main_def.index.isin(df_list[i].index), cols)] = df_list[i][cols]
        print('Data Merged',len(df_list),annotations)


    else:
        for i , src in enumerate(coords_src_division):
            for j , src2 in enumerate(coords_dest_division[i:]):
                tt = 0
                while True:
                    try:
                        temp_list_coord = src + src2
                        temp_list_id = ids_origin_division[i]  + ids_dest_division[j]
                        time_matrix= table(temp_list_coord,ids_origin=temp_list_id,output='dataframe',annotations = annotations)
                        df_list.append(time_matrix[0])
                        break
                    except:
                        tt += 10
                        print(tt,'went to Sleep for this time')
                        time.sleep(tt)
                        pass

        print('Merging Data')
        main_def = pd.DataFrame(index=ids_origin,columns=ids_dest,dtype=float)
        print('totalDfs to merge',len(df_list),annotations)
        if len(df_list) > 0:
            for i in range(len(df_list)):
                cols = list(df_list[i].columns) 
                main_def.loc[(main_def.index.isin(df_list[i].index), cols)] = df_list[i][cols]
        print('Data Merged',len(df_list),annotations)
