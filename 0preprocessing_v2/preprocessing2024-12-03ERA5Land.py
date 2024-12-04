#!/usr/bin/env python
# coding: utf-8

# ## Preparing data

# In[2]:


import xarray as xr
import geopandas as gpd
from dask.distributed import Client, LocalCluster
from datetime import datetime, timedelta
from functools import partial
from PyStemmusScope import variable_conversion as vc
from rasterio.warp import reproject, Resampling
import numpy as np
import glob
import dask.array as da
import pandas as pd
from dask_jobqueue import SLURMCluster


# In[ ]:


import argparse

# 设置命令行参数
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True, help='Year for the data')
args = parser.parse_args()

year = args.year

# 使用 year 和 month 执行你的逻辑
print(f"Processing data for {year}",flush=True)


# In[4]:


parent_in_path = f"/gpfs/work2/0/ttse0619/qianqian/global_data_Qianqian/1input_data"
data_paths = {
            "era5land": f"{parent_in_path}/{year}global/era5land/*.nc",
            "lai": f"{parent_in_path}/{year}global/lai_v2/*.nc",
            "ssm": f"{parent_in_path}/{year}global/ssm/GlobalGSSM11km2014_20240214.tif",
            "co2": f"{parent_in_path}/{year}global/co2/CAMS_CO2_2003-2020.nc",
            "landcover": f"{parent_in_path}/landcover/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2013-v2.0.7cds.nc",
            "vcmax": f"{parent_in_path}/Vcmax/TROPOMI_Vmax_Tg_mean.tif",
            "canopyheight": f"{parent_in_path}/canopy_height/canopy_height_11kmGlobal20240215.tif",
            }


# In[5]:


def era5_preprocess(ds):    
    # Convert the longitude coordinates from [0, 360] to [-180, 180]
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
    
    return ds

def fix_coords(ds):
    if 'band' in ds.dims:
        ds = ds.rename_dims({'band': 'time'})
        ds = ds.rename_vars({'band': 'time'})

    if 'x' in ds.dims and 'y' in ds.dims:
        ds = ds.rename_dims({'x': 'longitude', 'y': 'latitude'})
        ds = ds.rename_vars({'x': 'longitude', 'y': 'latitude'})
        
    elif 'lon' in ds.dims and 'lat' in ds.dims:
        ds = ds.rename_dims({'lon': 'longitude', 'lat': 'latitude'})
        ds = ds.rename_vars({'lon': 'longitude', 'lat': 'latitude'})
    return ds


# In[ ]:


cluster = SLURMCluster(
    name='dask-worker',
    cores=16,
    processes=16,
    queue='fat',
    memory='120GiB',
    local_directory='$TMPDIR',
    walltime='4:00:00'
)
cluster.scale(jobs=4)
client = Client(cluster)
print(client.dashboard_link, flush=True)


# In[9]:


era5land = xr.open_mfdataset(data_paths['era5land'], preprocess=era5_preprocess, chunks={'longitude': 250, 'latitude': 250})
# era5land = era5land.chunk({'time': 750})
era5land = era5land.sortby(['longitude', 'latitude'])
era5land = era5land.chunk(
    time=750, 
    longitude=250, 
    latitude=250
)
# # svae to zarr
out_path = f"{parent_in_path}/{year}global/era5land/{'era5land'}_{year}.zarr"
era5land.to_zarr(out_path, mode='w')


# In[ ]:


client.shutdown()

