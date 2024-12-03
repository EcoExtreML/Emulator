#!/usr/bin/env python
# coding: utf-8

# ## 0)import libraries 

# In[44]:


import os
import pickle

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr

from PyStemmusScope import variable_conversion as vc
from sklearn.preprocessing import OneHotEncoder
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster
import calendar


# ## 1)settings for Dask

# Setup a Dask cluster on 6 * 16 = 96 cores (6 * 4 = 24 workers with 4 threads each) and 6 * 120 GB = 720 GB memory in total ('fat' nodes on Snellius):

# In[ ]:


NWORKERS = 8
cluster = SLURMCluster(
    name='dask-worker',
    cores=16,
    processes=4,
    queue='fat',
    memory='120GiB',
    local_directory='$TMPDIR',
    walltime='1:00:00'
)
cluster.scale(jobs=NWORKERS)


# In[3]:


client = Client(cluster)
client.wait_for_workers(NWORKERS)


# ## 2)define working path, load trained model, define functions

# In[ ]:


import argparse

# 设置命令行参数
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True, help='Year for the data')
parser.add_argument('--month', type=int, required=True, help='Month for the data')

args = parser.parse_args()

year = args.year
month = args.month

# 使用 year 和 month 执行你的逻辑
print(f"Processing data for {year}-{month:02d}")


# In[244]:


# 动态获取该月的最后一天
end_day = calendar.monthrange(year, month)[1]  # 获取该月最后一天（28, 29, 30, 或 31）
# 确保生成有效日期
start_time = f"{year}-{month:02d}-01"
end_time = f"{year}-{month:02d}-{end_day}"

ROOT_DIR = '/gpfs/work2/0/ttse0619'
DATA_DIR = f'{ROOT_DIR}/qianqian/global_data_Qianqian/1input_data'
ERA5_PATH = f'{DATA_DIR}/{year}global/era5land/era5land_{year}.zarr'
LAI_PATH = f'{DATA_DIR}/{year}global/lai_v2/lai_v2_{year}.zarr'
SSM_PATH = f'{DATA_DIR}/{year}global/ssm/ssm_global_{year}.zarr'
CO2_PATH = f'{DATA_DIR}/{year}global/co2/co2_{year}.zarr'
LANDCOVER_PATH = f'{DATA_DIR}/landcover/landcover.zarr'
IGBP_CLASS_PATH = f'{DATA_DIR}/landcover/IGBP11unique.csv'
IGBP_TABLE_PATH = f'{DATA_DIR}/landcover//lccs_to_igbp_table.csv' 
hc_PATH = f'{DATA_DIR}/canopy_height/hc_global.zarr'
VCMAX_PATH = f'{DATA_DIR}/Vcmax/vcmax_fillnan.zarr'
MODEL_PATH = f'{ROOT_DIR}/qianqian/global_data_Qianqian/3RF_train/hourlyFluxes_OI2024-11-05.pkl'

LEH_PATH = f'{ROOT_DIR}/qianqian/global_data_Qianqian/5output_data/global_fluxes_RFOI_{year}-{month:02d}_v1.3.zarr'


# In[245]:


# function for loading the trained model
def load_model(path):
    # load trained RF model, better use not parallel model with Dask
    with open(path, 'rb') as f:
        rfLEHmulti = pickle.load(f)
    return rfLEHmulti


# ## 3) read data

# In[246]:


## 0) read era5land data
era5 = xr.open_zarr(ERA5_PATH)
# round coordinates to facilitate matching with other datasets
era5 = era5.assign_coords(
    longitude=era5.longitude.round(4),
    latitude=era5.latitude.round(4)
).sel(time=slice(pd.to_datetime(start_time) - pd.Timedelta(hours=1), end_time))


# In[247]:


# calculate the Rin and Rli difference for every hour
ssrd = era5['ssrd'] / 3600
Rin = ssrd.diff("time")
Rin[0::24] = ssrd[1::24]

strd = era5['strd'] / 3600
Rli = strd.diff("time")
Rli[0::24] = strd[1::24]


# In[248]:


Precip_msr = era5['tp'].diff("time")  #xr.concat([all1['ssrd'].isel(time=0),all1['ssrd']], dim="time")
Precip_msr[0::24] = era5['tp'][1::24] # assign the original values in t01


# In[217]:


p = era5["sp"][1:]/100  # Pa -> hPa
Ta = era5["t2m"][1:] - 273.15  # K -> degC
ea = vc.calculate_es(era5["d2m"][1:] - 273.15)*10 #kPa -> hPa
u = (era5["u10"][1:] ** 2 + era5["v10"][1:] ** 2) ** 0.5
Precip_msr = Precip_msr*1000 # mm


# In[230]:


### 1) read LAI data
LAI = xr.open_zarr(LAI_PATH)#.sel(time=str(year))
LAI = LAI.assign_coords(
    longitude=LAI.longitude.round(4),
    latitude=LAI.latitude.round(4)
)['LAI']
# 获取离 start_time 最近的时间
start_time_nearest = LAI.sel(time=start_time, method='nearest').time
end_time_nearest = LAI.sel(time=end_time, method='nearest')

# 获取 end_time 后的下一个时间点
# 在 LAI.time 中选择第一个大于 end_time_nearest.time 的时间点
end_time_next = LAI.time[LAI.time > end_time_nearest.time].min()

# 使用计算得到的前一期和后一期时间来选择数据
LAI = LAI.sel(time=slice(start_time_nearest, end_time_next))


# In[231]:


LAI['longitude'] = LAI['longitude'].astype('float32')


# In[232]:


# # # INTERPOLATION CREATES A SINGLE CHUNK IN TIME!
LAI = LAI.resample(time='1D').interpolate('linear')


# In[233]:


import dask.config
with dask.config.set({"array.slicing.split_large_chunks": True}):
    LAI = LAI.resample(time='1H').nearest()


# In[236]:


LAI = LAI.sel(time=slice(Rin.time[0], Rin.time[-1]))


# In[194]:


### 2) read SSM data
SSM = xr.open_zarr(SSM_PATH)
SSM = SSM.assign_coords(
    longitude=SSM.longitude.round(4),
    latitude=SSM.latitude.round(4)
).sel(time=slice(start_time, pd.to_datetime(end_time) + pd.Timedelta(days=1)))
SSM = SSM['SSM']


# In[195]:


# # INTERPOLATION CREATES A SINGLE CHUNK IN TIME!
SSM = SSM.resample(time='1H').interpolate('linear')/1000 


# In[196]:


SSM = SSM.reindex(time=Rin.time, method='ffill')


# In[197]:


### 3) read CO2 data
CO2 = xr.open_zarr(CO2_PATH)
CO2 = CO2.assign_coords(
    longitude=CO2.longitude.round(4),
    latitude=CO2.latitude.round(4)
).sel(time=slice(start_time, pd.to_datetime(end_time) + pd.Timedelta(days=1)))
CO2 = CO2['co2']


# In[198]:


with dask.config.set({"array.slicing.split_large_chunks": True}):
    CO2 = CO2.resample(time='1H').nearest()


# In[199]:


CO2 = CO2.reindex(time=Rin.time, method='bfill')


# **Done datasets up to here!** Moving on with landcover
# 
# ----

# In[200]:


## 4) read IGBP data
hc = xr.open_zarr(hc_PATH)
hc = hc.assign_coords(
    longitude=hc.longitude.round(4),
    latitude=hc.latitude.round(4)
)['hc']


# In[201]:


## 5) read Vcmax data
vcmax = xr.open_zarr(VCMAX_PATH)
vcmax = vcmax.assign_coords(
    longitude=vcmax.longitude.round(4),
    latitude=vcmax.latitude.round(4)
)['vcmax']


# ---

# In[202]:


## 6) read IGBP data
landcover = xr.open_zarr(LANDCOVER_PATH)
landcover = landcover.assign_coords(
    longitude=landcover.longitude.round(4),
    latitude=landcover.latitude.round(4)
)['lccs_class']


# In[203]:


# read IGBP unique values
training_testing_append = pd.read_csv(IGBP_CLASS_PATH)['0'].unique()
# read the table for converting landcover to IGBP
IGBP_table = pd.read_csv(IGBP_TABLE_PATH)


# In[204]:


def landcover_to_igbp(landcover, IGBP_table, training_testing_append):
    get_IGBP = np.vectorize(IGBP_table.set_index("lccs_class").T.to_dict('records')[0].get)
    IGBP = get_IGBP(landcover.values) 
    IGBP_all = pd.DataFrame(
        columns=[f'IGBP_veg_long{i}' for i in range(1, 12)]
    )
    
    # define one hot encoding for IGBP
    encoder = OneHotEncoder(
        categories=[training_testing_append],
        sparse=False,
        handle_unknown="ignore"
    )
    
    # transform data
    aa = encoder.fit_transform(IGBP.reshape(IGBP.shape[0]*IGBP.shape[1], 1))
    
    # assign 23-D IGBP into 23 columns
    for i in range(1, 12):
        IGBP_all[f'IGBP_veg_long{i}'] = aa[:,i-1]
    return IGBP_all


# ## 4) chunk all the input variables

# In[205]:


ds = xr.Dataset()

ds = ds.assign(
    Rin=Rin,
    Rli=Rli,
    p=p,
    Ta=Ta,
    ea=ea,
    u=u,
    Precip=Precip_msr,
    LAI=LAI,
    CO2=CO2,
    SSM=SSM,
)

ds = ds.to_array()

ds = ds.chunk(time=125, variable=-1)


# In[206]:


ds


# ## 5) predict fluxes with map_blocks

# In[27]:


INPUT_VARIABLES = [
    'Rin', 'Rli', 'p', 'Ta', 'ea', 'u', 'CO2','LAI','Vcmo','hc', 'Precip',  
    'SSM',  *[f'IGBP_veg_long{i}' for i in range(1, 12)]
]
OUTPUT_VARIABLES = ['Rn_OI','LE_OI','H_OI','updated_Gtot','Actot', 'SIF685', 'SIF740']
# OUTPUT_VARIABLES = ['Rn_OI_daily_std_SG', 'LE_OI_daily_std_SG', 'H_OI_daily_std_SG','updated_Gtot_daily_std_SG','Actot_daily_std_SG', 'SIF685_daily_std_SG','SIF740_daily_std_SG']
# OUTPUT_VARIABLES = ['Rn_OI_sameTime_std_SG', 'LE_OI_sameTime_std_SG','H_OI_sameTime_std_SG', 'updated_Gtot_sameTime_std_SG','Actot_sameTime_std_SG',
       # 'SIF685_sameTime_std_SG', 'SIF740_sameTime_std_SG']


# In[28]:


chunks = [ds.chunksizes[v] for v in ['time', 'latitude', 'longitude']]
chunks.append((len(OUTPUT_VARIABLES),))

template_LEH = xr.DataArray(
    name = 'LEH',
    data=da.zeros(
        (len(ds.time), len(ds.latitude), len(ds.longitude), len(OUTPUT_VARIABLES)), 
        chunks=chunks,
    ),
    dims=("time", "latitude", "longitude", "output_variable"),
    coords={
        "output_variable": OUTPUT_VARIABLES, 
        "time": ds.time, 
        "latitude": ds.latitude,
        "longitude": ds.longitude
    }
)


# In[29]:


def expand_time_dimension(data, n_time):
    """ 
    Expand the space-dependent data over the time dimension.
    
    Parameters
    ----------
    data : np.ndarray
        (ny, nx) matrix
    n_time : int
        number of elements in the time dimension
    
    Returns
    -------
    np.ndarray
        (1, ntime*ny*nx) matrix
    """
    expanded = np.tile(data.reshape(1, -1), (n_time, 1))
    return expanded.reshape(1, -1)
    

def predictFlux(ds, hc, Vcmo, landcover, IGBP_table, training_testing_append, path_model):
    n_time = len(ds.time)
    
    hc_ = expand_time_dimension(hc.data, n_time)
    Vcmo_ = expand_time_dimension(Vcmo.data, n_time)
    
    IGBP_all = landcover_to_igbp(landcover, IGBP_table, training_testing_append)
    IGBP_ = [
        expand_time_dimension(IGBP_all[f'IGBP_veg_long{i}'].to_numpy(), n_time)
        for i in range(1, 12)
    ]
    
    Rin_ = ds.sel(variable='Rin').data.reshape(1, -1)
    Rli_ = ds.sel(variable='Rli').data.reshape(1, -1)
    p_ = ds.sel(variable='p').data.reshape(1, -1)
    Ta_ = ds.sel(variable='Ta').data.reshape(1, -1)
    ea_ = ds.sel(variable='ea').data.reshape(1, -1)
    u_ = ds.sel(variable='u').data.reshape(1, -1)
    Precip_msr_ = ds.sel(variable='Precip').data.reshape(1, -1)
    LAI_ = ds.sel(variable='LAI').data.reshape(1, -1)
    CO2_ = ds.sel(variable='CO2').data.reshape(1, -1)
    SSM_ = ds.sel(variable='SSM').data.reshape(1, -1)

    features_arr = np.concatenate((
        Rin_, Rli_, p_, Ta_, ea_, u_,  CO2_,LAI_,Vcmo_,hc_,Precip_msr_,   SSM_,  *IGBP_
    ))
    features_arr = features_arr.transpose()
    df_features = pd.DataFrame(
        data=features_arr,
        columns=INPUT_VARIABLES,
    )
    invalid_index = df_features.isnull().any(axis=1)
    
    # Convert the nan value as 0 for the calculation
    df_features[invalid_index] = 0
    
    model = load_model(path_model)
    LEH = model.predict(df_features)
    LEH[invalid_index] = np.nan
    
    return xr.DataArray(
        name='LEH',
        data=LEH.reshape(len(ds.time), len(ds.latitude), len(ds.longitude), len(OUTPUT_VARIABLES)),
        dims=("time", "latitude", "longitude", "output_variable"),
        coords={
            "output_variable": OUTPUT_VARIABLES, 
            "time": ds.time, 
            "latitude": ds.latitude,
            "longitude":ds.longitude
        }
    )


# In[30]:


hc = hc.squeeze('band')
vcmax = vcmax.squeeze('band')
landcover = landcover.squeeze('time')


# In[31]:


LEH = xr.map_blocks(
    predictFlux,
    ds,
    args= [hc, vcmax, landcover],
    kwargs={
        "IGBP_table": IGBP_table, 
        "training_testing_append": training_testing_append, 
        "path_model": MODEL_PATH,
    },
    template=template_LEH,
)


# ## export

# In[32]:


LEH = LEH.chunk({"latitude":200, "longitude":200})


# In[33]:


LEH_ds = LEH.to_dataset(dim="output_variable") 


# In[34]:


LEH_PATH


# In[ ]:


LEH_ds.to_zarr(LEH_PATH, mode='w')


# In[36]:


client.shutdown()

