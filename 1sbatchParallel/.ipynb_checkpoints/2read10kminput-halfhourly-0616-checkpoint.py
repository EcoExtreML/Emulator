#!/usr/bin/env python
# coding: utf-8

# In[18]:


"""
Thesis_PhD_Qianqian Predicted_fluxes_glboal_stripes
date: 24-Feb-2023
author: Qianqian
Contact: q.han@utwente.nl
-------------------------------------
Description: 
"""
# libraries
import os
import joblib
from osgeo import gdal
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from rasterio.warp import reproject, Resampling
import xarray as xr
import pickle
from PyStemmusScope import variable_conversion as vc
from rasterio.warp import reproject, Resampling
from sklearn.preprocessing import OneHotEncoder
import time
import os
import re
import argparse
import glob

import glob

if 'glob' in dir():
    print("glob module is loaded", flush=True)
else:
    print("glob module is not loaded", flush=True)


import sklearn
print(sklearn.__version__, flush=True)
# In[31]:
from platform import python_version
print(python_version(), flush=True)

import dask
print("dask",dask.__version__, flush=True)
import sys
recursion_limit = sys.getrecursionlimit()
print(f"The current recursion limit is: {recursion_limit}")

# load trained RF model
with open('/gpfs/work1/0/einf2480/global_data_Qianqian/RF_train/RFLEH-LAI-hc-CO2-SSM-Vcmo-IGBP_multi7_1core_snellius.pkl', 'rb') as f:
    rfLEHmulti = pickle.load(f)

rfLEHmulti.jobs=1
# ## define function to read file

# In[33]:


def get_directories_with_number_and_ending(directory_path, ending):
    directories = []
    if ending:
        pattern = re.compile(r'^\d+.*{}$'.format(re.escape(ending)))  # Match directory names with number at the beginning and specific ending
    else:
        pattern = re.compile(r'^\d+$')  # Match directory names with only numbers

    for entry in os.scandir(directory_path):
        if entry.is_dir():
            directory_name = entry.name
            if pattern.match(directory_name):
                directories.append(directory_name)

    return directories

def export_to_nc(output_data,output_path):
    print("aa", flush=True)
    # method suggested by Bas, flexible
    from netCDF4 import Dataset
    print("bb", flush=True)

    lat = output_data.latitude.values # the list of latitudes
    print('lat',lat, flush=True)
    lon = output_data.longitude.values #
    print('lon', lon, flush=True)
    data = output_data.values # the soil moisture data
    print("data", data, flush=True)

    
    # Create a new empty netCDF file, in NETCDF3_CLASSIC format (more formats are available)
    nc = Dataset(output_path, mode='w', format='NETCDF3_CLASSIC')
    print("nc",nc, flush=True)

    # Create the dimensions, as required by netCDF
    nc.createDimension('x', size=len(lon)) # instead of x,y you can use lon,lat as dimension names
    nc.createDimension('y', size=len(lat))
    nc.createDimension('time', None)

    # Create the variables, as required by netCDF
    var_x = nc.createVariable('x', 'float64', ('x')) # instead of x,y you can use lon,lat as variable names
    var_y = nc.createVariable('y', 'float64', ('y'))
    var_t = nc.createVariable('time', 'float64', ('time',))

    # Fill the x, y, time variables with values
    var_x[:] = lon
    var_y[:] = lat
    # var_t[:] = variable.time.values
    # var_t.setncattr('units', 'seconds since 1970-01-01 00:00:00') # this is only a description; you can change it
    # var_t.setncattr('calendar', 'standard')

    # the actual data to be stored in the netcdf (soilmoisture?)
    var = nc.createVariable('LE', 'float32', ('time','y','x'))
    var[:] = data # if 'data' does not have the same structure as the one created here (time, y, x) you may need to transpose
    print("var",var, flush=True)
    # write the time as last
    var_t_length = data.shape[0] # the number of timesteps
    seconds_since_epoch = (output_data.time.values.astype('datetime64[s]') - np.datetime64('1970-01-01T00:00:00Z')).astype(int)
    var_t[:] = seconds_since_epoch
    print("vart",var_t, flush=True)
    # var_t[:] = [i*3600 for i in range(var_t_length)] # this is a simple computation; you may have a list of times, or you may want to compute the 'unix' time
    nc.close()
    
# In[53]:


def Read_LSFs_Estimated_fluxes(process, job):
    # get all the filefolders named as year
    year_list = get_directories_with_number_and_ending("/gpfs/work1/0/einf2480/global_data_Qianqian/", "NL")
    # the input data from year[0] to year[..], based on the process id in sbatch script
    year = year_list[process-1]
    print(year, flush=True)
    # reference file for spatial resample
    # t = '2011-03-01T00:00:00.000000000'
    era5 = xr.open_dataset('/gpfs/work1/0/einf2480/global_data_Qianqian/2017NL/era5land/era5-land_10m_u_component_of_wind_2017-01_FI-Hyy.nc')
    era5 = era5.sel(time = era5.time.values[0])['u10']
    starttime0 = time.time()
    
    ### 0) read era5land data
    all1 = xr.open_mfdataset("/gpfs/work1/0/einf2480/global_data_Qianqian/"+year+"/era5land/*01*.nc")
    all_resample = all1.resample(time="1800S").interpolate('linear')
    
    # 累减得到每小时的辐射
    Rin = all1.to_array()[2,:,:,:].copy().astype(float)
    Rin[::] = np.nan
    Rli = all1.to_array()[3,:,:,:].copy().astype(float)
    Rli[::] = np.nan
    for count_i,t in zip(range(len(all1.time)+1), all1.time.to_numpy()):
        ds_era5land0 = all1.sel(time=np.datetime64(t)- np.timedelta64(1,'h'), method='nearest').compute()
        ds_era5land = all1.sel(time=t, method='nearest').compute()
        ds0 = xr.merge([ds_era5land0])
        ds = xr.merge([ds_era5land])

        ds_ss = xr.Dataset()
        if (pd.to_datetime(t).hour==1):
            # print(t)
            ds_ss["Rin"] = (ds["ssrd"]) / 3600  # J * hr / m2 ->  W / m2
            ds_ss["Rli"] = (ds["strd"]) / 3600  # J * hr / m2 ->  W / m2
        else:
            ds_ss["Rin"] = (ds["ssrd"]-ds0['ssrd']) / 3600  # J * hr / m2 ->  W / m2
            ds_ss["Rli"] = (ds["strd"]-ds0['strd']) / 3600  # J * hr / m2 ->  W / m2
        Rin[count_i,:,:] = ds_ss["Rin"]
        Rli[count_i,:,:] = ds_ss["Rli"]
    #对辐射进行线性插值到半小时
    Rin = Rin.resample(time="1800S").interpolate('linear')
    Rli = Rli.resample(time="1800S").interpolate('linear')
    print(Rin)
    
    ### 1) read LAI data
    probav_files = sorted(Path("/gpfs/work1/0/einf2480/global_data_Qianqian/"+year+"/lai/").glob("*"+year[0:-2]+"*.nc"))
    print(probav_files, flush=True)
    lai_10km = []
    for i in probav_files:
        lai0 = xr.open_dataset(i)["LAI"].sel(lat=slice(55, 50), lon=slice(2, 7))
        # print(lai.values)

        lai0 = lai0.rio.write_crs('EPSG:4326')
        era5 = era5.rio.write_crs('EPSG:4326')
        lai0.rio.write_nodata(lai0.rio.nodata, inplace=True)
        #好像不能同时重采样多个波段，不然nan值会互相影响。从python导出的LAI，然后在ArcGIS打开看着一样，但是在python里面，non-nan值的个数却不一样。
        lai0_10km = lai0.rio.reproject_match(era5, resampling=Resampling.average)
        lai0_10km = lai0_10km.assign_coords({
            "x": lai0_10km.x,
            "y": lai0_10km.y,
        })
        lai_10km.append(lai0_10km)
    lai = xr.concat(lai_10km, dim='time').resample(time="1800S").interpolate('linear').rename({'x':'longitude','y':'latitude'})
    
    
    ### 2) read canopy height data
    hc_path = '/gpfs/work1/0/einf2480/global_data_Qianqian/'+year+'/canopy_height/mosaic_output10km.tif'
    hc = xr.open_dataset(hc_path, engine="rasterio").rename({'x':'longitude','y':'latitude'}).band_data
    
    
    ### 3) read CO2 data
    ds_co2 = xr.open_mfdataset("/gpfs/work1/0/einf2480/global_data_Qianqian/"+year+"/co2/CAMS_CO2_20*.nc")['co2'].sel(time=year[0:-2])#.sel(latitude=slice(65, 60), longitude=slice(20, 25))
    #ds_co2 = ds_co2.sel(time=ds_co2['time.month'] == 1)
    # convert unit: kg/kg to mg/m3
    ds_co2 = vc.co2_mass_fraction_to_kg_per_m3(ds_co2)*1e6
    # convert the longitude from [0, 360] to [-180, 180]
    lon_name = 'longitude'
    ds_co2['longitude_adjusted'] = xr.where(
        ds_co2[lon_name] > 180,
        ds_co2[lon_name] - 360,
        ds_co2[lon_name])
    ds_co2 = (
        ds_co2
        .swap_dims({lon_name: 'longitude_adjusted'})
        .sel(**{'longitude_adjusted': sorted(ds_co2.longitude_adjusted)})
        .drop(lon_name))
    ds_co2 = ds_co2.rename({'longitude_adjusted': lon_name})
    ds_co2 = ds_co2.rio.write_crs('EPSG:4326')
    era5 = era5.rio.write_crs('EPSG:4326')
    ds_co2.rio.write_nodata(ds_co2.rio.nodata, inplace=True)
    ds_co2_10km = ds_co2.rio.reproject_match(era5, resampling=Resampling.average)
    ds_co2_10km = ds_co2_10km.assign_coords({
        "x": ds_co2_10km.x,
        "y": ds_co2_10km.y,
    })
    ds_co2_10km = ds_co2_10km.resample(time="1800S").interpolate('linear').rename({'x':'longitude','y':'latitude'})
    #CO2有1483个波段，ERA5-Land有1487个波段。一个到21点，一个到23点。
    #值得注意的是ERA5-Land的时间是每个step的结束时间，而CO2是每个step的开始时间？
    #2011-03-01T00:30:00.000000000 在ERA5-Land代表的是00:00-00:30，而在CO2代表00:30-01:00？
    ds_co2_10km=ds_co2_10km.assign_coords({'time':('time',ds_co2_10km.time.values + np.timedelta64(30,'m'),ds_co2_10km.time.attrs)})
    #https://stackoverflow.com/questions/64737439/xarray-dataset-change-value-of-coordinates-while-keeping-attributes
    
    
    ### 4) read SSM data
    path_SSM = glob.glob("/gpfs/work1/0/einf2480/global_data_Qianqian/"+year+"/ssm/SM201*.tif")[0]
    ds_SSM = xr.open_rasterio(path_SSM, engine="rasterio")
    ds_SSM = ds_SSM.sortby(["x", "y"])
    #ds_SSM = ds_SSM.sel(x=slice(20,25), y=slice(60, 65))
    ds_SSM['band'] = pd.to_datetime(ds_SSM.band-1, unit='D', origin=str(path_SSM.split('/')[-1][2:6]))
    ds_SSM = ds_SSM.rename({'band':'time'})
    SSM_10km = []
    for i in ds_SSM.time:
        SSM0 = ds_SSM.sel(time=i)
        SSM0 = SSM0.rio.write_crs('EPSG:4326')
        era5 = era5.rio.write_crs('EPSG:4326')
        SSM0.rio.write_nodata(SSM0.rio.nodata, inplace=True)
        #好像不能同时重采样多个波段，不然nan值会互相影响。从python导出的LAI，然后在ArcGIS打开看着一样，但是在python里面，non-nan值的个数却不一样。
        SSM0_10km = SSM0.rio.reproject_match(era5, resampling=Resampling.average, nodata=np.nan) #
        SSM0_10km = SSM0_10km.assign_coords({
            "x": SSM0_10km.x,
            "y": SSM0_10km.y,
        })
        SSM_10km.append(SSM0_10km)
    SSM = xr.concat(SSM_10km, dim='time').resample(time="1800S").interpolate('linear').rename({'x':'longitude','y':'latitude'})/1000 
    
    
    ### 5) read Vcmax data
    ds_Vcmo = xr.open_rasterio("/gpfs/work1/0/einf2480/global_data_Qianqian/"+year+"/vcmax/TROPOMI_Vmax_Tg_mean10km.tif", engine="rasterio")
    
    
    ### 6) read IGBP data
    path_landcover = "/gpfs/work1/0/einf2480/global_data_Qianqian/"+year+"/igbp/landcover10km.tif"
    landcover = xr.open_rasterio(path_landcover, engine="rasterio")
    FILEPATH_LANDCOVER_TABLE = "/gpfs/work1/0/einf2480/global_data_Qianqian/"+year+"/igbp/lccs_to_igbp_table.csv"
    IGBP_table = pd.read_csv(FILEPATH_LANDCOVER_TABLE)
    IGBP = np.vectorize(IGBP_table.set_index("lccs_class").T.to_dict('records')[0].get)(landcover.values)
    training_testing_append = pd.read_csv('/gpfs/work1/0/einf2480/global_data_Qianqian/RF_train/training_testing-withindex_v3.csv')
    IGBP_all = pd.DataFrame(columns=['IGBP_veg_long1', 'IGBP_veg_long2', 'IGBP_veg_long3','IGBP_veg_long4','IGBP_veg_long5',
                              'IGBP_veg_long6','IGBP_veg_long7','IGBP_veg_long8','IGBP_veg_long9',
                             'IGBP_veg_long10','IGBP_veg_long11'])
    # define one hot encoding for IGBP
    encoder = OneHotEncoder(categories=[training_testing_append['IGBP_veg_long'].unique()]*1,sparse=False,
                           handle_unknown = "ignore")
    print("IGBP",[training_testing_append['IGBP_veg_long'].unique()]*1, flush=True)
    # transform data
    aa = encoder.fit_transform(IGBP.reshape(IGBP.shape[1]*IGBP.shape[1],1))
    # assign 23-D IGBP into 23 columns
    for i in range(1,12,1):
        IGBP_all['IGBP_veg_long'+str(i)] = aa[:,i-1]
    
    
    ### end) combine predictor variables and predict
    result_LE = all_resample.to_array()[0,:,:,:].copy().astype(float)
    result_LE[::] = np.nan
    result_H = all_resample.to_array()[0,:,:,:].copy().astype(float)
    result_H[::] = np.nan
    result_Rn = all_resample.to_array()[0,:,:,:].copy().astype(float)
    result_Rn[::] = np.nan
    result_RSSM = all_resample.to_array()[0,:,:,:].copy().astype(float)
    result_RSSM[::] = np.nan
    result_SIF685 = all_resample.to_array()[0,:,:,:].copy().astype(float)
    result_SIF685[::] = np.nan
    result_SIF740 = all_resample.to_array()[0,:,:,:].copy().astype(float)
    result_SIF740[::] = np.nan
    result_Actot = all_resample.to_array()[0,:,:,:].copy().astype(float)
    result_Actot[::] = np.nan
    #static predictors
    hc_line = hc[0,:,:].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size) #如果和动态变量一样放进ds_ss里显示为一些离散的点，不知道为什么，所以静态变量放在时间循环外面
    Vcmo_line = ds_Vcmo[0,:,:].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP1 = IGBP_all.iloc[:,0].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP2 = IGBP_all.iloc[:,1].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP3 = IGBP_all.iloc[:,2].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP4 = IGBP_all.iloc[:,3].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP5 = IGBP_all.iloc[:,4].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP6 = IGBP_all.iloc[:,5].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP7 = IGBP_all.iloc[:,6].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP8 = IGBP_all.iloc[:,7].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP9 = IGBP_all.iloc[:,8].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP10 = IGBP_all.iloc[:,9].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    IGBP11 = IGBP_all.iloc[:,10].to_numpy().reshape(1, hc.latitude.size*hc.longitude.size)
    print('read data{} seconds'.format(time.time() - starttime0), flush=True)   
    print("all_resample",all_resample, flush=True)
    print("Rin",Rin, flush=True)
    print("lai",lai, flush=True)
    print("ds_co2_10km", ds_co2_10km, flush=True)
    print("SSM", SSM, flush=True)
    starttime = time.time()

    for count_i,t in enumerate(all_resample.time.to_numpy()[96:9000], 96):#zip(range(100), all_resample.time.to_numpy()[500:502]): #
        print(t, flush=True)
        ds_era5land = all_resample.sel(time=t, method='nearest').compute()
        ds = xr.merge([ds_era5land])
        ds_ss = xr.Dataset()
        ds_ss["Rin"] = Rin.sel(time=t, method='nearest').compute()
        ds_ss["Rli"] = Rli.sel(time=t, method='nearest').compute()
        ds_ss["p"] = ds["sp"]/100   # Pa -> hPa
        ds_ss["Ta"] = ds["t2m"] - 273.15  # K -> degC
        ds_ss["ea"] = vc.calculate_es(ds["d2m"] - 273.15)
        ds_ss["u"] = (ds["u10"] ** 2 + ds["v10"] ** 2) ** 0.5
        ds_ss["Precip_msr"] = ds["tp"]*1000   # mm
        ds_ss['LAI'] = lai.sel(time=t, method='nearest').compute()
        ds_ss['CO2'] = ds_co2_10km.sel(time=t, method='nearest').compute()
        ds_ss['SSM'] = SSM.sel(time=t, method='nearest').compute()

        ds_ss = ds_ss.to_array()
        Rin_line = ds_ss[0,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        Rli_line = ds_ss[1,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        p_line = ds_ss[2,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        Ta_line = ds_ss[3,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        ea_line = ds_ss[4,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        u_line = ds_ss[5,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        Precip_msr_line = ds_ss[6,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        LAI_line = ds_ss[7,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        CO2_line = ds_ss[8,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)
        SSM_line = ds_ss[9,:,:].to_numpy().reshape(1, ds_ss.latitude.size*ds_ss.longitude.size)

        features_arr = np.concatenate((Rin_line, Rli_line, p_line, Ta_line,ea_line, u_line, Precip_msr_line,
                                       LAI_line, hc_line, CO2_line, SSM_line, Vcmo_line,
                                       IGBP1, IGBP2, IGBP3,IGBP4,IGBP5,IGBP6,IGBP7,IGBP8,IGBP9,IGBP10,IGBP11))
        features_arr = features_arr.transpose()
        print("features_arr",features_arr, flush=True)
        # Nan value.
        df_features = pd.DataFrame(data=features_arr)
        df_features_drop_nan = df_features.dropna()
        invalid_index = sorted(set(df_features.index.to_list()) - set(df_features_drop_nan.index.to_list()))

        # # Convert the nan value as 0 for the calculation
        where_are_NaNs = np.isnan(features_arr)
        features_arr[where_are_NaNs] = 0

        estimated_LEH = rfLEHmulti.predict(features_arr)
        estimated_LEH[invalid_index] = np.nan
        print(estimated_LEH.shape)
        LEH_map = estimated_LEH.reshape(ds_ss.latitude.size, ds_ss.longitude.size,7)
        print(LEH_map.shape)
        result_LE[count_i, ::] = LEH_map[:,:,0]
        print("LE",result_LE.shape)
        result_H[count_i, ::] = LEH_map[:,:,1]
        print("H",result_H.shape)
        result_Rn[count_i, ::] = LEH_map[:,:,2]
        print("Rn",result_Rn.shape)
        result_RSSM[count_i, ::] = LEH_map[:,:,3]
        print("RSSM",result_RSSM.shape)
        result_SIF685[count_i, ::] = LEH_map[:,:,4]
        print("SIF685",result_SIF685.shape)
        result_SIF740[count_i, ::] = LEH_map[:,:,5]
        print("SIF740",result_SIF740.shape)
        result_Actot[count_i, ::] = LEH_map[:,:,6]
        print("Actot",result_Actot.shape)
         
#    # export the result
#    result_LE.rio.write_crs('EPSG:4326').rio.to_raster(
#        '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-LEvalues.tif',
#        tiled=True,  # GDAL: By default striped TIFF files are created. This option can be used to force creation of tiled TIFF files.
#        windowed=True,  # rioxarray: read & write one window at a time
#    )
#    result_H.rio.write_crs('EPSG:4326').rio.to_raster(
#        '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-Hvalues.tif',
#        tiled=True,  # GDAL: By default striped TIFF files are created. This option can be used to force creation of tiled TIFF files.
#        windowed=True,  # rioxarray: read & write one window at a time
#    )
#    print("result_LE", result_LE, flush=True)
#    print("result_LE_values", result_LE.values, flush=True)
#    file_path1 = "/gpfs/work1/0/einf2480/global_data_Qianqian/output/file50lon.txt"
#    file_path2 = "/gpfs/work1/0/einf2480/global_data_Qianqian/output/file50lat.txt"
#    file_path3 = "/gpfs/work1/0/einf2480/global_data_Qianqian/output/file50values.npy"
#    np.savetxt(file_path1, result_LE.longitude.values)
#    np.savetxt(file_path2, result_LE.latitude.values)
#    np.save(file_path3, result_LE.values)

    export_to_nc(result_LE, '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-LE_0616sne6m.nc')
    print("exportLE")
    export_to_nc(result_H, '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-H_0616sne6m.nc')
    print("exportH")
    export_to_nc(result_Rn, '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-Rn_0616sne6m.nc')
    print("exportRn")
    export_to_nc(result_RSSM, '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-RSSM_0616sne6m.nc')
    print("exportRSSM")
    export_to_nc(result_SIF685, '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-SIF685_0616sne6m.nc')
    print("exportSIF685")
    export_to_nc(result_SIF740, '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-SIF740_0616sne6m.nc')
    print("exportSIF740")
    export_to_nc(result_Actot, '/gpfs/work1/0/einf2480/global_data_Qianqian/output/'+year+'-Actot_0616sne6m.nc')
    print("exportActot")

    print('That took {} seconds'.format(time.time() - starttime), flush=True)     
    return process, job, result_LE, result_H



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process ID argument')
    parser.add_argument('-p',"--process_id", type=int, help='ID of the process')
    parser.add_argument('-jo',"--job_id",type=int, help="ID of the iteration")
    args = parser.parse_args()

    process_id = args.process_id
    job_id = args.job_id
    num_processes = 3
    

    if process_id < 1 or process_id > num_processes:
        print(f"Invalid process ID. Process ID must be between 1 and {num_processes}.")
        exit(1)
    
    process, job, result_LE, result_H = Read_LSFs_Estimated_fluxes(process_id, job_id)
    print(f"Process {process}, Job {job}", flush=True)
    print(result_LE)
