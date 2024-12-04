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
print("aa")
cluster = SLURMCluster(
    name='dask-worker',
    cores=16,
    processes=16,
    queue='fat',
    memory='120GiB',
    local_directory='$TMPDIR',
    walltime='00:01:00'
)
cluster.scale(jobs=1)
client = Client(cluster)
print(client.dashboard_link)

for i in range(100):
  print(i)
