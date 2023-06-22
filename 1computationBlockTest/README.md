run_parallel.sh is the sbatch script for parallel running the python script.
2read10kminput_halfhourly-0616.py is the python script for predicting 11 km fluxes.

1) Job 2926905 was run for 1 spatial unit, 7 variables, 100 timesteps.
2) Job 2927300 was run for 1 spatial unit, 7 variables, 1 month -Jan.
3) Job 2933096 was run for 1 spatial unit, 7 variables, 1 year. 
But I got recursion error. I thought it is too many time loop. So I tried to run November and December, but both failed with this recursion error. 

Then I realized the difference between Jan and Nov, Dec is the input variables Rin and Rli (in line 151 of 2read10kminput-halfhourly-0616.py). I have to use the Rin and Rli in T01:00 to subtract T00:00 to get hourly values. I only did it for Jan, so Nov and Dec are all nan. This caused that the predicted result of Jan has values, but all nan values for Nov and Dec. 

I also tried Feb, same recursion error, but when I change 745 to 1500 in line 151 to make Rin and Rli has values for Feb, Feb succeeded. This means the input variables can not be all nan values for too many steps?

I tried to look at this error in jupyter notebook, the real reason why all nan values cause the recursion error is in cell 21 of 2read10kminput-halfhourly-0608py.ipynb. It crash in the 492nd step inside the for loop (the last 500 steps of Dec is the for loop), when I tried to print(result_LE[count_i::].values. I can not understand this but it seems the error is from this. And the predicted result (estimated_LEH) seems no problem, because I can print it (cell 71 of 2read10kminput-halfhourly-0608py-Copy1.ipynb). Then why result_LE[count_i, ::] = LEH_map[:,:,0]; print("LE values",result_LE[count_i, ::].values) throw the error? What is the reason? For this 500 steps of Dec, if I increase recursionlimit to 3000, the error disappeared(similar but not same problem: https://stackoverflow.com/questions/75815668/memoryerror-unable-to-allocate-33-1-gib-for-an-array-with-shape-192-384-6026).

I found the reason: when I just create a dataarray result_LE ([17519,51,51]), and assign np.nan (instead of using RF to predict) to it for 500 times in a for loop (loop the first dimension), it throw recursion error at 432nd step. So the dataarray can not be assigned np.nan for too many times? However when I tried to assign 0 or 1 to result_LE, it throw the recursion error at 492nd step. When I tried to assgin different values to result_LE, also recursion error at 492nd step. When I tried to read the exported .nc file to assign its values to result_LE, also recursion error at 492nd step. Then it gets confusing again, because I managed to predict Jan and assign the predicted result to result_LE which is 1500 steps, why it did not have recursion error? Or the problem is the dataarray result_LE because I created it from copying ERA5Land data? But then still why in Jan it worked?

all1 = xr.open_mfdataset("/data/private/DL/datadownload/"+year+"/era5land/*.nc")

all_resample = all1.resample(time="1800S").interpolate('linear')

result_LE = all_resample.to_array()[0,:,:,:].copy().astype(float)

result_LE[::] = np.nan

for count_i,t in enumerate(all_resample.time.to_numpy()[0:1500]):

    result_LE[count_i, ::] = np.nan
	
    print(count_i)
	
    print(result_LE[count_i,::].values)  
	
When I tried to create the dataarray in the below way, and then assign values for 1500 steps, no recursion error, also finished in 1 second:
shape = (17519, 51, 51)
data_array = xr.DataArray(np.full(shape, np.nan))

 
This error was fixed by making the Rin and Rli not nan. We can have a look at this question if we have time left. Other things are more important probably, e.g. how to make the python script run faster and make the parallel computing plan for global scale.

4) Job 2957047 is run for 1 spatial unit, 7 variables, 6 months (Jan-June).

I change 745 in line 151 to range(len(all1.time)+1) which is 17520 to calculate hourly Rin and Rli for the whole year. But after I do this, seems lines 316-342 is running very slow. With 745, it was 4 seconds for each loop, with 17520, it is 20 seconds for each loop. That is why I am running for 6 months instead of 1 year. And the most time-consuming code is between line 316-342, the predict line 358 is fast actually. Could you help me check can we make line 316-342 fast?


