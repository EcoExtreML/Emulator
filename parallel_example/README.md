# Emulator
This is a test of parallel running by sbatch script.

The comparison is: 1 process (slurm-2868866.out) and 2 processes(slurm-2868865.out). In both jobs I requested 2 threads, but in 1 process job, the second thread was not used (written in singleProcess_output_2.txt). 
The running time for job 2868866 is 00:02:11, for job 2868865 is 00:01:44.

The output of these two jobs are test_singleProcess_output_1.txt, test_singleProcess_output_2.txt; test_doubleProcess_output_1.txt, test_doubleProcess_output_2.txt. For job 2868866, the testing samples size is 1000000, the time for predicting is 84 seconds. For job 2868865 the testing samples size for each process is 500000, and the time for predicting are 50 seconds for both process. This (84/50=1.68) is not exactly 2 times faster. 

Note: The training part is same for each process, no difference in setup (so we just compare the time for prediction in above). But the training time for 1 process is 33 seconds, the training time for 2 processes setup is 38 seconds for each process.
