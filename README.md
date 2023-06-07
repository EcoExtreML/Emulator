# Emulator
This is a test of parallel running by sbatch script.

The comparison is: 1 process (slurm-2867410.out) and 2 processes(slurm-2867413.out). In both jobs I requested 2 threads, but in 1 process job, the second thread was not used (written in singleProcess_output_2.txt). 
The running time for job 2867410 is 00:02:16, for job 2867413 is 00:01:29. It is not double speed but 1.5 times faster, is this normal?

The output of these two jobs are singleProcess_output_1.txt, singleProcess_output_2.txt; doubleProcess_output_1.txt, doubleProcess_output_2.txt.
For job 2867410, the testing samples size is 1000000, the time for training and predicting is 120.4 seconds. For job 2867413 the testing samples size for each process is 500000, and the time for training and predicting are 74.5 seconds for both process.
