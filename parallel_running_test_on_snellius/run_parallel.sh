#!/bin/bash
#SBATCH --job-name=parallel_job
#SBATCH --nodes=1
#SBATCH --partition=staging
#SBATCH --cpus-per-task=2
#SBATCH --time=1:00:00

# Load any necessary modules or activate conda environments
# (if required for your specific job)
module load 2021
module load Miniconda3/4.9.2
source /sw/arch/Centos8/EB_production/2021/software/Miniconda3/4.9.2/bin/activate firstEnv
# Define the number of processes or threads to use
NUM_PROCESSES=2

TMP_DIR="/home/qiahan/parallel"
# Start the parallel job
echo "Starting parallel job..."

# Create a loop to iterate over each process
for i in `seq 1 $NUM_PROCESSES`; do
   python3 parallel_script_process_jobid.py -p $i -jo ${SLURM_JOB_ID} > "$TMP_DIR/doubleProcess_output_$i.txt" &
  # echo "${SLURM_JOB_ID}"
done

# Wait for all background processes to finish
wait

echo "Parallel job completed!"

# Perform any post-processing or cleanup steps

