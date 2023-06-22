#!/bin/bash
#SBATCH --job-name=parallel_job
#SBATCH --nodes=1
#SBATCH --partition=thin
#SBATCH --cpus-per-task=3
#SBATCH --time=40:00:00
#SBATCH --mem=200GB

# Load any necessary modules or activate conda environments
# (if required for your specific job)
module load 2021
module load Miniconda3/4.9.2
source /sw/arch/Centos8/EB_production/2021/software/Miniconda3/4.9.2/bin/activate firstEnv
# Define the number of processes or threads to use
NUM_PROCESSES=3

TMP_DIR="/projects/0/einf2480/global_data_Qianqian/"
# Start the parallel job
echo "Starting parallel job..."

# Create a loop to iterate over each process
for i in `seq 1 $NUM_PROCESSES`; do
   python3 2read10kminput-halfhourly-0608py.py -p $i -jo ${SLURM_JOB_ID} > "$TMP_DIR/test_3Process_output_${SLURM_JOB_ID}_$i.txt" &
  # echo "${SLURM_JOB_ID}"
done

# Wait for all background processes to finish
wait

echo "Parallel job completed!"

# Perform any post-processing or cleanup steps

