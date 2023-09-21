#!/bin/bash
#SBATCH --time 3:00:00
#SBATCH --nodes=1
#SBATCH --partition=fat
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=240GB
  
# Make sure the jupyter command is available, either by loading the appropriate modules, sourcing your own virtual environment, etc.
module load 2021
#module load IPython/7.25.0-GCCcore-10.3.0
module load Miniconda3/4.9.2

# Choose random port and print instructions to connect
PORT=`shuf -i 5000-5999 -n 1`
LOGIN_HOST=int5-pub.snellius.surf.nl
BATCH_HOST=$(hostname)
 
echo "To connect to the notebook type the following command from your local terminal:"
echo "ssh -J ${USER}@${LOGIN_HOST} ${USER}@${BATCH_HOST} -L ${PORT}:localhost:${PORT}"
echo
echo "After connection is established in your local browser go to the address:"
echo "http://localhost:${PORT}"
#source /sw/arch/Centos8/EB_production/2021/software/Miniconda3/4.9.2/bin/activate emulator

source /sw/arch/Centos8/EB_production/2021/software/Miniconda3/4.9.2/bin/activate /home/qiahan/.conda/envs/mamba/envs/emulator

#conda activate gdal 
cd /projects/0/ttse0619/qianqian
jupyter lab --no-browser --port $PORT

