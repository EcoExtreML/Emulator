#!/bin/bash
#SBATCH --job-name=parallel_job
#SBATCH --nodes=1
#SBATCH --partition=thin
#SBATCH --time=6:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=28GB
# Load any necessary modules or activate conda environments
# (if required for your specific job)
module load 2021
module load Miniconda3/4.9.2
source /sw/arch/Centos8/EB_production/2021/software/Miniconda3/4.9.2/bin/activate /home/qiahan/.conda/envs/mamba/envs/emulator

# Define the number of processes or threads to use

# 循环遍历每个月份（1到12月）
for year in {2016..2019}
do
  # 运行Python脚本，传递年份和月份作为参数
  python preprocessing2024-12-03ERA5Land.py --year $year &

  # 可以选择添加适当的延迟，避免资源过度使用
  # sleep 1
done
wait
