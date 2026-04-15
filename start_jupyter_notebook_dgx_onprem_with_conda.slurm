#!/bin/bash

#SBATCH --job-name=jupyter
#SBATCH --output=slurmout/jupyter-%j.out
#SBATCH --error=slurmout/jupyter-%j.err
#SBATCH --time=05-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-task=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=80G

# Ensure log directory exists
mkdir -p slurmout

export PYTHONNOUSERSITE=1   # make Python use the system site-packages, not from local user

# Load conda init script
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

# Random port
port=$((RANDOM % 1000 + 8000))
token="$(openssl rand -hex 16)"

# Detect node hostname/IP
node_host=$(hostname -f)

# Print URL to .out
echo "=========================================================="
echo "Your Jupyter Lab session is available at:"
echo "   http://${node_host}:${port}/lab?token=${token}"
echo "=========================================================="

# Start Jupyter Lab
jupyter lab --no-browser --ip=0.0.0.0 --port=$port \
  --NotebookApp.token="${token}"