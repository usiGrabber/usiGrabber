#!/bin/bash -eux
#SBATCH --job-name=usigrabber_build_big
#SBATCH --account=sci-renard-usi-grabber
#SBATCH --partition=cpu-batch
#SBATCH --cpus-per-task=16 # -c
#SBATCH --mem=128gb
#SBATCH --time=48:00:00

uv run usigrabber build --reset --max-workers 8 --ontology-workers 2