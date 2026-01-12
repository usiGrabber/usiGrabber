#!/bin/bash -eux
#SBATCH --job-name=usigrabber_build_big
#SBATCH --account=sci-renard-usi-grabber
#SBATCH --partition=cpu-batch
#SBATCH --cpus-per-task=16 # -c
#SBATCH --mem=256gb
#SBATCH --time=160:00:00

uv run usigrabber build --max-workers 6 --ontology-workers 2
