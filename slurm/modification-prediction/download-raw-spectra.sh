#!/bin/bash -eux
#SBATCH --job-name=download-raw-spectra-with-phospho
#SBATCH --account=sci-renard-usi-grabber
#SBATCH --output=slurm/modification-prediction/logs/download-raw-spectra-with-phospho-%j.out
#SBATCH --partition=cpu-batch
#SBATCH --cpus-per-task=8 # -c
#SBATCH --mem=32gb
#SBATCH --time=24:00:00

uv run download-raw-spectra \
    /sc/projects/sci-renard/usi-grabber/shared/csvs/final/retry_with_phospho.csv \
    /sc/projects/sci-renard/usi-grabber/shared/mgf_files/final/with_phospho/ \
    -y \
    -w 7 \
    -d 8 \
    --convert-to-mgf
