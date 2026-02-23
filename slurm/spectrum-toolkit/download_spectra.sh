#!/bin/bash
#SBATCH --job-name=usigrabber-spectra-download
#SBATCH --output=slurm/modification-prediction/logs/spectra-download_%j.out
#SBATCH --time=12:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --partition=cpu-batch
#SBATCH --account=sci-renard-usi-grabber

uv run download-spectra data/psm_data.parquet data/enriched_psm_data
