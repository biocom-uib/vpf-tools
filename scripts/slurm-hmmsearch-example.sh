#!/bin/bash

#SBATCH --qos=debug
#SBATCH --time=0:30:00
#SBATCH --constraint=haswell
#SBATCH --ntasks=512
#SBATCH --cpus-per-task=64

module load openmpi
module load hmmer
#module load prodigal # prodigal is not a module anymore, so it should be in $PATH or specified with --prodigal

#  --virus-pattern '([^_]+_____[^_]+)'

srun --cpu-bind=cores "$HOME/.local/bin/vpf-class" --mpi      \
  --prodigal      ../bin/prodigal-2.6.3                       \
  --input-seqs    ../vpf-data/test.fna                        \
  --vpf           ../vpf-data/final_list.hmms                 \
  --vpf-class     ../vpf-data/vpf-classes.tsv                 \
  --workdir       vpf-class-generated-files/                  \
  --output        generated-virus-classification.tsv          \
  --workers       "$((SLURM_CPUS_PER_TASK-1))"                \
  --chunk-size    "${CHUNK_SIZE:-5}"                          \
  +RTS -qa -N "-maxN$SLURM_CPUS_PER_TASK"

#"$((SLURM_CPUS_PER_TASK-1))"                \
  # +RTS "-N$SLURM_CPUS_PER_TASK" # Do not allow sparks to take up more threads than requested
