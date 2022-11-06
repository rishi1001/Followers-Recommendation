#!/bin/bash

/home/cse/btech/cs1190356/scratch/A3/convert /scratch/cse/phd/anz198717/TA/COL380/A3/dummy_data/ /home/cse/btech/cs1190356/scratch/A3/convertedDummyData/

# qsub -P col380.cs1190356 -m bea -M cs1190356@iitd.ac.in -l select=5:ncpus=5 -l walltime=00:30:00 HNSWPred.sh