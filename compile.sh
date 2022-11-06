#!/bin/bash
g++ -o convert convert.cpp
module load compiler/gcc/9.1/openmpi/4.0.2
mpic++ -std=c++17 -fopenmp -o run main.cpp