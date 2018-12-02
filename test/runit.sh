#!/bin/bash
# Author: Nisarg Thakkar  (nmt324@nyu.edu)
OUTDIR=${1:-test/output/}
shift

# INS="1 2 3 3.5 3.7 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 test test_1 test_2 test_3 test_4"

INS=$(ls -l test/input/input_* | grep -o input_.*)

for test in ${INS}; do
	f=${test#*_}
	echo "input_${f}"
	python3 main.py < test/input/input_${f} > ${OUTDIR}/output_${f}
done

