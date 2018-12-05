#!/bin/bash
# Author: Nisarg Thakkar  (nmt324@nyu.edu)
OUTDIR=${1:-test/output}
shift

INS=$(ls -l test/input/input_* | grep -o input_.*)

mkdir -p ${OUTDIR}
for test in ${INS}; do
	f=${test#*_}
	echo "input_${f}"
	touch ${OUTDIR}/output_${f}
	python3 main.py < test/input/input_${f} > ${OUTDIR}/output_${f}
done

