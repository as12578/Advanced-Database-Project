#!/bin/bash
set -x
CURRENT_DIR=$(pwd)
REPROZIPPED_FILE="advdb_project.rpz"
REPROUNZIPPED_DIR="reprounzipped_dir"
REPROUNZIPPED_CHROOT="reprounzipped_chroot"

USE_REPROUNZIP_DIR=1
USE_REPROUNZIP_CHROOT=0

rm ${REPROZIPPED_FILE}
reprozip trace --overwrite test/runit.sh
reprozip pack ${REPROZIPPED_FILE}

if [ $USE_REPROUNZIP_DIR -eq 1 ]; then
	rm -rf ${REPROUNZIPPED_DIR}

	reprounzip directory setup ${REPROZIPPED_FILE} ${REPROUNZIPPED_DIR}
	reprounzip directory run ${REPROUNZIPPED_DIR}
fi

if [ $USE_REPROUNZIP_CHROOT -eq 1 ]; then
	sudo reprounzip chroot destroy ${REPROUNZIPPED_CHROOT}

	sudo reprounzip chroot setup ${REPROZIPPED_FILE} ${REPROUNZIPPED_CHROOT}
	sudo reprounzip chroot run ${REPROUNZIPPED_CHROOT}
fi

DIFFCMD="diff -b -B -E"
DIRECTORY_LOG="DIRECTORY_LOG"
CHROOT_LOG="CHROOT_LOG"

rm -rf ${DIRECTORY_LOG}

INS=$(ls -l test/input/input_* | grep -o input_.*)

for test in ${INS}; do
	f=${test#*_}
	echo "output_${f}"
	OUTPUT_FILE="test/output/output_${f}"

	if [ $USE_REPROUNZIP_DIR -eq 1 ]; then
		echo "################### ${OUTPUT_FILE}##################" >> ${DIRECTORY_LOG}
		${DIFFCMD} ${DARGS} ${REPROUNZIPPED_DIR}/root/${CURRENT_DIR}/${OUTPUT_FILE} ${OUTPUT_FILE} >> ${DIRECTORY_LOG}
	fi

	if [ $USE_REPROUNZIP_CHROOT -eq 1 ]; then
		echo "################### ${OUTPUT_FILE}##################" >> ${CHROOT_LOG}
		${DIFFCMD} ${DARGS} ${REPROUNZIPPED_CHROOT}/root/${CURRENT_DIR}/${OUTPUT_FILE} ${OUTPUT_FILE} >> ${CHROOT_LOG}
	fi
done

set +x