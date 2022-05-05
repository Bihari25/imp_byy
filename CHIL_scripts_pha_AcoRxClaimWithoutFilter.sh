#!/bin/sh
###############################################################################
#
#    SCRIPT NAME :  AcoRxClaimWithoutFilter.sh
#
#    Description :
#                 The Script read the Rx Claims File, Doesn't perform any validation
#                 against provided ACO File and generate output file in outbound directory.
#                 The script process file "WITHOUT" Filter
#
#    USAGE:
#    sh AcoRxClaimWithoutFilter.sh <SCRIPT_PATH> <INPUTFILE_LOCATION> <OUTPUTFILE_LOCATION> <QUEUE> >> <LOG FILE NAME> 2>&1
#
#    e.g nohup sh AcoRxClaimWithoutFilter.sh /mapr/datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_scripts/pha /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_landing/stg43/stg43_incremental/pha/test  /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_outbound/stg43/stg43_incremental/pha/test ihrgpbat_q1 >> /mapr/datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_logs/stg43/stg43_incremental/pha/test/test_nonfilter.log 2>&1 &
#
#    Version    Date           Developer           Comment
#    -------------------------------------------------------------------------
#
#    1.0     03-SEPT-2018    Nilabh/Vivek       Initial Version
#
###############################################################################
SCRIPT=`basename $0`

script_path=$1
inputfilepath=$2
OutputPath=$3
queue=$4

ls -1 /mapr/${inputfilepath}/*RXCHF70CL.TXT | while read rx_fl
do

Filename=`echo $rx_fl | rev | cut -d"/" -f1  | rev`
inputfile=${inputfilepath}/${Filename}

echo "Processing $inputfile"

spark-submit --class com.cadillac.spark.sparkjob.RXClaims  --master yarn --queue ihrgpbat_q1 --num-executors 40 --driver-memory 40G --executor-memory 40G --executor-cores 6 --conf spark.sql.shuffle.partitions=12 --conf spark.default.parallelism=15 $script_path/sparkjob-0.0.9-SNAPSHOT.jar $inputfile $OutputPath
for subdir in /mapr/$OutputPath/filename*; do
if [[ -d $subdir ]]; then
for file in $subdir/*; do
fl_nm=`echo $subdir | awk -F"filename=" '{print $NF}' | sed 's/\/.*//g'`
cat $file >> /mapr/$OutputPath/$fl_nm
done
if [ $? -eq 0 ]; then
rm -rf $subdir
fi
fi
done
done
