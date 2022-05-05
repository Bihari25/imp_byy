#!/bin/sh
###############################################################################
#
#    SCRIPT NAME :  AcoRxClaimWithFilter.sh
#
#    Description :
#                 The Script read the Rx Claims File, Validate against provided ACO File
#                 And generate output file in outbound directory. The script process file "WITH"
#                 Filter
#
#    USAGE:
#    sh AcoRxClaimWithFilter.sh <SCRIPT_PATH> <Input file path> <ACO File> <Outbound File Path> <Log file path> <Queue Name > >> <LOG FILE NAME> 2>&1
#
#
#    e.g. nohup sh AcoRxClaimWithFilter.sh /mapr/datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_scripts/pha /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_landing/stg43/stg43_incremental/pha/test /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_staging/UAT/4ACOS_June_Roster_368_UAT.txt /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_outbound/stg43/stg43_incremental/pha/test /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_logs/stg43/stg43_incremental/pha/test ihrgpbat_q1 >> /mapr/datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_logs/stg43/stg43_incremental/pha/test/test.log 2>&1 &
#
#
#
#    Version    Date           Developer           Comment
#    -------------------------------------------------------------------------
#
#    1.0     03-SEPT-2018    Nilabh/Vivek       Initial Version
#
###############################################################################

SCRIPT=`basename $0`
export CurrDate=`date '+%Y%m%d'`

#Input Parameter
script_path=$1
inputfilepath=$2
ACOFile=$3
OutputPath=$4
LogPath=$5
Qname=$6

ls -1 /mapr/${inputfilepath}/*RXCHF70CL.TXT | while read rx_fl
do

Filename=`echo $rx_fl | rev | cut -d"/" -f1  | rev`
inputfile=${inputfilepath}/${Filename}

echo "Processing $inputfile"
#Spark Command

spark-submit --class com.cadillac.spark.sparkjob.RXClaimsFilter_New3 --master yarn --queue $Qname --num-executors 40 --driver-memory 40G --executor-memory 40G --executor-cores 6 --conf spark.sql.shuffle.partitions=12 --conf spark.default.parallelism=15 $script_path/sparkjob-0.0.9-SNAPSHOT.jar $inputfile $ACOFile $OutputPath $LogPath

#Post Script Command

for subdir in /mapr/$OutputPath/filename*; do
   if [[ -d $subdir ]]; then
      for file in $subdir/*; do
#fl_nm=`echo $subdir | awk -F"filename=" '{print $NF}' | sed 's/\/.*//g'`
          cat $file >> /mapr/$OutputPath/$Filename
      done
      if [ $? -eq 0 ]; then
         rm -rf $subdir
      fi
   fi
done

for subdir in /mapr/$LogPath/filename*; do
    if [[ -d $subdir ]]; then
#fl_nm=`echo $subdir | awk -F"filename=" '{print $NF}' | sed 's/\/.*//g'`
       if [[ -e /mapr/$LogPath/$Filename ]]
       then
          rm -f /mapr/$LogPath/$Filename
       fi
       echo "ALT_ID|SUBSCRIBER_ID|INDV_ID|MEMBER_FIRST_NAME|MEMBER_LAST_NAME|MEMBER_BIRTH_DATE|MEMBER_GENDER|MEMBER_ID_NUMBER|PATIENT_FIRST_NAME|PATIENT_LAST_NAME|PATIENT_DATE_OF_BIRTH|GENDER" >> /mapr/$LogPath/$fl_nm
       for file in $subdir/*; do
#fl_nm=`echo $subdir | awk -F"filename=" '{print $NF}' | sed 's/\/.*//g'`
           cat $file >> /mapr/$LogPath/$Filename
       done

if [ $? -eq 0 ]; then
rm -rf $subdir
fi
fi
done
done
