#!/bin/sh
###############################################################################
#
#    SCRIPT NAME :  AcoMediClaimWithoutFilter.sh
#
#    Description :
#                 The Script read the MEDICAL Claims File, Doesn't perform any validation
#                 against provided ACO File and generate output file in outbound directory. 
#                 The script process file "WITHOUT" Filter
#                 
#    USAGE:
#    sh AcoMediClaimWithoutFilter.sh <SCRIPT_PATH> <LANDING_LOCATION> <INBOUND_LOCATION> <OUTPUTFILE_LOCATION> <QUEUE> >> <LOG FILE NAME> 2>&1
#
#    e.g. nohup sh AcoMediClaimWithoutFilter.sh /mapr/datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_scripts/clm /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_landing/stg43/stg43_incremental/clm/test /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_landing/stg43/stg43_incremental/clm/test /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_outbound/clm/test ihrgpbat_q1 >> /mapr/datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_logs/clm/test/test_medical_withoutfilter_20180904.log 2>&1 &
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
#exec 1>>/mapr/$6/${SCRIPT}_${CurrDate}.log 2>&1

date
script_path=$1
landingLoc=/mapr/$2
Inbound=/mapr/$3
ClaimFile=$3/thread*x12.file*.txt
OutputFile=$4
OutputFileName=med_claims_chwy_x12_$(date +%Y%m%d%H%M%S%3N).txt
Qname=$5

#  doing some lookup in medatdata file 
awk -F'~'  '{gsub("^0*", "", $6); gsub ("^0*", "", $7)} 1' OFS='~' $Inbound/thread*metadata.file*.txt > $Inbound/thread_metadata.txt
if [ $? -eq 0 ]; then
   metaDataFile=$3/thread_metadata.txt

# Spark command
spark-submit --class com.cadillac.spark.sparkjob.MedicalClaims  --master yarn --queue $Qname --num-executors 40 --driver-memory 40G --executor-memory 40G --executor-cores 6 --conf spark.sql.shuffle.partitions=12 --conf spark.default.parallelism=15  $script_path/sparkjob-0.0.9-SNAPSHOT.jar $metaDataFile $ClaimFile $OutputFile

#Post Spark shell
   date
fi

for file in /mapr$OutputFile; do
    if [ -f $file/part*.txt ]; then
       cp $file/part*.txt /mapr/$OutputFile/$OutputFileName
       if [ $? -eq 0 ]; then
          rm -rf $file/part*
       fi
    fi
done

awk 'BEGIN{RS="IEA[*]1[*]000000001~\n+";ORS="IEA*1*000000001~\n"} {$1=$1}1' /mapr/$OutputFile/$OutputFileName > /mapr/$OutputFile/temp_$OutputFileName
sed 's/~ /~/g' /mapr/$OutputFile/temp_$OutputFileName > /mapr/$OutputFile/$OutputFileName

if [ $? -eq 0 ]; then
   rm -rf /mapr/$OutputFile/temp_$OutputFileName
fi

date
