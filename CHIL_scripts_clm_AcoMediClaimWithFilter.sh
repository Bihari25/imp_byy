#!/bin/sh
###############################################################################
#
#    SCRIPT NAME :  AcoMediClaimWithFilter.sh
#
#    Description :
#                 The Script read the MEDICAL Claims File, Validate against provided ACO File
#                 And generate output file in outbound directory. The script process file "WITH"
#                 Filter
#                 
#    USAGE:
#    sh AcoMediClaimWithFilter.sh <JAR_SCRIPT_PATH> <LANDING_LOCATION> <INBOUND_LOCATION>  <ACO FILE> <OUTPUTFILE_LOCATION> <LOG LOCATION> <PRE-INBOUND LOCATION> <QUEUE> >> <LOG FILE NAME> 2>&1 &
#    e.g nohup sh AcoMediClaimWithFilter.sh /mapr/datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_scripts/clm /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_landing/stg43/stg43_incremental/clm/test /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_landing/stg43/stg43_incremental/clm/test /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_staging/UAT/4ACOS_June_Roster_368_UAT.txt  /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_outbound/clm/test /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_logs/clm/test/ /datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_preinbound/test ihrgpbat_q1 >> /mapr/datalake/optum/optuminsight/p_edh/prd/uhc/ihr_stg/stg/s_logs/clm/test/test.log 2>&1 &
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
##exec 1>>/mapr/$6/${SCRIPT}_${CurrDate}.log 2>&1

set -x
date
#input Parameter

script_path=$1
landingLoc=/mapr/$2
Inbound=/mapr/$3
ACOFile=$4
OutputFile=$5/output/
LogPath=$6
PreInbound=$7
Qname=$8

echo
echo
echo "Input Path - $Inbound"
echo "Metadata File - $metaDataFile"
echo "Claim File - $ClaimFile"
echo "Output File - $OutputFileName"
echo "Starting Spark Code"
echo "LogPath - $LogPath"
echo "PreInbound Path - $PreInbound"
echo "Queue Name - $Qname"

ls -1 ${landingLoc}/*x12* | while read x12_fl
do

   clm_fl=`echo $x12_fl | awk -F"/" '{print $NF}' | sed 's/#/*/g' | sed 's/\[/*/g' | sed 's/\]/*/g'`
   mtdt_fl=`echo $clm_fl | sed 's/x12/metadata/g'`
   fl_tmstmp=`echo $x12_fl | awk -F"/" '{print $NF}' | awk -F"." '{print $1$2$3$4$5"_"$7}' | sed 's/-//g'|sed 's/thread//g'`
#metaDataFile=$3/thread*metadata.file*.txt
#ClaimFile=$3/thread*x12.file*.txt
   metaDataFile=$3/$mtdt_fl
   ClaimFile=$3/$clm_fl
   OutputFileName=Med_Claims_Chwy_x12_${fl_tmstmp}.txt

   echo "x12 File - $clm_fl"
   echo
   echo

#cp $landingLoc/thread* $Inbound/
#awk -F'~'  '{gsub ("^0*", "", $7)} 1' OFS='~' $Inbound/thread*metadata.file*.txt > $Inbound/thread_metadata.txt
#awk -F'~'  '{gsub("^0*", "", $6); gsub ("^0*", "", $7)} 1' OFS='~' $Inbound/$mtdt_fl > $Inbound/${mtdt_fl}_temp.txt
   awk -F'~'  '{gsub("^0*", "", $6); gsub ("^0*", "", $7)} 1' OFS='~' $Inbound/$mtdt_fl > /mapr/$PreInbound/${mtdt_fl}_temp.txt
#cp $Inbound/${mtdt_fl}_temp.txt $Inbound/$mtdt_fl

   cp /mapr/$PreInbound/${mtdt_fl}_temp.txt /mapr/$PreInbound/$mtdt_fl

   if [ $? -eq 0 ]; then
      rm -rf /mapr/$PreInbound/${mtdt_fl}_temp.txt
   fi

   metaDataFile=$PreInbound/$mtdt_fl

spark-submit --class com.cadillac.spark.sparkjob.MedicalClaimsFilter_new1 --master yarn --queue $Qname --num-executors 40 --driver-memory 40G --executor-memory 40G --executor-cores 6 --conf spark.sql.shuffle.partitions=12 --conf spark.default.parallelism=15 $script_path/sparkjob-0.0.9-SNAPSHOT.jar  $metaDataFile $ClaimFile $ACOFile $OutputFile $LogPath

#fi

   date

   for file in /mapr$OutputFile; do

       if [ -f $file/part*.txt ]; then
          cp $file/part*.txt /mapr/$OutputFile/$OutputFileName
          
          if [ $? -eq 0 ]; then
             rm -rf $file/part*
          fi
       fi
    done

    echo "Completed Copying Output File"

    for file in /mapr$LogPath; do
       if [ -f $file/part*.csv ]; then
          cp $file/part*.csv /mapr$LogPath/Med_Claims_Chwy_${fl_tmstmp}.log
          if [ $? -eq 0 ]; then
             rm -rf $file/part*
          fi
       fi
    done

    echo "Completed Logging"

    awk 'BEGIN{RS="IEA[*]1[*]000000001~\n+";ORS="IEA*1*000000001~\n"} {$1=$1}1' /mapr/$OutputFile/$OutputFileName > /mapr/$OutputFile/temp_$OutputFileName
    sed 's/~ /~/g' /mapr/$OutputFile/temp_$OutputFileName > /mapr/$OutputFile/$OutputFileName

    if [ $? -eq 0 ]; then
       rm -rf /mapr/$OutputFile/temp_$OutputFileName
    fi

    mv /mapr/$OutputFile/* /mapr/$5
    rmdir /mapr/$OutputFile

done
