# Medical_Claims_Script.sh
# Author : Sravanthi
# 05-Mar-2018
#
# This bash shell script to  Split the claims
#
#--------------------------------------------------------------------------------------------------------------------
#                            CHANGE LOG
#--------------------------------------------------------------------------------------------------------------------
# DATE          AUTHOR          TAG                        DESCRIPTION
#--------------------------------------------------------------------------------------------------------------------
#
#--------------------------------------------------------------------------------------------------------------------
# USAGE:
# sh Medical_Claims_Script.sh <scriptPaht> <landingLoc> <Inbound> <OutputPaht> &
#
#Ex:sh Medical_Claims_Script.sh /mapr/datalake/optum/optuminsight/udw/dev/ihr/dev/d_scripts/clm /mapr/datalake/optum/optuminsight/udw/dev/ihr/dev/d_landing/clm/inbox /datalake/optum/optuminsight/udw/dev/ihr/dev/d_inbound/clm/inbox /datalake/optum/optuminsight/udw/dev/ihr/dev/d_outbound/clm 
# set -x

date
script_path=$1
landingLoc=/mapr/$2
Inbound=/mapr/$3
ClaimFile=$3/thread*x12.file*.txt
OutputFile=$4
OutputFileName=med_claims_chwy_x12_$(date +%Y%m%d%H%M%S%3N).txt


cp $landingLoc/* $Inbound/
awk -F'~'  '{gsub("^0*", "", $6); gsub ("^0*", "", $7)} 1' OFS='~' $Inbound/thread*metadata.file*.txt > $Inbound/thread_metadata.txt
if [ $? -eq 0 ]; then
metaDataFile=$3/thread_metadata.txt
spark-submit --class com.cadillac.spark.sparkjob.MedicalClaims  --master yarn --queue ihrgpbat_q1 --num-executors 10 --driver-memory 10G --executor-memory 10G --executor-cores 6 --conf spark.sql.shuffle.partitions=12 --conf spark.default.parallelism=15  $script_path/sparkjob-0.0.9-SNAPSHOT.jar $metaDataFile $ClaimFile $OutputFile
date
fi
for file in /mapr$OutputFile; do
if [ -f $file/part*.txt ]; then
#echo "$subdir"
#echo "File Count is - $fl_cnt"
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
