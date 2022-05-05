# cat Medical_Claims_Script.sh
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
# sh Medical_claims_Filter.sh <scriptPaht> <landingLoc> <Inbound> <OutputPaht> &
#
#Ex:sh Medical_claims_Filter.sh /mapr/datalake/optum/optuminsight/udw/dev/ihr/dev/d_scripts/clm/Dev_Testing  /mapr/datalake/optum/optuminsight/udw/dev/ihr/dev/d_landing/clm/inbox /datalake/optum/optuminsight/udw/dev/ihr/dev/d_inbound/clm/inbox /datalake/optum/optuminsight/udw/dev/ihr/dev/d_landing/pha/inbound/ACO_MemberRoster_20180227-upd_matchingTargeted417.txt /datalake/optum/optuminsight/udw/dev/ihr/dev/d_outbound/clm
set -x
date
script_path=$1
landingLoc=/mapr/$2
Inbound=/mapr/$3
clm_fl=`echo $7 | awk -F"/" '{print $NF}' | sed 's/#/*/g' | sed 's/\[/*/g' | sed 's/\]/*/g'`
mtdt_fl=`echo $clm_fl | sed 's/x12/metadata/g'`
fl_tmstmp=`echo $7 | awk -F"/" '{print $NF}' | awk -F"." '{print $1$2$3$4$5"_"$7}' | sed 's/-//g'|sed 's/thread//g'`
#metaDataFile=$3/thread*metadata.file*.txt
#ClaimFile=$3/thread*x12.file*.txt
metaDataFile=$3/$mtdt_fl
ClaimFile=$3/$clm_fl
ACOFile=$4
OutputFile=$5
#ArchiveLoc=/mapr/datalake/optum/optuminsight/udw/dev/ihr/dev/d_archive/clm
LogPath=$6
OutputFileName=Med_Claims_Chwy_x12_${fl_tmstmp}.txt
PreInbound=$8

echo "x12 File - $clm_fl"
echo "Input Path - $Inbound"
echo "Metadata File - $metaDataFile"
echo "Claim File - $ClaimFile"
echo "Output File - $OutputFileName"
echo "Starting Spark Code"
echo "LogPath - $LogPath"
echo "PreInbound Path - $PreInbound"

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

spark-submit --class com.cadillac.spark.sparkjob.MedicalClaimsFilter_new1 --master yarn --queue ihrgpbat_q1 --num-executors 10 --driver-memory 10G --executor-memory 10G --executor-cores 6 --conf spark.sql.shuffle.partitions=12 --conf spark.default.parallelism=15 $script_path/sparkjob-0.0.9-SNAPSHOT.jar  $metaDataFile $ClaimFile $ACOFile $OutputFile $LogPath

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

