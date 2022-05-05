#usage:
#sh scriptname <inputfilename> <outputpath>
#sample:
#sh PhaClaims_Script.sh /datalake/optum/optuminsight/udw/dev/ihr/dev/d_landing/pha/inbound/UHCCOMM20180214_RXCHF70CL.TXT /datalake/optum/optuminsight/udw/dev/ihr/dev/d_landing/pha/inbound/Dev_Testing/ACO_MemberRoster_20180227-upd_01.txt /datalake/optum/optuminsight/udw/dev/ihr/dev/d_outbound/RXClaims
inputfile=$1
ACOFile=$2
OutputPath=$3
#LogPath=$4
spark-submit --class com.cadillac.spark.sparkjob.RXClaimsFilterwolog --master yarn --queue ihrgpbat_q1  --driver-memory 10G --executor-memory 10G  --num-executors 10 --executor-cores 6  --conf spark.sql.shuffle.partitions=12 --conf spark.default.parallelism=15   sparkjob-0.0.9-SNAPSHOT.jar $inputfile $ACOFile $OutputPath
#for f in /mapr/$OutputPath/*.csv; do
#mv -- "$f" "${f%.csv}.txt"
#done
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

#for subdir in /mapr/$LogPath/filename*; do
#if [[ -d $subdir ]]; then
#fl_nm=`echo $subdir | awk -F"filename=" '{print $NF}' | sed 's/\/.*//g'`
#if [[ -e /mapr/$LogPath/$fl_nm ]]
#then
#rm -f /mapr/$LogPath/$fl_nm
#fi
#echo "ALT_ID|SUBSCRIBER_ID|INDV_ID|MEMBER_FIRST_NAME|MEMBER_LAST_NAME|MEMBER_BIRTH_DATE|MEMBER_GENDER|MEMBER_ID_NUMBER|PATIENT_FIRST_NAME|PATIENT_LAST_NAME|PATIENT_DATE_OF_BIRTH|GENDER" >> /mapr/$LogPath/$fl_nm
#for file in $subdir/*; do
#fl_nm=`echo $subdir | awk -F"filename=" '{print $NF}' | sed 's/\/.*//g'`
#cat $file >> /mapr/$LogPath/$fl_nm
#done
#if [ $? -eq 0 ]; then
#rm -rf $subdir
#fi
#fi
#done

