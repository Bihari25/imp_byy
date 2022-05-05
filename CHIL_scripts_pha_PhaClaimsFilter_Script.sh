#usage:
#sh scriptname <claiminputfilename> <ACOinputfile> <outputpath> 
#sample:
#sh PhaClaims_Script.sh /datalake/optum/optuminsight/udw/ihr/dev/d_landing/pha/inbox/UHCGOVT20180325_RXCHF70CL.TXT /datalake/optum/optuminsight/udw/ihr/dev/d_landing/ACO/inbox/ACOS_TEST33_417_MEMBERS.csv /datalake/optum/optuminsight/udw/ihr/dev/d_outbound/pha
inputfile=$1
ACOFile=$2
OutputPath=$3
spark-submit --class com.cadillac.spark.sparkjob.RXClaimsFilter --master yarn-cluster --queue ihrgpstg_q1 --driver-memory 10G --executor-memory 10G  --num-executors 10 --executor-cores 6  --conf spark.sql.shuffle.partitions=12 --conf spark.default.parallelism=15 sparkjob-0.0.9-SNAPSHOT.jar $inputfile $ACOFile $OutputPath
for f in /mapr/$OutputPath/*.csv; do
mv -- "$f" "${f%.csv}.txt"
done
