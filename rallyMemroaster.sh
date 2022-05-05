SCRIPT=`basename $0`
export CurrDate=`date '+%Y%m%d'`
export CurrDateTime=`date '+%Y%m%d%H%M%S'`

echo "Script Start Time - `date`"

if [ ! $# -eq 6 ]
  then
    echo "**************************************************************************"
    echo "Please pass the exact number of arguments required to run this script"
    echo "**************************************************************************"
    exit 1
fi


#Input Parameter
script_path=$1
oldmasterfile=$2
newmasterfile=$3
outputroasterpath=$4
LogFile=$5
email_from=$6
email_to=$7
Qname=$8

echo
echo "Script Path -$script_path"
echo "old master File Path - $oldmasterfile"
echo "new master File Path - $newmasterfile"
echo "outputroasterpath - $outputroasterpath"
echo "LogFile - $LogFile"
echo "email_from - $email_from"
echo "email_to - $email_to"
echo "Qname - $Qname"





echo "Starting the  spark job"
echo " Spark Job - `date`"

######################################################################################################
# Spark submit for Rally member Roaster
######################################################################################################


spark-submit --class com.cadillac.spark.sparkjob.rallyMemberRoster --master yarn --queue $Qname --packages javax.mail:mail:1.4.3 --driver-memory 35G --executor-memory 35G  --num-executors 35 --executor-cores 5 --conf spark.sql.shuffle.partitions=200 --conf spark.default.parallelism=200 $script_path/ihr-chil-rallyloopback-1.0.0-SNAPSHOT.jar $oldmasterfile $newmasterfile $outputroasterpath $LogFile $email_from $email_to

 if [ $? -eq 0 ]; then
  echo "Spark job completed for rally member roaster"
  echo "Post Spark - rally member roaster - `date`"
 fi

done

echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"


exit 0

echo "Spark job processing Completed - `date`"