#	The two arguments passed to this
#	should be the name of the query
#	filename, the number of results
#	to be retrieved, and the output
# 	folder.

queryFilename=$1
numResults=$2
outputFolder=$3
queryResultName=$queryFilename".queresult"

echo ""
echo "******************************** NOTE *********************************"
echo "Please make sure that this script is being run by the appropriate user."
echo "Also make sure that all proper Hadoop environment variables are set."
echo "***********************************************************************"
echo ""

#	Delete the output folder first.
hdfs dfs -rm -R $outputFolder
hdfs dfs -rm $queryResultName
rm $queryResultName

#	Now run the query
hadoop jar CBIRQuery.jar inq.txt $queryFilename $numResults $outputFolder

#	Now fetch the query results
hdfs dfs -test -e $queryResultName
rc=$?

while [ "$rc" == "1" ]
do
	#echo no file
	hdfs dfs -test -e $queryResultName > /dev/null 2>&1
	rc=$?
done

counter=0

while [ $counter -lt 10 ]
do	 
	string=''
	i=0
	while [ $i -lt $counter ]
	do
		string=$string"==" 
		i=`expr $i + 1`
	done
	string=$string"==>"
	while [ $i -lt 9 ]
	do
		string=$string"__"    
		i=`expr $i + 1`
	done

	counter=`expr $counter + 1`
	perc=`expr $counter \* 10`
	echo -ne 'Fetching requested file[' $string '](' $perc '%)\r'
	sleep 1
done

hdfs dfs -get $queryResultName > /dev/null 2>&1
echo -ne "Requested file has been fetched..                                \r"
