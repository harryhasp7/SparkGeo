#!/bin/bash
echo "Spark Cluster Experiments"

inputfile='all_nodes'
#inputfile='tweets-2014-06-14'
membudget='10 100 250 500'


for i in {1..3}
do
	for j in $membudget
	do
		for k in $inputfile
		do
#			./bin/spark-submit  --class "test" --master spark://harry-Lab:7077 mytest-1.0.jar $i $j $k |& tee -a myBIGoutput
			./bin/spark-submit  --class "test" mytest-1.0.jar $i $j $k |& tee -a myBIGoutput
		done
	done
done

