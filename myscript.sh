#!/bin/bash
echo "Hello World! PLEASE HELP"

cd
cd spark/sbin/

./stop-all.sh

./start-master.sh

./start-slave.sh spark://harry-Lab:7077


cd ..

inputfiles='/media/harry/MyPassport/tweets-2014-06-14'
membudget='10 100 250 500'

for i in {1..3}
do
	for j in $membudget
	do
		for k in $inputfiles
		do
			./bin/spark-submit  --class "test" --master spark://harry-Lab:7077  /home/harry/workspace/mytest/target/mytest-1.0.jar $i $j $k |& tee -a /home/harry/Desktop/myBIGoutput
		done
	done
done


#inputfile=$1

#./bin/spark-submit  --class "test" --master spark://harry-Lab:7077  /home/harry/workspace/mytest/target/mytest-1.0.jar $inputfile |& tee /home/harry/Desktop/myoutput


cd
cd spark/sbin/

./stop-all.sh
