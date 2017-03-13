#!/bin/bash
echo "Hello World! PLEASE HELP"

cd
cd spark/sbin/

./stop-all.sh

./start-master.sh

./start-slave.sh spark://harry-Lab:7077


cd ..

inputfile=$1

./bin/spark-submit  --class "test" --master spark://harry-Lab:7077  /home/harry/workspace/mytest/target/mytest-1.0.jar $inputfile |& tee /home/harry/Desktop/myoutput


cd
cd spark/sbin/

./stop-all.sh
