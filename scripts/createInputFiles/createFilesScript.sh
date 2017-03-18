#!/bin/bash
echo "Create new files"

inputfile='all_nodes'
outNum='536480353 1072960705 1609441058 2145921410'

membudget='10 100 250 500'

#inputfile='tweets-2014-06-14'
#outNum='2626851 5253702 7880552'

k=1
one=1
num=20

for i in $outNum
do
	temp=`expr $k \* $num`
#	echo $temp
	name="0$temp$inputfile"
	echo $name
	shuf -n $i $inputfile > $name

	for i in {1..3}
	do
		for j in $membudget
		do
			./bin/spark-submit  --class "test" mytest-1.0.jar $i $j $name |& tee -a outDifInputSizes
		done
	done

	k=`expr $k + $one`
done

echo "Done"
