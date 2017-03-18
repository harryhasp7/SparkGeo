
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class mySamplingTest {

    static long[] queryPoints = { 539289201, 2102587674, 177338570, 1684050323, 1972444342, 1263939584, 2620760160L,
            2264956411L, 1830550771, 1340526882, 977287718, 1445957120, 1031142987, 2196891539L, 1762414965, 1238788844,
            183327226, 2476234227L, 1154008328, 1480326738 };

    // tweets points
    //long[] queryPoints = {3971781, 10081489, 8430327, 6530062, 2894280} ;

    private final static int capacity = 10;

    private static double[] splits;

    private static ArrayList<myLeaf> leafNodes = new ArrayList<myLeaf>();

    public static void sampleSpark(JavaSparkContext sc, int memoryBudget, String fileName, int type, double selectivity)
            throws IOException {

        long startTime = System.nanoTime();

        JavaRDD<String> inputFile = sc.textFile(fileName); //Spark

        /*
        //
        // FInd mbr
        //
        
        class mbr implements Serializable {
            double maxX;
            double minX;
            double maxY;
            double minY;
        
            public mbr(double maxX, double minX, double maxY, double minY) {
                this.maxX = maxX;
                this.minX = minX;
                this.maxY = maxY;
                this.minY = minY;
            }
        }        
        JavaRDD<mbr> mbrData = inputFile.map(new Function<String, mbr>() {
            public mbr call(String s) {
                //myPoint2 result = s.trim().toUpperCase();
        
                //final String tokenSplit = "\t";
                final String tokenSplit = ",";
                String[] parts = s.split(tokenSplit);
                mbr pt = new mbr(Double.parseDouble(parts[1]), Double.parseDouble(parts[1]),
                        Double.parseDouble(parts[2]), Double.parseDouble(parts[2]));
        
                return pt;
            }
        });        
        mbr myMbr = mbrData.reduce(new Function2<mbr, mbr, mbr>() {
            public mbr call(mbr a, mbr b) {
                if (a.maxX < b.maxX) {
                    a.maxX = b.maxX;
                }
                if (a.minX > b.minX) {
                    a.minX = b.minX;
                }
                if (a.maxY < b.maxY) {
                    a.maxY = b.maxY;
                }
                if (a.minY > b.minY) {
                    a.minY = b.minY;
                }
        
                return a;
            }
        });
        System.out.println("--> max X = " + myMbr.maxX);
        System.out.println("--> min X = " + myMbr.minX);
        System.out.println("--> max Y = " + myMbr.maxY);
        System.out.println("--> min Y = " + myMbr.minY);
        */

        //long startTime = System.nanoTime();

        long count = inputFile.count(); //Spark
        System.out.println("--> count = " + count);

        final int mega = 1000000;
        final int sampleSize = (memoryBudget * mega) / 16;
        double fraction = (double) sampleSize / count;
        System.out.println("--> fraction = " + fraction);

        JavaRDD<String> sampleString = inputFile.sample(false, fraction);

        JavaRDD<myPoint2> pointData = sampleString.map(new Function<String, myPoint2>() {
            //JavaRDD<myPoint2> pointData = inputFile.map(new Function<String, myPoint2>() {
            public myPoint2 call(String s) {
                final String tokenSplit = "\t";
                //final String tokenSplit = ",";
                String[] parts = s.split(tokenSplit);
                myPoint2 pt = new myPoint2(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));

                return pt;
            }
        });

        List<myPoint2> sample = pointData.collect();
        //List<myPoint2> sample = pointData.takeSample(false, (int) sampleSize); //Spark

        System.out.println("--> Took the sample = " + sample.size() + " - " + sampleSize);

        long startTimeKD = System.nanoTime();
        ArrayList<myPoint2> treedSample = new ArrayList<myPoint2>(sample);
        sample = null;
        createKDtree(treedSample, capacity); // create KD tree
        long endTimeKD = System.nanoTime();
        long durationKD = (endTimeKD - startTimeKD); // time in nanoseconds

        long endTime = System.nanoTime();
        long duration = (endTime - startTime); // time in nanoseconds
        //System.out.println("-----> Data process time Samlping : " + duration + " - For memory budget : " + memoryBudget); // print result in nanoseconds
        System.out.println("-----> Data process time Samlping: " + (duration / 1000000000) + " - For memory budget : "
                + memoryBudget); // print result in seconds
        System.out.println("For KD tree we had : " + (durationKD / 1000000000) + "sec");
        /*
        //
        // Answer queries
        //
        
        for (int i = 0; i < queryPoints.length; i++) {
            startTime = System.nanoTime();
        
            //answerQuery(i, maxLongitude, minLongitude, maxLatitude, minLatitude, selectivity, count, sampleSize);
        
            endTime = System.nanoTime();
            duration = (endTime - startTime); //divide by 1000000 to get milliseconds
            System.out.println("Execution time query " + i + " : " + duration / 1000);
        }
        */
    }

    public static void createKDtree(ArrayList<myPoint2> points, int capacity) {

        long countSort = 0;
        long countMedian = 0;

        // Enumerate all partition IDs to be able to count leaf nodes in any split
        int numSplits = (int) Math.ceil((double) points.size() / capacity);
        System.out
                .println("points.size = " + points.size() + ", capacity = " + capacity + ", numSplits = " + numSplits);

        String[] ids = new String[numSplits]; //ids for leaf nodes

        for (int id = numSplits; id < 2 * numSplits; id++) {
            ids[id - numSplits] = Integer.toBinaryString(id);
        }
        /*
        // Keep splitting the space into halves until we reach the desired number of partitions
        @SuppressWarnings("unchecked")
        Comparator<myPoint2>[] comparators = new Comparator[] {
                new Comparator<myPoint2>() {
                    @Override
                    public int compare(myPoint2 a, myPoint2 b) {
                        return a.longitude < b.longitude ? -1 : (a.longitude > b.longitude ? 1 : 0);
                    }},
                new Comparator<myPoint2>() {
                    @Override
                    public int compare(myPoint2 a, myPoint2 b) {
                        return a.latitude < b.latitude ? -1 : (a.latitude > b.latitude ? 1 : 0);
                    }}
        };
        */
        class SplitTask {
            int fromIndex; // Incusive
            int toIndex; // Exclusive
            int direction;
            int partitionID; // the partition that will be created

            /** Constructor using all fields */
            public SplitTask(int fromIndex, int toIndex, int direction, int partitionID) {
                this.fromIndex = fromIndex;
                this.toIndex = toIndex;
                this.direction = direction;
                this.partitionID = partitionID;
            }

        }
        Queue<SplitTask> splitTasks = new ArrayDeque<SplitTask>();
        splitTasks.add(new SplitTask(0, points.size(), 0, 1));

        splits = new double[numSplits];

        while (!splitTasks.isEmpty()) {
            SplitTask splitTask = splitTasks.remove();
            if (splitTask.partitionID < numSplits) {
                /*
                //need to be split
                String child1 = Integer.toBinaryString(splitTask.partitionID * 2);
                String child2 = Integer.toBinaryString(splitTask.partitionID * 2 + 1);
                int size_child1 = 0 ;
                int size_child2 = 0 ;
                //System.out.println("child1 = " + child1 + "child2 = " + child2) ;
                for (int i = 0; i < ids.length; i++) {
                    if (ids[i].startsWith(child1)) {
                        //System.out.println("child1 ids = " + ids[i]);
                        size_child1++;
                    }
                    else if (ids[i].startsWith(child2)) {
                        //System.out.println("child2 ids = " + ids[i]) ;
                        size_child2++;
                    }
                }
                
                // Calculate the index which partitions the subrange into sizes proportional to size_child1 and size_child2
                //System.out.println("size_child1 = " + size_child1 + "size_child2 = " + size_child2) ;
                int splitIndex = (int) (((long)size_child1 * splitTask.toIndex + (long)size_child2 * splitTask.fromIndex) / (size_child1 + size_child2)) ;
                //System.out.println("splitIndex = " + splitIndex) ;
                */

                int splitIndex = (splitTask.fromIndex + splitTask.toIndex) / 2;
                //System.out.println("splitIndex = " + splitIndex) ;+

                if ((splitTask.toIndex - 1) - splitTask.fromIndex > 50000) {
                    myMedian.findMedian(points, splitTask.fromIndex, (splitTask.toIndex - 1), splitIndex,
                            splitTask.direction);
                    countMedian++;
                } else {
                    quickSort.quickSort(points, splitTask.fromIndex, (splitTask.toIndex - 1), splitTask.direction);
                    countSort++;
                }
                //quickSort.quickSort(points, splitTask.fromIndex, (splitTask.toIndex - 1), splitTask.direction ) ;
                //myMedian.findMedian(points, splitTask.fromIndex, (splitTask.toIndex - 1), splitIndex, splitTask.direction) ;
                /*
                Iterator itr = points.iterator();
                while(itr.hasNext()){
                    myPoint2 st=(myPoint2)itr.next();
                    System.out.println(st.longitude + "\t" + st.latitude);
                }
                System.out.println("----------");
                */
                myPoint2 splitValue = points.get(splitIndex);

                splits[splitTask.partitionID] = splitTask.direction == 0 ? splitValue.longitude : splitValue.latitude;

                splitTasks.add(new SplitTask(splitTask.fromIndex, splitIndex, 1 - splitTask.direction,
                        splitTask.partitionID * 2));

                splitTasks.add(new SplitTask(splitIndex, splitTask.toIndex, 1 - splitTask.direction,
                        splitTask.partitionID * 2 + 1));
            } else {
                // this is a leaf node - splittask contains the from and to indexes for the range points
                //System.out.println("from = " + splitTask.fromIndex + ", to = " + splitTask.toIndex) ;
                ArrayList<myPoint2> tempLeaf = new ArrayList<myPoint2>();
                for (int i = splitTask.fromIndex; i < splitTask.toIndex; i++) {
                    tempLeaf.add(points.get(i));
                }
                myLeaf temp = new myLeaf(splitTask.partitionID, tempLeaf);
                leafNodes.add(temp);
            }
            /*
            for (int i = 0 ; i < splits.length ; i++) {
                System.out.println("splits[" + i + "] = " + splits[i]);
            }
            System.out.println("----------");
            */
        }

        System.out.println("-----> On median = " + countMedian + " times");
        System.out.println("-----> On sort = " + countSort + " times");
        /*
        for (int i = 0 ; i < splits.length ; i++) {
            System.out.println("splits[" + i + "] = " + splits[i]);
        }
        for (int i = 0 ; i < leafNodes.size() ; i++) {
            System.out.println("For leaf No " + leafNodes.get(i).id + ":") ;
            for (int j = 0 ; j < leafNodes.get(i).leafList.size() ; j++) {
                System.out.println("x = " + leafNodes.get(i).leafList.get(j).longitude + ", y = " + leafNodes.get(i).leafList.get(j).latitude) ;
            }
            System.out.println("----------");
        }
        */
    }

}
