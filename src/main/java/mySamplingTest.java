
import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class mySamplingTest {

    static long[] queryPoints = { 539289201, 2102587674, 177338570, 1684050323, 1972444342, 1263939584, 2620760160L,
            2264956411L, 1830550771, 1340526882, 977287718, 1445957120, 1031142987, 2196891539L, 1762414965, 1238788844,
            183327226, 2476234227L, 1154008328, 1480326738 };

    // tweets points
    //long[] queryPoints = {3971781, 10081489, 8430327, 6530062, 2894280} ;

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
                //final String tokenSplit = "\t";
                final String tokenSplit = ",";
                String[] parts = s.split(tokenSplit);
                myPoint2 pt = new myPoint2(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));

                return pt;
            }
        });

        List<myPoint2> sample = pointData.collect();
        //List<myPoint2> sample = pointData.takeSample(false, (int) sampleSize); //Spark

        System.out.println("--> Took the sample = " + sample.size() + " - " + sampleSize);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime); // time in nanoseconds
        System.out.println("-----> Data process time: " + duration); // print result in nanoseconds
        //System.out.println("-----> Data process time: " + (duration/1000000000)); // print result in seconds

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

}
