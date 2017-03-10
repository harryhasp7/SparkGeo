
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class mySamplingTest {

    static long[] queryPoints = { 539289201, 2102587674, 177338570, 1684050323, 1972444342, 1263939584, 2620760160L,
            2264956411L, 1830550771, 1340526882, 977287718, 1445957120, 1031142987, 2196891539L, 1762414965, 1238788844,
            183327226, 2476234227L, 1154008328, 1480326738 };

    // tweets points
    //long[] queryPoints = {3971781, 10081489, 8430327, 6530062, 2894280} ;

    //private myPoint2[] queryPointsCoordinates ;
    //private ArrayList<myPoint2> queryPointsCoordinates = new ArrayList<myPoint2>();

    //private double[] splits;

    //private ArrayList<myLeaf> leafNodes = new ArrayList<myLeaf>();

    //private int capacity = 10;

    public static void sampleSpark(JavaSparkContext sc, int memoryBudget, String fileName, int type, double selectivity)
            throws IOException {

        //long startTime = System.nanoTime();

        JavaRDD<String> inputFile = sc.textFile(fileName); //Spark

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
                if (pt.minY < -175.0) {
                    System.out.println(pt.minY);
                }

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

        /*
        Comparator<myPoint2> compX = new myComparatorX();
        myPoint2 maxX = pointData.max(compX);
        System.out.println("--> max X = " + maxX.longitude);
        myPoint2 minX = pointData.min(compX);
        System.out.println("--> min X = " + minX.longitude);
        Comparator<myPoint2> compY = new myComparatorY();
        myPoint2 maxY = pointData.max(compY);
        System.out.println("--> max Y = " + maxY.latitude);
        myPoint2 minY = pointData.min(compY);
        System.out.println("--> min Y = " + minY.latitude);
        */

        //System.out.println("--> Turn to points");

        long startTime = System.nanoTime();

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
                //myPoint2 result = s.trim().toUpperCase();

                //final String tokenSplit = "\t";
                final String tokenSplit = ",";
                String[] parts = s.split(tokenSplit);
                myPoint2 pt = new myPoint2(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));

                return pt;
            }
        });

        //List<String> sample2 = inputFile.takeSample(false, (int) sampleSize); //Spark

        List<myPoint2> sample = pointData.collect();
        //List<myPoint2> sample = pointData.takeSample(false, (int) sampleSize); //Spark

        System.out.println("--> Took the sample = " + sample.size() + " - " + sampleSize);

        System.out.println("--> Create the list");
        /**/
        long endTime = System.nanoTime();
        long duration = (endTime - startTime); //divide by 1000000 to get milliseconds
        System.out.println("-----> Data process time: " + duration / 1000000000);

        for (int i = 0; i < queryPoints.length; i++) {
            startTime = System.nanoTime();

            //answerQuery(i, maxLongitude, minLongitude, maxLatitude, minLatitude, selectivity, count, sampleSize);

            endTime = System.nanoTime();
            duration = (endTime - startTime); //divide by 1000000 to get milliseconds
            System.out.println("Execution time query " + i + " : " + duration / 1000);
        }
    }

}
