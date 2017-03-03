
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class mySamplingTest {

    // babababbaba

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

        long startTime = System.nanoTime();

        JavaRDD<String> inputFile = sc.textFile(fileName); //Spark

        //long count = inputFile.count(); //Spark

        JavaRDD<myPoint2> pointData = inputFile.map(new Function<String, myPoint2>() {
            public myPoint2 call(String s) {
                //myPoint2 result = s.trim().toUpperCase();

                //final String tokenSplit = "\t";
                final String tokenSplit = ",";
                String[] parts = s.split(tokenSplit);
                myPoint2 pt = new myPoint2(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));

                return pt;
            }
        });

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

        System.out.println("--> Turn to points");

        final int mega = 1000000;
        final int sampleSize = (memoryBudget * mega) / 16;

        //List<String> sample2 = inputFile.takeSample(false, (int) sampleSize); //Spark

        List<myPoint2> sample = pointData.takeSample(false, (int) sampleSize); //Spark

        System.out.println("--> Took the sample = " + sample.size() + " - " + sampleSize);

        List<myPoint2> points = new ArrayList<myPoint2>(sample.size());

        System.out.println("--> Create the list");

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
