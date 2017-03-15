import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class nonUniformBinningTest {

    long[] queryPoints = { 539289201, 2102587674, 177338570, 1684050323, 1972444342, 1263939584, 2620760160L,
            2264956411L, 1830550771, 1340526882, 977287718, 1445957120, 1031142987, 2196891539L, 1762414965, 1238788844,
            183327226, 2476234227L, 1154008328, 1480326738 };

    // tweets points
    //long[] queryPoints = {3971781, 10081489, 8430327, 6530062, 2894280} ;

    private ArrayList<myPoint2> queryPointsCoordinates = new ArrayList<myPoint2>();

    public static void binning(JavaSparkContext sc, int memoryBudget, String fileName, int type, double selectivity)
            throws IOException {

        long startTime = System.nanoTime();

        JavaRDD<String> inputFile = sc.textFile(fileName); //Spark

        //
        // find mins and maxs
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

        //
        //
        //

        //
        // --- create sample (on Spark) ---
        //

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

        //
        // --- Sort the sample on x (Spark) ---
        //
        /*
        final JavaRDD<myPoint2> sortedX = pointData.sortBy(new Function<myPoint2, Double>() {
            public Double call(myPoint2 value) throws Exception {
                return value.longitude;
            }
        }, true, 1);
        List<myPoint2> sortedXsample = sortedX.collect();
        for (int i = 0; i < sortedXsample.size(); i++)
            System.out.println("--> " + sortedXsample.get(i).longitude);
        */

        //
        // --- Sort sample on y (Spark) ---
        //
        /*
        final JavaRDD<myPoint2> sortedY = pointData.sortBy(new Function<myPoint2, Double>() {
            public Double call(myPoint2 value) throws Exception {
                return value.latitude;
            }
        }, true, 1);
        List<myPoint2> sortedYsample = sortedY.collect();
        //for (int i = 0; i < sortedsample.size(); i++)
        //    System.out.println("--> " + sortedsample.get(i).longitude);
        */

        //
        // --- Find the borders for the splits (local) ---
        //

        //long numBin = (memoryBudget * mega) / 8;
        long numBin = 50;
        double sqroot = Math.sqrt(numBin);
        int printezis = (int) sqroot; // number of bins in each side
        System.out.println("printezis = " + printezis);

        int numPointsBin = sampleSize / printezis; // numPointsBin = number of points in each bin
        int restPoint = sampleSize % printezis;
        if (restPoint >= (numPointsBin / 2)) {
            numPointsBin++;
            System.out.println("Increase the numPointsBin");
        }
        System.out.println("numPointsBin = " + numPointsBin);

        List<Double> xborders = new ArrayList<Double>();
        List<Double> yborders = new ArrayList<Double>();
        myPoint2 temp;

        List<myPoint2> sample = pointData.collect();
        ArrayList<myPoint2> sortedSample = new ArrayList<myPoint2>(sample);
        sample = null;

        quickSort.quickSort(sortedSample, 0, (sortedSample.size() - 1), 0); // sorting on longitude
        System.out.println("sample size = " + sortedSample.size());
        xborders.add(myMbr.minX); // create the xborders
        for (int i = 1; i < printezis; i++) {
            temp = sortedSample.get(i * numPointsBin);
            xborders.add(temp.longitude);
        }
        xborders.add(myMbr.maxX);

        quickSort.quickSort(sortedSample, 0, (sortedSample.size() - 1), 1); // sorting on latitude
        System.out.println("sample size = " + sortedSample.size());
        yborders.add(myMbr.minY); // create the yborders
        for (int i = 1; i < printezis; i++) {
            temp = sortedSample.get(i * numPointsBin);
            yborders.add(temp.latitude);
        }
        yborders.add(myMbr.maxY);

        sortedSample = null;

        //
        // --- Calculate histograms ---
        //

        JavaRDD<long[]> histograms = mbrData.mapPartitions(new FlatMapFunction<Iterator<mbr>, long[]>() {
            @Override
            public Iterator<long[]> call(Iterator<mbr> j) throws Exception {
                List<Long> binCounter = new ArrayList<Long>();
                int togoBin = 0;
                long tempcount = 0;
                final long zero = 0;
                for (long i = 0; i < printezis * printezis; i++) {
                    binCounter.add(zero);
                }
                while (j.hasNext()) {
                    mbr tempMbr = j.next();
                    //owerCaseLines.add(line.toLowerCase());

                    int x = 0;
                    x = Collections.binarySearch(xborders, tempMbr.maxX);
                    if (x < 0) {
                        x = 0 - x - 1;
                        x--;
                    } else if (x == printezis) {
                        x--;
                    }

                    int y = 0;
                    y = Collections.binarySearch(yborders, tempMbr.maxY);
                    if (y < 0) {
                        y = 0 - y - 1;
                        y--;
                    } else if (y == printezis) {
                        y--;
                    }

                    //System.out.println("x - " + x + ", xborders.get(x) = " + xborders.get(x));
                    //System.out.println("y = " + y + ", yborders.get(y) = " + yborders.get(y));
                    togoBin = (int) ((y * printezis) + x);
                    tempcount = binCounter.get(togoBin);
                    tempcount++;
                    binCounter.set(togoBin, tempcount);
                }
                //for (int i = 0; i < binCounter.size(); i++) {
                //    System.out.println("-> " + i + " : " + binCounter.get(i));
                //}

                long[] returnValue = new long[binCounter.size()];

                for (int i = 0; i < returnValue.length; i++) {
                    returnValue[i] = binCounter.get(i);
                    //System.out.println("-> " + i + " : " + returnValue[i]);
                }

                return Arrays.asList(returnValue).iterator();
            }
        });

        long[] histogram = histograms.reduce(new Function2<long[], long[], long[]>() {
            public long[] call(long[] h1, long[] h2) throws Exception {
                for (int i = 0; i < h1.length; i++) {
                    //System.out.println("-> " + h1[i] + " + " + h2[i]);
                    h1[i] = h1[i] + h2[i];
                }
                return h1;
            }
        });

        //for (int i = 0; i < histogram.length; i++) {
        //    System.out.println("-> " + i + " : " + histogram[i]);
        //}

        //
        // final count of points in each bin
        //

        System.out.println("----------");
        int x = 0;
        int y = 0;
        int tempBinUp = 0;
        int tempBinLeft = 0;
        int tempBinDiag = 0;
        for (int i = 0; i < histogram.length; i++) {
            x = i % printezis;
            y = i / printezis;
            //System.out.println(x + " - " + y);
            if ((x == 0) && (y == 0)) {
                // Do nothing
            } else if (x == 0) {
                tempBinLeft = ((y - 1) * printezis) + x;
                //tempcount1 = histogram.get(i) + histogram.get(tempBinLeft);
                //histogram.set(i, tempcount1);
                histogram[i] = histogram[i] + histogram[tempBinLeft];
            } else if (y == 0) {
                tempBinUp = (y * printezis) + (x - 1);
                //tempcount1 = histogram.get(i) + histogram.get(tempBinUp);
                //histogram.set(i, tempcount1);
                histogram[i] = histogram[i] + histogram[tempBinUp];
            } else {
                tempBinUp = (y * printezis) + (x - 1);
                tempBinLeft = ((y - 1) * printezis) + x;
                tempBinDiag = ((y - 1) * printezis) + (x - 1);
                //tempcount1 = histogram.get(i) + histogram.get(tempBinUp) + histogram.get(tempBinLeft) - histogram.get(tempBinDiag);
                //binCounter.set(i, tempcount1);
                histogram[i] = histogram[i] + histogram[tempBinUp] + histogram[tempBinLeft] - histogram[tempBinDiag];

            }
            //System.out.println(x + " - " + y + " - " + binCounter.get(i)) ;
        }

        //for (int i = 0; i < histogram.length; i++) {
        //    System.out.println("-> " + i + " : " + histogram[i]);
        //}

        long endTime = System.nanoTime();
        long duration = (endTime - startTime); //divide by 1000000 to get milliseconds
        System.out.println("-----> Data process time nonUniform: " + (duration / 1000000000) + " - For memory budget : "
                + memoryBudget); // print result in seconds

        //
        //
        //

        /*
        for (int i = 0; i < queryPoints.length; i++) {
            //for (int i = 0 ; i < 1 ; i++) {
            long startTime = System.nanoTime();
        
            answerQuery(i, maxLongitude, minLongitude, maxLatitude, minLatitude, selectivity, xborders, yborders,
                    printezis);
        
            long endTime = System.nanoTime();
            long duration = (endTime - startTime); //divide by 1000000 to get milliseconds
            System.out.println("Execution time query " + i + " : " + duration / 1000);
        }
        
        selectivity = 0.001;
        double realSelectivity = selectivity / 100;
        System.out.println("----------------------------------> selectivity = " + selectivity);
        for (int i = 0; i < queryPoints.length; i++) {
            //for (int i = 0 ; i < 1 ; i++) {
            long startTime = System.nanoTime();
        
            answerQuery(i, maxLongitude, minLongitude, maxLatitude, minLatitude, realSelectivity, xborders, yborders,
                    printezis);
        
            long endTime = System.nanoTime();
            long duration = (endTime - startTime); //divide by 1000000 to get milliseconds
            System.out.println("Execution time query " + i + " : " + duration / 1000);
        }
        */
    }

}
