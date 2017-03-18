import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class partialBinningTest {

    long[] queryPoints = { 539289201, 2102587674, 177338570, 1684050323, 1972444342, 1263939584, 2620760160L,
            2264956411L, 1830550771, 1340526882, 977287718, 1445957120, 1031142987, 2196891539L, 1762414965, 1238788844,
            183327226, 2476234227L, 1154008328, 1480326738 };

    private ArrayList<myPoint2> queryPointsCoordinates = new ArrayList<myPoint2>();

    //private List<Long> binCounter = new ArrayList<Long>();

    public static void binning(JavaSparkContext sc, int memoryBudget, String fileName, int type, double selectivity)
            throws IOException {

        long startTime = System.nanoTime();

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

                final String tokenSplit = "\t";
                //final String tokenSplit = ",";
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

        System.out.println("selectivity = " + selectivity);

        final double totalx = myMbr.maxX - myMbr.minX; // find the total area size, edge of square, etc
        final double totaly = myMbr.maxY - myMbr.minY;
        final double totalArea = totalx * totaly;
        final double edge = Math.sqrt(selectivity * totalArea);

        System.out.println("minLongitude = " + myMbr.minX);
        System.out.println("maxLongitude = " + myMbr.maxX);
        System.out.println("minLatitude = " + myMbr.minY);
        System.out.println("maxLatitude = " + myMbr.maxY);
        System.out.println("totalx = " + totalx);
        System.out.println("totaly = " + totaly);
        System.out.println("totalArea = " + totalArea);
        System.out.println("edge = " + edge);

        //List<Long> binCounter = new ArrayList<Long>();

        final int mega = 1000000; // find the split
        //final long numBin = (memoryBudget * mega) / 8;
        final long numBin = 50;
        //long numBin = 100 ;
        double sqroot = Math.sqrt(numBin);
        final int printezis = (int) sqroot; // timh pou 8a ginetai to split
        final double xSplit = (myMbr.maxX - myMbr.minX) / printezis;
        final double ySplit = (myMbr.maxY - myMbr.minY) / printezis;

        JavaRDD<long[]> histograms = mbrData.mapPartitions(new FlatMapFunction<Iterator<mbr>, long[]>() {
            @Override
            public Iterator<long[]> call(Iterator<mbr> x) throws Exception {
                List<Long> binCounter = new ArrayList<Long>();
                int togoBin = 0;
                long tempcount = 0;
                final long zero = 0;
                for (long i = 0; i < printezis * printezis; i++) {
                    binCounter.add(zero);
                }
                while (x.hasNext()) {
                    mbr tempMbr = x.next();
                    //owerCaseLines.add(line.toLowerCase());

                    double tempbin = (tempMbr.maxX - myMbr.minX) / xSplit;
                    long xbin = (long) tempbin;
                    //System.out.println(longitude + " - " + xbin);
                    tempbin = (tempMbr.maxY - myMbr.minY) / ySplit;
                    long ybin = (long) tempbin;
                    //System.out.println(latitude + " - " + ybin);

                    if (tempMbr.maxX == myMbr.maxX) {
                        xbin--;
                    }
                    if (tempMbr.maxY == myMbr.maxY) {
                        ybin--;
                    }

                    togoBin = (int) ((ybin * printezis) + xbin);
                    //System.out.println("---> " + togoBin);

                    //System.out.print("---> " + binCounter.get(togoBin));
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

        // final count of points in each bin
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
        System.out.println("-----> Data process time partialBinning: " + (duration / 1000000000)
                + " - For memory budget : " + memoryBudget); // print result in seconds

        /*
        for (int i = 0; i < queryPoints.length; i++) {
            startTime = System.nanoTime();
        
            answerQuery(i, maxLongitude, minLongitude, maxLatitude, minLatitude, printezis, xSplit, ySplit,
                    selectivity);
        
            endTime = System.nanoTime();
            duration = (endTime - startTime); //divide by 1000000 to get milliseconds
            System.out.println("Execution time query " + i + " : " + duration / 1000);
        }
        selectivity = 0.1;
        double realSelectivity = selectivity / 100;
        System.out.println("----------------------------------> selectivity = " + selectivity);
        for (int i = 0; i < queryPoints.length; i++) {
            startTime = System.nanoTime();
        
            answerQuery(i, maxLongitude, minLongitude, maxLatitude, minLatitude, printezis, xSplit, ySplit,
                    realSelectivity);
        
            endTime = System.nanoTime();
            duration = (endTime - startTime); //divide by 1000000 to get milliseconds
            System.out.println("Execution time query " + i + " : " + duration / 1000);
        }
        */
    }

}
