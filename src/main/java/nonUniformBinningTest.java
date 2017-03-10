import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class nonUniformBinningTest {

    long[] queryPoints = { 539289201, 2102587674, 177338570, 1684050323, 1972444342, 1263939584, 2620760160L,
            2264956411L, 1830550771, 1340526882, 977287718, 1445957120, 1031142987, 2196891539L, 1762414965, 1238788844,
            183327226, 2476234227L, 1154008328, 1480326738 };

    // tweets points
    //long[] queryPoints = {3971781, 10081489, 8430327, 6530062, 2894280} ;

    private ArrayList<myPoint2> queryPointsCoordinates = new ArrayList<myPoint2>();

    List<Long> binCounter = new ArrayList<Long>();

    public void binning(JavaSparkContext sc, int memoryBudget, String fileName, int type, double selectivity)
            throws IOException {

        long startTime = System.nanoTime();

        JavaRDD<String> inputFile = sc.textFile(fileName); //Spark

        //FileReader in = new FileReader("G:\\all_nodes");
        //FileReader in = new FileReader("G:\\tweets-2014-06-14");
        FileReader in = new FileReader(fileName);
        BufferedReader br = new BufferedReader(in);
        ArrayList<myPoint2> sample = new ArrayList<myPoint2>();

        String line;
        long count = 0;
        double minLongitude = 0.0;
        double maxLongitude = 0.0;
        double minLatitude = 0.0;
        double maxLatitude = 0.0;
        double testx = 0;
        double testy = 0;
        //long countSquare = 0 ;

        int mega = 1000000;

        //int tempSize = (memoryBudget * mega) / 3 ;

        // create sample

        int sampleSize = (memoryBudget * mega) / 16;
        System.out.println("sampleSize = " + sampleSize);

        while ((line = br.readLine()) != null) {
            String tokenSplit;
            if (type == 0) {
                tokenSplit = "\t";
                //String[] parts = line.toString().split("\t");
            } else if (type == 1) {
                tokenSplit = ",";
                //String[] parts = line.toString().split(",");
            } else {
                System.out.println("--> Wrong type for split in sampling");
                break;
            }
            String[] parts = line.toString().split(tokenSplit);
            double longitude = Double.parseDouble(parts[1]);
            double latitude = Double.parseDouble(parts[2]);
            //System.out.println(count + " " + line);

            if (count == 0) { // compute the min and max
                minLongitude = longitude;
                maxLongitude = longitude;
                minLatitude = latitude;
                maxLatitude = latitude;
            } else {
                if (minLongitude > longitude) {
                    minLongitude = longitude;
                } else if (maxLongitude < longitude) {
                    maxLongitude = longitude;
                }
                if (minLatitude > latitude) {
                    minLatitude = latitude;
                } else if (maxLatitude < latitude) {
                    maxLatitude = latitude;
                }
            }

            myPoint2 osfp = new myPoint2(longitude, latitude); // create new point from dataset

            if (count < sampleSize) { // den exoume gemisei to sample
                //System.out.println("count = " + count);
                sample.add(osfp);
            } else {
                //int ran = new Random().nextInt(count + 1) ;
                long ran = ThreadLocalRandom.current().nextLong(count + 1);
                int intRan = (int) ran;

                if (ran < sampleSize) {
                    sample.set(intRan, osfp);
                }

            }

            count++;

            /*
            if (count == 10) {
                break ;
            }
            */
            if (count % 100000000 == 0) {
                System.out.println("Don't worry - " + count);
            }
            if (count == pointNum) {
                System.out.println(count + " - " + longitude + " - " + latitude);
                testx = longitude;
                testy = latitude;
            }
            for (int i = 0; i < queryPoints.length; i++) {
                if (count == queryPoints[i]) {
                    System.out.println(count + " - " + longitude + " - " + latitude);
                    this.queryPointsCoordinates.add(osfp);
                    //this.queryPointsCoordinates[i].longitude = longitude ;
                    //this.queryPointsCoordinates[i].latitude = latitude ;
                }
            }
            /**/
            //System.out.println(count + " - Telos insert");
            /*
            Iterator itr = sample.iterator();
            while(itr.hasNext()){
                //System.out.println("malakas");
                myPoint st=(myPoint)itr.next();
                System.out.println(st.longitude + "\t" + st.latitude + "\t" + st.ranValue);
            }
            */
        }
        in.close();
        System.out.println("final count = " + count);

        System.out.println("sample size = " + sample.size());
        System.out.println("selectivity = " + selectivity);

        //double totalx = maxLongitude - minLongitude ;               // find the total area size, edge of square, etc
        //double totaly = maxLatitude - minLatitude ;
        //double totalArea = totalx * totaly ;
        //double edge =  Math.sqrt(selectivity * totalArea) ;

        //System.out.println("minLongitude = " + minLongitude);
        //System.out.println("maxLongitude = " + maxLongitude);
        //System.out.println("minLatitude = " + minLatitude);
        //System.out.println("maxLatitude = " + maxLatitude);
        //System.out.println("totalx = " + totalx);
        //System.out.println("totaly = " + totaly);
        //System.out.println("totalArea = " + totalArea);
        //System.out.println("edge = " + edge);

        // start nonUniformBinning pre-process

        long numBin = (memoryBudget * mega) / 8;
        double sqroot = Math.sqrt(numBin);
        int printezis = (int) sqroot; // number of bins in each side
        System.out.println("printezis = " + printezis);
        //if (sampleSize % printezis)
        int numPointsBin = sampleSize / printezis; // numPointsBin = number of points in each bin
        int restPoint = sampleSize % printezis;
        if (restPoint >= (numPointsBin / 2)) {
            numPointsBin++;
            System.out.println("Increase the numPointsBin");
        }
        System.out.println("numPointsBin = " + numPointsBin);
        List<Double> xborders = new ArrayList<Double>();
        List<Double> yborders = new ArrayList<Double>();
        //List<Long> binCounter = new ArrayList<Long>() ;
        long zero = 0;
        for (long i = 0; i < numBin; i++) {
            binCounter.add(zero);
        }

        int togoBin = 0;
        long tempcount = 0;
        myPoint2 temp;

        quickSort.quickSort(sample, 0, (sample.size() - 1), 0); // sorting on longitude
        System.out.println("sample size = " + sample.size());

        xborders.add(minLongitude); // create the xborders
        for (int i = 1; i < printezis; i++) {
            temp = sample.get(i * numPointsBin);
            xborders.add(temp.longitude);
        }
        xborders.add(maxLongitude);

        quickSort.quickSort(sample, 0, (sample.size() - 1), 1); // sorting on longitude
        System.out.println("sample size = " + sample.size());

        yborders.add(minLatitude); // create the yborders
        for (int i = 1; i < printezis; i++) {
            temp = sample.get(i * numPointsBin);
            yborders.add(temp.latitude);
        }
        yborders.add(maxLatitude);

        sample = null;

        in = new FileReader(fileName);
        br = new BufferedReader(in);

        count = 0;

        while ((line = br.readLine()) != null) {
            String tokenSplit;
            if (type == 0) {
                tokenSplit = "\t";
                //String[] parts = line.toString().split("\t");
            } else if (type == 1) {
                tokenSplit = ",";
                //String[] parts = line.toString().split(",");
            } else {
                System.out.println("--> Wrong type for split in sampling");
                break;
            }
            String[] parts = line.toString().split(tokenSplit);
            double longitude = Double.parseDouble(parts[1]);
            double latitude = Double.parseDouble(parts[2]);

            int x = 0;
            /*
            while (xborders.get(x) < longitude) {
                x++ ;
            }
            if (x == printezis) {
                x-- ;
            }
            */
            x = Collections.binarySearch(xborders, longitude);
            if (x < 0) {
                x = 0 - x - 1;
                x--;
            } else if (x == printezis) {
                x--;
            }

            int y = 0;
            /*
            while (yborders.get(y) < latitude) {
                y++ ;
            }
            if (y == printezis) {
                y-- ;
            }
            */
            y = Collections.binarySearch(yborders, latitude);
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

            if (count % 100000000 == 0) { // print gia na 3erw oti trexei (comment)
                System.out.println("Don't worry - " + count);
            }
            if (count == 0) { // print gia na 3erw oti trexei (comment)
                System.out.println("logitude = " + longitude + ", x = " + x + ", (x-1) = " + xborders.get(x - 1)
                        + ", (x) = " + xborders.get(x) + ", (x+1) = " + xborders.get(x + 1));
                System.out.println("latitude = " + latitude + ", y = " + y + ", (y-1) = " + yborders.get(y - 1)
                        + ", (y) = " + yborders.get(y) + ", (y+1) = " + yborders.get(y + 1));
            }

            count++;
        }
        System.out.println("sto 0,0 = " + binCounter.get(0));

        // final count of points in each bin
        System.out.println("----------");
        int x = 0;
        int y = 0;
        int tempBinUp = 0;
        int tempBinLeft = 0;
        int tempBinDiag = 0;
        for (int i = 0; i < binCounter.size(); i++) {
            x = i % printezis;
            y = i / printezis;
            //System.out.println(x + " - " + y);
            if ((x == 0) && (y == 0)) {
                // Do nothing
            } else if (x == 0) {
                tempBinLeft = ((y - 1) * printezis) + x;
                tempcount = binCounter.get(i) + binCounter.get(tempBinLeft);
                binCounter.set(i, tempcount);
            } else if (y == 0) {
                tempBinUp = (y * printezis) + (x - 1);
                tempcount = binCounter.get(i) + binCounter.get(tempBinUp);
                binCounter.set(i, tempcount);
            } else {
                tempBinUp = (y * printezis) + (x - 1);
                tempBinLeft = ((y - 1) * printezis) + x;
                tempBinDiag = ((y - 1) * printezis) + (x - 1);
                tempcount = binCounter.get(i) + binCounter.get(tempBinUp) + binCounter.get(tempBinLeft)
                        - binCounter.get(tempBinDiag);
                binCounter.set(i, tempcount);

            }
            //System.out.println(x + " - " + y + " - " + binCounter.get(i)) ;
        }

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
