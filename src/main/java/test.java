
/* SimpleApp.java */
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class test {

    public static void main(String[] args) throws IOException {

        //String logFile = "/home/harry/spark/README.md"; // Should be some file on your system
        //String logFile = "/media/harry/MyPassport/blabla"; // Should be some file on your system
        //String logFile = "/media/harry/MyPassport/all_nodes"; // Should be some file on your system
        //String logFile = "/media/harry/MyPassport/tweets-2014-06-14"; // Should be some file on your system

        int method = Integer.parseInt(args[0]);
        System.out.println(method);
        int memoryBudget = Integer.parseInt(args[1]);
        System.out.println(memoryBudget);
        String logFile = args[2];
        System.out.println(logFile);

        // SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("spark://harry-Lab:7077");
        SparkConf conf = new SparkConf().setAppName("Simple Application");

        //SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local"); //chan

        //JavaSparkContext sc = new JavaSparkContext("local[8]", "Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf); // use this

        long startTime = System.nanoTime();

        double selectivity = 0.0001;
        //int memoryBudget = 10;

        /**/
        double realSelectivity = selectivity / 100;
        int mega = 1000000;
        int x = (memoryBudget * mega) / 8;
        System.out.println(x);
        int size1 = Double.SIZE / Byte.SIZE;
        System.out.println(size1);
        int size2 = Long.SIZE / Byte.SIZE;
        System.out.println(size2);

        if (method == 1) {

            int type = 1;
            System.out.println(
                    "Let's go for: method = Sampling - memoryBudget = " + memoryBudget + " - file = " + logFile);
            mySamplingTest.sampleSpark(sc, memoryBudget, logFile, type, realSelectivity);

        } else if (method == 2) {

            int type = 1;
            System.out.println(
                    "Let's go for: method = Partial Binning - memoryBudget = " + memoryBudget + " - file = " + logFile);
            partialBinningTest.binning(sc, memoryBudget, logFile, type, realSelectivity);

        } else if (method == 3) {

            int type = 1;
            System.out.println("Let's go for: method = NonUniform Binning - memoryBudget = " + memoryBudget
                    + " - file = " + logFile);
            nonUniformBinningTest.binning(sc, memoryBudget, logFile, type, realSelectivity);

        } else {
            System.out.println("--------------> What?");
        }

        /*
        // S A M P L I N G
        
        //String filePath = new String("F:\\all_nodes");
        //int type = 0;
        //String filePath = new String("F:\\tweets-2014-06-14");
        int type = 1;
        mySamplingTest.sampleSpark(sc, memoryBudget, logFile, type, realSelectivity);
        //sampling(memoryBudget, logFile, type, realSelectivity, pointNum);
        */

        /*
        // B I N N I N G
        
        //String filePath = new String("F:\\all_nodes") ;
        //int type = 0;
        //String filePath = new String("E:\\tweets-2014-06-14") ;
        int type = 1;
        //myBinning bin = new myBinning();
        //partialBinning bin = new partialBinning();
        //nonUniformBinning bin = new nonUniformBinning() ;
        //bin.binning(memoryBudget, filePath, type);
        //bin.binning(memoryBudget, filePath, type, realSelectivity, pointNum);
        //partialBinningTest.binning(sc, memoryBudget, logFile, type, realSelectivity);
        nonUniformBinningTest.binning(sc, memoryBudget, logFile, type, realSelectivity);
        */

        long endTime = System.nanoTime();
        long duration = (endTime - startTime); //divide by 1000000 to get milliseconds
        System.out.println("Execution time general: " + duration);
        System.out.println("END");

        sc.stop();
    }

}
