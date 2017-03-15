import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by sir7o on 2/8/2017.
 */

public class myMedian {

    public static void findMedian(ArrayList<myPoint2> points, int start, int end, int k, int direction) {
        //int start = 0 ;
        //int end = points.size()-1 ;
        //int k = (points.size()/2) + 10 ;
        int median = -1;

        while (true) {
            //System.out.print("[ ");
            //for (int i = 0; i < points.size(); i++) {
            //    System.out.print(points.get(i) + ", ");
            //}
            //System.out.println("]");
            int ranRange = end - start;
            //System.out.println("Ran between : [" + start + " , " + (start + (end-start+1)) + ")");
            long ran = start + ThreadLocalRandom.current().nextLong(ranRange + 1);
            int intRan = (int) ran;
            //System.out.println("ran = " + intRan + " -- " + points.get(intRan)) ;
            swap(points, intRan, end);
            //System.out.println("New ran = " + intRan + " -- " + points.get(intRan)) ;

            myPoint2 pivot = points.get(end);
            if (direction == 0) {
                median = partitionX(points, start, end, pivot);
            } else if (direction == 1) {
                median = partitionY(points, start, end, pivot);
            } else {
                System.out.println("Wrong direction");
            }
            //median = partitionIt(points, start, end, pivot) ;
            //System.out.println("median = " + median) ;
            if (median == k) {
                //System.out.println("found it") ;
                //System.out.println("--> " + points.get(median).longitude) ;
                break;
            } else if (median > k) {
                end = median - 1;
                //System.out.println("end = " + end) ;
            } else if (median < k) {
                start = median + 1;
                //System.out.println("start = " + start) ;
            } else {
                System.out.println("WHAT????");
            }
        }

    }

    private static int partitionX(ArrayList<myPoint2> points, int left, int right, myPoint2 pivot) {
        int leftPtr = left - 1;
        int rightPtr = right;
        while (true) {
            //while (arr[++leftPtr] < pivot) ;
            while (points.get(++leftPtr).longitude < pivot.longitude);

            //while (rightPtr > 0 && arr[--rightPtr] > pivot) ;
            while (rightPtr > 0 && points.get(--rightPtr).longitude > pivot.longitude);

            if (leftPtr >= rightPtr)
                break;
            else
                swap(points, leftPtr, rightPtr);
        }
        swap(points, leftPtr, right);
        return leftPtr;
    }

    private static int partitionY(ArrayList<myPoint2> points, int left, int right, myPoint2 pivot) {
        int leftPtr = left - 1;
        int rightPtr = right;
        while (true) {
            //while (arr[++leftPtr] < pivot) ;
            while (points.get(++leftPtr).latitude < pivot.latitude);

            //while (rightPtr > 0 && arr[--rightPtr] > pivot) ;
            while (rightPtr > 0 && points.get(--rightPtr).latitude > pivot.latitude);

            if (leftPtr >= rightPtr)
                break;
            else
                swap(points, leftPtr, rightPtr);
        }
        swap(points, leftPtr, right);
        return leftPtr;
    }

    private static void swap(ArrayList<myPoint2> points, int dex1, int dex2) {
        myPoint2 temp = points.get(dex1);
        points.set(dex1, points.get(dex2));
        points.set(dex2, temp);
    }

}
