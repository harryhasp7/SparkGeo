import java.util.ArrayList;

public class quickSort {

    public static int partitionX(ArrayList<myPoint2> sample, int left, int right) {
        int i = left;
        int j = right;
        myPoint2 tmp;
        //int pivot = arr[(left + right) / 2];
        myPoint2 pivotPoint = sample.get((left + right) / 2);
        double pivot = pivotPoint.longitude;

        while (i <= j) {
            while ((sample.get(i)).longitude < pivot)
                i++;
            while ((sample.get(j)).longitude > pivot)
                j--;
            if (i <= j) {
                //tmp = arr[i];
                tmp = sample.get(i);
                //arr[i] = arr[j];
                sample.set(i, sample.get(j));
                //arr[j] = tmp;
                sample.set(j, tmp);
                i++;
                j--;
            }
        } ;

        return i;
    }

    public static int partitionY(ArrayList<myPoint2> sample, int left, int right) {
        int i = left;
        int j = right;
        myPoint2 tmp;
        //int pivot = arr[(left + right) / 2];
        myPoint2 pivotPoint = sample.get((left + right) / 2);
        double pivot = pivotPoint.latitude;

        while (i <= j) {
            while ((sample.get(i)).latitude < pivot)
                i++;
            while ((sample.get(j)).latitude > pivot)
                j--;
            if (i <= j) {
                //tmp = arr[i];
                tmp = sample.get(i);
                //arr[i] = arr[j];
                sample.set(i, sample.get(j));
                //arr[j] = tmp;
                sample.set(j, tmp);
                i++;
                j--;
            }
        } ;

        return i;
    }

    public static void quickSort(ArrayList<myPoint2> sample, int left, int right, int dimension) {
        int index = 0;
        if (dimension == 0) {
            index = partitionX(sample, left, right);
        } else if (dimension == 1) {
            index = partitionY(sample, left, right);
        } else {
            System.out.println("Wrong dimension");
            return;
        }
        //int index = partition(sample, left, right);
        if (left < index - 1)
            quickSort(sample, left, index - 1, dimension);
        if (index < right)
            quickSort(sample, index, right, dimension);
    }

}
