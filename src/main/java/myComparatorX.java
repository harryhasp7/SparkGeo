import java.io.Serializable;
import java.util.Comparator;

public class myComparatorX implements Comparator<myPoint2>, Serializable {

    public int compare(myPoint2 a, myPoint2 b) {
        return a.longitude < b.longitude ? -1 : (a.longitude > b.longitude ? 1 : 0);
    }

}
