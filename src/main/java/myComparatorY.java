import java.io.Serializable;
import java.util.Comparator;

public class myComparatorY implements Comparator<myPoint2>, Serializable {

    public int compare(myPoint2 a, myPoint2 b) {
        return a.latitude < b.latitude ? -1 : (a.latitude > b.latitude ? 1 : 0);
    }

}
