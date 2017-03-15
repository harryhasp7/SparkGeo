import java.util.ArrayList;

/**
 * Created by sir7o on 1/15/2017.
 */

public class myLeaf {

    int id;
    ArrayList<myPoint2> leafList = new ArrayList<myPoint2>();

    myLeaf(int id, ArrayList<myPoint2> points) {
        this.id = id;
        this.leafList = points;
    }

}
