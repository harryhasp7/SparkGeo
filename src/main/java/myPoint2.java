import java.io.Serializable;

public class myPoint2 implements Serializable {

    double longitude;
    double latitude;

    myPoint2(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

}
