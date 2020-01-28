package ash.nazg.spatial;

import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class SpatialUtils implements Serializable {
    private int recursion = 1;
    private int resolution = 15;

    private static H3Core h3 = null;

    private double radius = Double.NEGATIVE_INFINITY;

    private void setupH3(double radius) {
        try {
            if (h3 == null) {
                h3 = H3Core.newInstance();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (this.radius != radius) {
            for (int i = 15; ; i--) {
                double length = h3.edgeLength(i, LengthUnit.m);
                if (length > radius) {
                    recursion = (int) Math.floor(length / h3.edgeLength(i + 1, LengthUnit.m));
                    resolution = i + 1;
                    this.radius = radius;
                    return;
                }
            }
        }
    }

    public List<Long> getNeighbours(long h3index, double radius) {
        setupH3(radius);

        return h3.kRing(h3index, recursion);
    }

    public long getHash(double lat, double lon, double radius) {
        setupH3(radius);

        return h3.geoToH3(lat, lon, resolution);
    }
}
