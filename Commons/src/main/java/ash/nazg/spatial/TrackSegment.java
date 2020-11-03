package ash.nazg.spatial;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.operation.BoundaryOp;

import java.util.Arrays;
import java.util.Iterator;

public class TrackSegment extends GeometryCollection implements Lineal, Iterable<Geometry> {
    public TrackSegment(Point[] geometries, GeometryFactory factory) {
        super(geometries, factory);
    }

    public TrackSegment(Geometry[] geometries, GeometryFactory factory) {
        super(geometries, factory);
    }

    public int getDimension() {
        return 1;
    }

    public int getBoundaryDimension() {
        return 0;
    }

    public String getGeometryType() {
        return "TrackSegment";
    }

    public Geometry getBoundary() {
        return (new BoundaryOp(this)).getBoundary();
    }

    public Geometry reverse() {
        int nLines = geometries.length;
        Point[] revPoints = new Point[nLines];
        for (int i = 0; i < geometries.length; i++) {
            revPoints[nLines - 1 - i] = (Point) geometries[i].reverse();
        }
        return new TrackSegment(revPoints, getFactory());
    }

    protected TrackSegment copyInternal() {
        Point[] point = new Point[this.geometries.length];
        for (int i = 0; i < point.length; i++) {
            point[i] = (Point) this.geometries[i].copy();
        }
        return new TrackSegment(point, factory);
    }

    public boolean equalsExact(Geometry other, double tolerance) {
        if (!isEquivalentClass(other)) {
            return false;
        }
        return super.equalsExact(other, tolerance);
    }

    @Override
    public Iterator<Geometry> iterator() {
        return Arrays.stream(geometries).iterator();
    }

    public Geometry[] geometries() {
        return geometries;
    }
}
