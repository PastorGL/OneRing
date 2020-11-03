package ash.nazg.spatial;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.operation.BoundaryOp;

import java.util.Arrays;
import java.util.Iterator;

public class SegmentedTrack extends GeometryCollection implements Lineal, Iterable<Geometry> {
    public SegmentedTrack(Geometry[] geometries, GeometryFactory factory) {
        super(geometries, factory);
    }

    public SegmentedTrack(TrackSegment[] geometries, GeometryFactory factory) {
        super(geometries, factory);
    }

    public int getDimension() {
        return 1;
    }

    public int getBoundaryDimension() {
        return 0;
    }

    public String getGeometryType() {
        return "SegmentedTrack";
    }

    public Geometry getBoundary() {
        return (new BoundaryOp(this)).getBoundary();
    }

    public Geometry reverse() {
        int nLines = geometries.length;
        TrackSegment[] revSegments = new TrackSegment[nLines];
        for (int i = 0; i < geometries.length; i++) {
            revSegments[nLines - 1 - i] = (TrackSegment) geometries[i].reverse();
        }
        return new SegmentedTrack(revSegments, getFactory());
    }

    protected SegmentedTrack copyInternal() {
        TrackSegment[] segments = new TrackSegment[this.geometries.length];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = (TrackSegment) this.geometries[i].copy();
        }
        return new SegmentedTrack(segments, factory);
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
