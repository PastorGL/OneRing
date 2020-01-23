package ash.nazg.spatial.functions;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

public class PointCSVMapper implements FlatMapFunction<Iterator<Object>, Point> {
    private final int latCol;
    private final int lonCol;
    private final Integer radiusColumn;

    private final char delimiter;
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final Map<String, Integer> outputColumns;
    private Double defaultRadius;

    public PointCSVMapper(Map<String, Integer> columns, int latCol, int lonCol, Integer radiusColumn, Double defaultRadius, char delimiter, List<String> outputColumns) {
        this.outputColumns = columns.entrySet().stream()
                .filter(c -> (outputColumns.size() == 0) || outputColumns.contains(c.getKey()))
                .collect(Collectors.toMap(c -> c.getKey().replaceFirst("^[^.]+\\.", ""), Map.Entry::getValue));
        this.latCol = latCol;
        this.lonCol = lonCol;
        this.radiusColumn = radiusColumn;
        this.defaultRadius = defaultRadius;
        this.delimiter = delimiter;
    }

    @Override
    public Iterator<Point> call(Iterator<Object> it) throws Exception {
        CSVParser parser = new CSVParserBuilder().withSeparator(delimiter).build();

        List<Point> result = new ArrayList<>();

        Text latAttr = new Text(GEN_CENTER_LAT);
        Text lonAttr = new Text(GEN_CENTER_LON);
        Text radiusAttr = new Text(GEN_RADIUS);

        while (it.hasNext()) {
            Object line = it.next();
            String l = line instanceof String ? (String) line : String.valueOf(line);

            String[] row = parser.parseLine(l);

            double lat = new Double(row[latCol]);
            double lon = new Double(row[lonCol]);

            MapWritable properties = new MapWritable();

            for (Map.Entry<String, Integer> col : outputColumns.entrySet()) {
                properties.put(new Text(col.getKey()), new Text(row[col.getValue()]));
            }

            Double radius = defaultRadius;
            if (radiusColumn != null) {
                radius = new Double(row[radiusColumn]);
            }

            Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
            point.setUserData(properties);
            properties.put(latAttr, new DoubleWritable(lat));
            properties.put(lonAttr, new DoubleWritable(lon));
            if (radius != null) {
                properties.put(radiusAttr, new DoubleWritable(radius));
            }

            result.add(point);
        }

        return result.iterator();
    }
}
