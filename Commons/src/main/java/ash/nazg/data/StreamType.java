/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.data.spatial.PointEx;
import ash.nazg.data.spatial.PolygonEx;
import ash.nazg.data.spatial.SegmentedTrack;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Map;

public enum StreamType {
    PlainText {
        @Override
        public Accessor<Text> accessor(Map<String, List<String>> ignored) {
            return new PlainTextAccessor();
        }
    },
    Columnar {
        @Override
        public Accessor<ash.nazg.data.Columnar> accessor(Map<String, List<String>> columnNames) {
            return new ColumnarAccessor(columnNames);
        }
    },
    KeyValue {
        @Override
        public Accessor<ash.nazg.data.Columnar> accessor(Map<String, List<String>> columnNames) {
            return new ColumnarAccessor(columnNames);
        }
    },
    Point {
        @Override
        public Accessor<PointEx> accessor(Map<String, List<String>> propNames) {
            return new PointAccessor(propNames);
        }
    },
    Track {
        @Override
        public Accessor<SegmentedTrack> accessor(Map<String, List<String>> propNames) {
            return new TrackAccessor(propNames);
        }
    },
    Polygon {
        @Override
        public Accessor<PolygonEx> accessor(Map<String, List<String>> propNames) {
            return new PolygonAccessor(propNames);
        }
    },
    Passthru {
        @Override
        public Accessor<?> accessor(Map<String, List<String>> propNames) {
            throw new InvalidConfigurationException("Attribute accessor of Passthru type DataStream must never be called");
        }
    };

    public abstract Accessor<?> accessor(Map<String, List<String>> propNames);
}
