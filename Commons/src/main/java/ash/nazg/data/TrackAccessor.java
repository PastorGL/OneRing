/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import ash.nazg.data.spatial.SegmentedTrack;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrackAccessor extends SpatialAccessor<SegmentedTrack> {
    public TrackAccessor(Map<String, List<String>> properties) {
        this.properties = new HashMap<>();
        if (properties.containsKey("value")) {
            this.properties.put("track", properties.get("value"));
        }
        if (properties.containsKey("track")) {
            this.properties.put("track", properties.get("track"));
        }
        if (properties.containsKey("segment")) {
            this.properties.put("segment", properties.get("segment"));
        }
        if (properties.containsKey("point")) {
            this.properties.put("point", properties.get("point"));
        }
    }
}
