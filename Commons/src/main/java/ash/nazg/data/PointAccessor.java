/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import ash.nazg.data.spatial.PointEx;
import org.apache.commons.collections4.map.SingletonMap;

import java.util.List;
import java.util.Map;

public class PointAccessor extends SpatialAccessor<PointEx> {
    public PointAccessor(Map<String, List<String>> properties) {
        if (properties.containsKey("value")) {
            this.properties = new SingletonMap<>("point", properties.get("value"));
        }
        if (properties.containsKey("point")) {
            this.properties = new SingletonMap<>("point", properties.get("point"));
        }
    }
}
