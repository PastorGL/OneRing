/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import ash.nazg.data.spatial.PolygonEx;
import org.apache.commons.collections4.map.SingletonMap;

import java.util.List;
import java.util.Map;

public class PolygonAccessor extends SpatialAccessor<PolygonEx> {
    public PolygonAccessor(Map<String, List<String>> properties) {
        if (properties.containsKey("value")) {
            this.properties = new SingletonMap<>("polygon", properties.get("value"));
        }
        if (properties.containsKey("polygon")) {
            this.properties = new SingletonMap<>("polygon", properties.get("polygon"));
        }
    }
}
