/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import ash.nazg.data.spatial.SpatialRecord;
import org.locationtech.jts.geom.Geometry;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SpatialAccessor<G extends Geometry & SpatialRecord<G>> implements Accessor<G> {
    protected Map<String, List<String>> properties = new HashMap<>();

    public List<String> attributes(String category) {
        if ("value".equals(category)) {
            if (properties.containsKey("polygon")) {
                return properties.get("polygon");
            }
            if (properties.containsKey("track")) {
                return properties.get("track");
            }
            if (properties.containsKey("point")) {
                return properties.get("point");
            }
        }
        return properties.getOrDefault(category, Collections.EMPTY_LIST);
    }

    @Override
    public Map<String, List<String>> attributes() {
        return properties;
    }

    @Override
    public void set(G obj, String attr, Object value) {
        obj.put(attr, value);
    }

    @Override
    public AttrGetter getter(G obj) {
        Map<String, Object> props = (Map<String, Object>) obj.getUserData();
        if (props.isEmpty()) {
            return (p) -> null;
        }
        return (p) -> obj.asIs(p);
    }
}
