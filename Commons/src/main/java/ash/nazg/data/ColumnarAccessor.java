/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.collections4.map.SingletonMap;

import java.util.List;
import java.util.Map;

public class ColumnarAccessor implements Accessor<Columnar> {
    final ListOrderedMap<String, Integer> columns;

    public ColumnarAccessor(Map<String, List<String>> columns) {
        this.columns = new ListOrderedMap<>();
        int[] n = {0};
        columns.get("value").forEach(e -> this.columns.put(e, n[0]++));
    }

    public List<String> attributes(String category) {
        return columns.keyList();
    }

    @Override
    public Map<String, List<String>> attributes() {
        return new SingletonMap<>("value", columns.keyList());
    }

    @Override
    public void set(Columnar rec, String column, Object value) {
        if (columns.containsKey(column)) {
            rec.put(column, value);
        }
    }

    @Override
    public AttrGetter getter(Columnar rec) {
        return (column) -> rec.asIs(column);
    }
}
