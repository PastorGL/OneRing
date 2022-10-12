/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.scripting;

import java.io.Serializable;
import java.util.List;

public class QueryItem implements Serializable {
    public final List<Expression<?>> expression;
    public final String category;

    public QueryItem(List<Expression<?>> expression, String category) {
        this.expression = expression;
        this.category = (category == null) ? "value" : category;
    }

    public QueryItem() {
        expression = null;
        category = null;
    }
}
