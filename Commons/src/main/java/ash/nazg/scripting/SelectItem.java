/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.scripting;

import java.io.Serializable;
import java.util.List;

public class SelectItem implements Serializable {
    public final List<Expression<?>> expression;
    public final String alias;
    public final String category;

    public SelectItem(List<Expression<?>> expression, String alias, String category) {
        this.expression = expression;
        this.alias = alias;
        this.category = (category == null) ? "value" : category;
    }
}
