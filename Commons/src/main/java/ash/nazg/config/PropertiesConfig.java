/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class PropertiesConfig {
    /**
     * Separator for value lists.
     */
    public static final String COMMA = ",";
    /**
     * By default, CSV RDD values are delimited by a TAB
     */
    public static final char DEFAULT_DELIMITER = '\t';

    public static final String REP_SEP = ":";
    public static final Pattern REP_VAR = Pattern.compile("\\{([^}]+?)}");

    private Properties properties = new Properties();
    private Properties overrides = new Properties();

    private String prefix = null;

    protected String getPrefix() {
        return prefix;
    }

    protected void setPrefix(String prefix) {
        if (prefix != null) {
            this.prefix = prefix + ".";
        } else {
            this.prefix = null;
        }
    }

    protected void overrideProperty(String index, String property) {
        String pIndex = replaceVars(index);
        if (pIndex != index) { // yes, String comparison via equality operator is intentional here
            properties.remove(index);
            index = pIndex;
        }

        properties.setProperty(index, replaceVars(property));
    }

    private String replaceVars(String stringWithVars) {
        Matcher hasRepVar = REP_VAR.matcher(stringWithVars);
        while (hasRepVar.find()) {
            String rep = hasRepVar.group(1);

            String repVar = rep;
            String repDef = null;
            if (rep.contains(REP_SEP)) {
                String[] rd = rep.split(REP_SEP, 2);
                repVar = rd[0];
                repDef = rd[1];
            }

            String val = overrides.getProperty(repVar, repDef);

            if (val != null) {
                stringWithVars = stringWithVars.replace("{" + rep + "}", val);
            }
        }

        return stringWithVars;
    }

    protected String getProperty(String index) {
        return properties.getProperty(index);
    }

    protected String getProperty(String index, String defaultValue) {
        return properties.getProperty(index, defaultValue);
    }

    protected String[] getArray(String key) throws InvalidConfigValueException {
        String property = getProperty(key);

        if (property == null || property.length() == 0) {
            return null;
        }

        String[] strings = Arrays.stream(property.split(COMMA)).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
        return (strings.length == 0) ? null : strings;
    }

    protected Properties getProperties() {
        return properties;
    }

    protected void setProperties(Properties source) {
        properties.clear();

        if (prefix != null) {
            final int prefixLength = prefix.length();
            source.entrySet().stream()
                    .filter(e -> e.getKey().toString().startsWith(prefix))
                    .forEach(e -> overrideProperty(e.getKey().toString().substring(prefixLength), e.getValue().toString()));
        } else {
            source.forEach((key, value) -> overrideProperty(key.toString(), value.toString()));
        }
    }

    protected Properties getOverrides() {
        return overrides;
    }

    protected void setOverrides(Properties overrides) {
        this.overrides = new Properties(overrides);
    }
}
