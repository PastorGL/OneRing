/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import java.util.Arrays;
import java.util.Map;
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
    public static final String DS_OUTPUT_DELIMITER = "ds.output.delimiter";
    public static final String DS_INPUT_DELIMITER = "ds.input.delimiter";

    public static final String REP_SEP = ":";
    public static final Pattern REP_VAR = Pattern.compile("\\{([^}]+?)}");

    private final Properties properties = new Properties();
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

    public void overrideProperty(String index, String property) {
        overrideProperty(index, property, false);
    }

    public void overrideProperty(String index, String property, final boolean replace) {
        String pIndex = replaceVars(index);
        if (pIndex != index) { // yes, String comparison via equality operator is intentional here
            properties.remove(index);
            index = pIndex;
        }

        properties.setProperty(index, replace ? replaceVars(property) : property);
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

    protected Properties getLayerProperties(String layerPrefix) {
        return getLayerProperties(layerPrefix, true);
    }

    protected Properties getLayerProperties(String layerPrefix, boolean replace) {
        Properties layerProperties = new Properties();

        for (Map.Entry<Object, Object> e : properties.entrySet()) {
            String prop = (String) e.getKey();
            if (prop.startsWith(layerPrefix)) {
                String val = (String) e.getValue();

                layerProperties.setProperty(prop, replace ? replaceVars(val) : val);
            }
        }

        return layerProperties;
    }

    public void setProperties(Properties source) {
        setProperties(source, false);
    }

    public void setProperties(Properties source, final boolean replace) {
        properties.clear();

        if (prefix != null) {
            final int prefixLength = prefix.length();
            source.entrySet().stream()
                    .filter(e -> e.getKey().toString().startsWith(prefix))
                    .forEach(e -> overrideProperty(e.getKey().toString().substring(prefixLength), e.getValue().toString(), replace));
        } else {
            source.forEach((key, value) -> overrideProperty(key.toString(), value.toString(), replace));
        }
    }

    protected Properties getOverrides() {
        return overrides;
    }

    public void setOverrides(Properties overrides) {
        this.overrides = new Properties(overrides);
    }

    protected char getDsOutputDelimiter() {
        String delimiter = getProperty(DS_OUTPUT_DELIMITER);

        if ((delimiter == null) || delimiter.isEmpty()) {
            return DEFAULT_DELIMITER;
        }

        return delimiter.charAt(0);
    }

    protected char getDsInputDelimiter() {
        String delimiter = getProperty(DS_INPUT_DELIMITER);

        if ((delimiter == null) || delimiter.isEmpty()) {
            return DEFAULT_DELIMITER;
        }

        return delimiter.charAt(0);
    }
}
