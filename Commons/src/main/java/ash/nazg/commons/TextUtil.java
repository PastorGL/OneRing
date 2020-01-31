/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons;

import ash.nazg.config.PropertiesConfig;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility to manipulate with CSV/TSV strings stored to {@link org.apache.hadoop.io.Text} at lowest level
 * avoiding conversions to String as much as possible. Does not handle escaping and quoting.
 */
@Deprecated
public class TextUtil {
    /**
     * Uses {@link PropertiesConfig#DEFAULT_DELIMITER} as default delimiter
     */
    public static final Text DELIMITER_TEXT = new Text("" + PropertiesConfig.DEFAULT_DELIMITER);

    /**
     * Get n-th column
     *
     * @param in           input Text
     * @param columnNumber n
     * @return column value, or null if n is out of bounds
     */
    public static Text column(Text in, int columnNumber) {
        return (Text) columnOps(in, null, columnNumber, 1);
    }

    /**
     * Get n-th column using custom delimiter
     *
     * @param delimiter custom delimiter
     */
    public static Text column(Text in, String delimiter, int columnNumber) {
        return (Text) columnOps(in, delimiter, columnNumber, 1);
    }

    /**
     * Get all columns starting with n-th to the end of input
     */
    public static Text columns(Text in, int startColumn) {
        return (Text) columnOps(in, null, startColumn, -1);
    }

    /**
     * Get all columns starting with n-th to the end of input using custom delimiter
     */
    public static Text columns(Text in, String delimiter, int startColumn) {
        return (Text) columnOps(in, delimiter, startColumn, -1);
    }

    /**
     * Get a number of columns starting with n-th to the end of input
     *
     * @param numberOfColumns number of columns
     */
    public static Text columns(Text in, int startColumn, int numberOfColumns) {
        return (Text) columnOps(in, null, startColumn, numberOfColumns);
    }

    /**
     * Get a number of columns starting with n-th to the end of input using custom delimiter
     */
    public static Text columns(Text in, String delimiter, int startColumn, int numberOfColumns) {
        return (Text) columnOps(in, delimiter, startColumn, numberOfColumns);
    }

    /**
     * Count a number of columns in the input
     *
     * @return number of columns
     */
    public static int columnCount(Text in) {
        return (Integer) columnOps(in, null, -1, -1);
    }

    /**
     * Count a number of columns in the input using custom delimiter
     */
    public static int columnCount(Text in, String delimiter) {
        return (Integer) columnOps(in, delimiter, -1, -1);
    }

    /**
     * Append stringified value to the end of input Text. {@link #append(Text, Text, Text)},
     * {@link #append(Text, Object)} and {@link #append(Text, Text)} are synonyms.
     *
     * @param to        if null, will be created a new empty Text instance
     * @param delimiter if null, {@link #DELIMITER_TEXT} will be used
     * @param o         safe to be null
     * @return source Text, modified
     */
    public static Text append(Text to, Text delimiter, Object o) {
        return appendOps(to, delimiter, new Text(String.valueOf(o)));
    }

    public static Text append(Text to, Text delimiter, Text text) {
        return appendOps(to, delimiter, text);
    }

    public static Text append(Text to, Text text) {
        return appendOps(to, null, text);
    }

    public static Text append(Text to, Object o) {
        return appendOps(to, null, new Text(String.valueOf(o)));
    }

    private static Text appendOps(Text to, Text delimiter, Text text) {
        if (to == null) {
            to = new Text(text);
            return to;
        }

        if (delimiter == null) {
            delimiter = DELIMITER_TEXT;
        }

        to.append(delimiter.getBytes(), 0, delimiter.getLength());
        to.append(text.getBytes(), 0, text.getLength());

        return to;
    }

    private static Object columnOps(Text in, String delimiter, int startColumn, int numberOfColumns) {
        if (delimiter == null) {
            delimiter = "" + PropertiesConfig.DEFAULT_DELIMITER;
        }

        List<Integer> columnPositions = new ArrayList<>();
        columnPositions.add(0);
        int pos = -1;
        do {
            pos = in.find(delimiter, pos + 1);
            if (pos >= 0) {
                columnPositions.add(pos + 1);
            }
        } while (pos >= 0);

        if (startColumn < 0) {
            return columnPositions.size();
        }

        if (startColumn >= columnPositions.size()) {
            return null;
        }

        int startPosition = (startColumn > 0) ? columnPositions.get(startColumn) : 0;
        int len = ((numberOfColumns > 0) && (numberOfColumns < (columnPositions.size() - startColumn)))
                ? columnPositions.get(startColumn + numberOfColumns) - startPosition - 1
                : in.getLength() - startPosition;

        Text ret = new Text();
        ret.set(in.getBytes(), startPosition, len);
        return ret;
    }
}
