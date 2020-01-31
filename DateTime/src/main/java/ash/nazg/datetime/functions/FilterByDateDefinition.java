/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.functions;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;

public class FilterByDateDefinition implements Serializable, Cloneable {
    public char inputDelimiter;

    public Integer dateCol;
    public Integer yearCol;
    public Integer dowCol;
    public Integer monthCol;
    public Integer hourCol;
    public Integer minuteCol;

    public Date start;
    public Date end;
    
    public Integer[] dates;
    public Integer[] years;
    public Integer[] dows;
    public Integer[] months;
    public Integer[] hours;
    public Integer[] minutes;

    public Integer startHHMM;
    public Integer endHHMM;

    @Override
    public Object clone() throws CloneNotSupportedException {
        FilterByDateDefinition clone = (FilterByDateDefinition) super.clone();

        clone.dateCol = dateCol;
        clone.yearCol = yearCol;
        clone.dowCol = dowCol;
        clone.monthCol = monthCol;
        clone.hourCol = hourCol;
        clone.minuteCol = minuteCol;

        clone.inputDelimiter = inputDelimiter;

        clone.start = (start == null) ? null : (Date) start.clone();
        clone.end = (end == null) ? null : (Date) end.clone();

        clone.dates = (dates == null) ? null : dates.clone();
        clone.years = (years == null) ? null : years.clone();
        clone.dows = (dows == null) ? null : dows.clone();
        clone.months = (months == null) ? null : months.clone();
        clone.hours = (hours == null) ? null : hours.clone();
        clone.minutes = (minutes == null) ? null : minutes.clone();

        clone.startHHMM = startHHMM;
        clone.endHHMM = endHHMM;

        return clone;
    }

    public static Date parseDate(String timestampText) {
        try {
            // timestamp is in milliseconds
            long timestamp = new Double(timestampText).longValue();

            // timestamp is in seconds
            if (timestamp < 100_000_000_000L) {
                timestamp *= 1000L;
            }

            return new Date(timestamp);
        } catch (NumberFormatException e) {
            // timestamp is ISO
            return Date.from(Instant.parse(timestampText));
        } // fail otherwise
    }
}
