/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.functions;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;

public class FilterByDateFunction implements FlatMapFunction<Iterator<Object>, Text> {
    private FilterByDateDefinition filterByDateDefinition;

    public FilterByDateFunction(FilterByDateDefinition filterByDateDefinition) {
        this.filterByDateDefinition = filterByDateDefinition;
    }

    @Override
    public Iterator<Text> call(Iterator<Object> it) throws Exception {
        List<Text> ret = new ArrayList<>();

        CSVParser parser = new CSVParserBuilder().withSeparator(filterByDateDefinition.inputDelimiter).build();

        while (it.hasNext()) {
            Object v = it.next();
            String l = v instanceof String ? (String) v : String.valueOf(v);

            boolean matches = true;

            String[] ll = parser.parseLine(l);

            if ((filterByDateDefinition.start != null) || (filterByDateDefinition.end != null)) {
                Calendar cc = Calendar.getInstance();

                if (filterByDateDefinition.yearCol != null) {
                    cc.set(Calendar.YEAR, new Integer(ll[filterByDateDefinition.yearCol]));
                }
                if (filterByDateDefinition.monthCol != null) {
                    cc.set(Calendar.MONTH, new Integer(ll[filterByDateDefinition.monthCol]));
                }
                if (filterByDateDefinition.dateCol != null) {
                    cc.set(Calendar.DATE, new Integer(ll[filterByDateDefinition.dateCol]));
                }
                if (filterByDateDefinition.hourCol != null) {
                    cc.set(Calendar.HOUR, new Integer(ll[filterByDateDefinition.hourCol]));
                }
                if (filterByDateDefinition.minuteCol != null) {
                    cc.set(Calendar.MINUTE, new Integer(ll[filterByDateDefinition.minuteCol]));
                }
                cc.set(Calendar.SECOND, 0);

                if (filterByDateDefinition.start != null) {
                    matches = matches && cc.getTime().after(filterByDateDefinition.start);
                }
                if (filterByDateDefinition.end != null) {
                    matches = matches && cc.getTime().before(filterByDateDefinition.end);
                }
            }

            if ((filterByDateDefinition.dates != null) && (filterByDateDefinition.dateCol != null)) {
                matches = matches && Arrays.asList(filterByDateDefinition.dates).contains(new Integer(ll[filterByDateDefinition.dateCol]));
            }
            if ((filterByDateDefinition.years != null) && (filterByDateDefinition.yearCol != null)) {
                matches = matches && Arrays.asList(filterByDateDefinition.years).contains(new Integer(ll[filterByDateDefinition.yearCol]));
            }
            if ((filterByDateDefinition.dows != null) && (filterByDateDefinition.dowCol != null)) {
                matches = matches && Arrays.asList(filterByDateDefinition.dows).contains(new Integer(ll[filterByDateDefinition.dowCol]));
            }
            if ((filterByDateDefinition.months != null) && (filterByDateDefinition.monthCol != null)) {
                matches = matches && Arrays.asList(filterByDateDefinition.months).contains(new Integer(ll[filterByDateDefinition.monthCol]));
            }
            if ((filterByDateDefinition.hours != null) && (filterByDateDefinition.hourCol != null)) {
                matches = matches && Arrays.asList(filterByDateDefinition.hours).contains(new Integer(ll[filterByDateDefinition.hourCol]));
            }
            if ((filterByDateDefinition.minutes != null) && (filterByDateDefinition.minuteCol != null)) {
                matches = matches && Arrays.asList(filterByDateDefinition.minutes).contains(new Integer(ll[filterByDateDefinition.minuteCol]));
            }

            if (((filterByDateDefinition.startHHMM != null) || (filterByDateDefinition.endHHMM != null)) && (filterByDateDefinition.hourCol != null) && (filterByDateDefinition.minuteCol != null)) {
                int hhmm = new Integer(ll[filterByDateDefinition.hourCol]) * 100 + new Integer(ll[filterByDateDefinition.minuteCol]);

                if ((filterByDateDefinition.startHHMM != null) && (filterByDateDefinition.endHHMM != null)) {
                    if (filterByDateDefinition.startHHMM < filterByDateDefinition.endHHMM) {
                        matches = matches && (filterByDateDefinition.startHHMM < hhmm) && (hhmm <= filterByDateDefinition.endHHMM);
                    } else {
                        matches = matches && ((filterByDateDefinition.startHHMM < hhmm) || (hhmm <= filterByDateDefinition.endHHMM));
                    }
                }
                if ((filterByDateDefinition.startHHMM != null) && (filterByDateDefinition.endHHMM == null)) {
                    matches = matches && (filterByDateDefinition.startHHMM < hhmm);
                }
                if ((filterByDateDefinition.startHHMM == null) && (filterByDateDefinition.endHHMM != null)) {
                    matches = matches && (hhmm <= filterByDateDefinition.endHHMM);
                }
            }

            if (matches) {
                ret.add(new Text(l));
            }
        }

        return ret.iterator();
    }
}
