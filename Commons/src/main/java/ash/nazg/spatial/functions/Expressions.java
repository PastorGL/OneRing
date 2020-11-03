package ash.nazg.spatial.functions;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public final class Expressions {
    public static StringExpr stringRegex(int regexFlags, final String rv) {
        final Pattern p = Pattern.compile(rv, regexFlags);

        return (r) -> (r != null) && p.matcher(r).matches();
    }

    public static NumericExpr numericGreater(final Double rv) {
        return (r) -> (r != null) && (r > rv);
    }

    public static NumericExpr numericGreaterEq(final Double rv) {
        return (r) -> (r != null) && (r >= rv);
    }

    public static NumericExpr numericLess(final Double rv) {
        return (r) -> (r != null) && (r < rv);
    }

    public static NumericExpr numericLessEq(final Double rv) {
        return (r) -> (r != null) && (r >= rv);
    }

    public static NumericExpr numericEqual(final Double rv) {
        return (r) -> (r != null) && (r.doubleValue() == rv);
    }

    public static NumericExpr numericUnequal(final Double rv) {
        return (r) -> (r != null) && (r.doubleValue() != rv);
    }

    public static StringExpr stringEqual(final String rv) {
        return (r) -> (r != null) && r.equals(rv);
    }

    public static StringExpr stringUnequal(final String rv) {
        return (r) -> (r != null) && !r.equals(rv);
    }

    public static NullExpr isNull() {
        return Objects::isNull;
    }

    public static NullExpr isNotNull() {
        return Objects::nonNull;
    }

    public static LogicUnaryExpr not() {
        return (r) -> !r;
    }

    public static LogicBinaryExpr and() {
        return (a, b) -> a && b;
    }

    public static LogicBinaryExpr or() {
        return (a, b) -> a || b;
    }

    public static PropGetter propGetter(final String prop) {
        return (r) -> r.get(new Text(prop));
    }

    public static StackGetter stackGetter(final int num) {
        return (r) -> r.subList(r.size() - num, r.size());
    }

    public interface QueryExpr extends Serializable {
    }

    @FunctionalInterface
    public interface LogicBinaryExpr extends QueryExpr {
        boolean eval(boolean a, boolean b);
    }

    @FunctionalInterface
    public interface LogicUnaryExpr extends QueryExpr {
        boolean eval(boolean rv);
    }

    @FunctionalInterface
    public interface NumericExpr extends QueryExpr {
        boolean eval(Double rv);
    }

    @FunctionalInterface
    public interface PropGetter extends QueryExpr {
        Object get(MapWritable obj);
    }

    @FunctionalInterface
    public interface StackGetter extends QueryExpr {
        List<Boolean> eval(List<Boolean> r);
    }

    @FunctionalInterface
    public interface StringExpr extends QueryExpr {
        boolean eval(String rv);
    }

    @FunctionalInterface
    public interface NullExpr extends QueryExpr {
        boolean eval(Object rv);
    }
}
