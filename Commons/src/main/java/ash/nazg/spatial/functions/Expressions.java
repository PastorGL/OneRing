package ash.nazg.spatial.functions;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;
import java.util.regex.Pattern;

public final class Expressions {
    public static StringExpr stringRegex(int regexFlags, final String rv) {
        final Pattern p = Pattern.compile(rv, regexFlags);

        return (r) -> (r != null) && p.matcher(r).matches();
    }

    public static NumericExpr numericGreater(final double rv) {
        return (r) -> (r != null) && (r > rv);
    }

    public static NumericExpr numericGreaterEq(final double rv) {
        return (r) -> (r != null) && (r >= rv);
    }

    public static NumericExpr numericLess(final double rv) {
        return (r) -> (r != null) && (r < rv);
    }

    public static NumericExpr numericLessEq(final double rv) {
        return (r) -> (r != null) && (r >= rv);
    }

    public static NumericExpr numericEqual(final double rv) {
        return (r) -> (r != null) && (r == rv);
    }

    public static NumericExpr numericUnequal(final double rv) {
        return (r) -> (r != null) && (r != rv);
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

    public static PropGetter getString(final String prop) {
        return (r) -> {
            Writable raw = r.get(new Text(prop));
            return (raw != null) ? raw.toString() : null;
        };
    }

    public static PropGetter getNumber(final String prop) {
        return (r) -> {
            Writable raw = r.get(new Text(prop));
            return (raw != null) ? new Double(raw.toString()) : null;
        };
    }

    public static StackGetter stackGetter(final int num) {
        return (stack) -> {
            Deque<Boolean> top = new LinkedList<>();
            for (int i = 0; i < num; i++) {
                top.push(stack.pop());
            }
            return top;
        };
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
        Deque<Boolean> eval(Deque<Boolean> r);
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
