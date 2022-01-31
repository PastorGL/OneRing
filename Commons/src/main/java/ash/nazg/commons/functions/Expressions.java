/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.functions;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;

public final class Expressions {
    public static IsExpr isNull() {
        return Objects::isNull;
    }

    public static IsExpr nonNull() {
        return Objects::nonNull;
    }

    public static InExpr in(Set<String> h) {
        return h::contains;
    }

    public static InExpr inNum(Set<Double> h) {
        return (n) -> {
            double t = (n instanceof Number) ? ((Number) n).doubleValue() : Double.parseDouble(String.valueOf(n));
            return h.contains(t);
        };
    }

    public static InExpr notIn(Set<String> h) {
        return (n) -> !h.contains(n);
    }

    public static InExpr notInNum(Set<Double> h) {
        return (n) -> {
            double t = (n instanceof Number) ? ((Number) n).doubleValue() : Double.parseDouble(String.valueOf(n));
            return !h.contains(t);
        };
    }

    public static SubQueryExpr subIn(int i) {
        return (subs, rv) -> subs.get(i).contains(rv);
    }

    public static SubQueryExpr notInSub(int i) {
        return (subs, rv) -> !subs.get(i).contains(rv);
    }

    public static BetweenExpr between(double l, double r) {
        return (b) -> {
            double t = (b instanceof Number) ? ((Number) b).doubleValue() : Double.parseDouble(String.valueOf(b));
            return (t >= l) && (t <= r);
        };
    }

    public static BetweenExpr notBetween(double l, double r) {
        return (b) -> {
            double t = (b instanceof Number) ? ((Number) b).doubleValue() : Double.parseDouble(String.valueOf(b));
            return (t < l) || (t > r);
        };
    }

    public static BoolItem boolItem(boolean immediate) {
        return () -> immediate;
    }

    @FunctionalInterface
    public interface BetweenExpr extends ExprItem<Boolean> {
        boolean eval(final Object b);
    }

    @FunctionalInterface
    public interface SubQueryExpr extends ExprItem<Boolean> {
        boolean eval(List<Set<Object>> sub, Object rv);
    }

    @FunctionalInterface
    public interface InExpr extends ExprItem<Boolean> {
        boolean eval(Object n);
    }

    @FunctionalInterface
    public interface IsExpr extends ExprItem<Boolean> {
        boolean eval(Object rv);
    }

    public interface ExprItem<T> extends Serializable {
    }

    @FunctionalInterface
    public interface PropItem extends ExprItem<String> {
        Object get(BiFunction<Object, String, Object> propGetter, Object obj);
    }

    public static PropItem propItem(String propName) {
        return new PropItem() {
            @Override
            public Object get(BiFunction<Object, String, Object> propGetter, Object r) {
                return propGetter.apply(r, propName);
            }

            @Override
            public String toString() {
                return propName;
            }
        };
    }

    @FunctionalInterface
    public interface StringItem extends ExprItem<String> {
        String get();
    }

    public static StringItem stringItem(String immediate) {
        return () -> immediate;
    }

    @FunctionalInterface
    public interface NumericItem extends ExprItem<Double> {
        Double get();
    }

    public static NumericItem numericItem(double immediate) {
        return () -> immediate;
    }

    @FunctionalInterface
    public interface NullItem extends ExprItem<Void> {
        Void get();
    }

    public static NullItem nullItem() {
        return () -> null;
    }

    @FunctionalInterface
    public interface SpatialItem extends ExprItem<String> {
        String get();
    }

    public static SpatialItem spatialItem(String typeName) {
        return () -> typeName;
    }

    @FunctionalInterface
    public interface OpItem extends ExprItem<Object> {
        Object eval(Deque<Object> args);
    }

    public static OpItem opItem(Operator op) {
        return op::op;
    }

    @FunctionalInterface
    public interface StackGetter extends ExprItem<Deque<Object>> {
        Deque<Object> get(Deque<Object> stack);
    }

    public static StackGetter stackGetter(int num) {
        return (stack) -> {
            Deque<Object> top = new LinkedList<>();
            for (int i = 0; i < num; i++) {
                top.push(stack.pop());
            }
            return top;
        };
    }

    @FunctionalInterface
    public interface BoolItem extends ExprItem<Boolean> {
        Boolean get();
    }
}
