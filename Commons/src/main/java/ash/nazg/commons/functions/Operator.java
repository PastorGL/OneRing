package ash.nazg.commons.functions;

import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum Operator {
    TERNARY1(":", 0, 2, true) {
        @Override
        public Object op(Deque<Object> args) {
            return op0(args);
        }

        @Override
        protected Object op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            return (a == null) ? b : a;
        }
    },
    TERNARY2("?", 5) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = popBoolean(args);
            Object b = args.pop();
            return a ? b : null;
        }
    },

    OR("OR", 10) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = popBoolean(args);
            boolean b = popBoolean(args);
            return a || b;
        }
    },
    XOR("XOR", 10) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = popBoolean(args);
            boolean b = popBoolean(args);
            return a != b;
        }
    },
    AND("AND", 20) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = popBoolean(args);
            boolean b = popBoolean(args);
            return a && b;
        }
    },
    NOT("NOT", 30, 1, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            return !Operator.popBoolean(args);
        }
    },

    IN("IN", 35, 2, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator IN call");
        }
    },
    IS("IS", 35, 1, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator IS call");
        }
    },
    BETWEEN("BETWEEN", 35, 3, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator BETWEEN call");
        }
    },

    EQ("=", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            if (a instanceof Number) {
                return (double) a == Double.parseDouble(String.valueOf(b));
            }
            if (a instanceof Boolean) {
                return (boolean) a == Boolean.parseBoolean(String.valueOf(b));
            }
            return Objects.equals(a, b);
        }
    },
    EQ2("==", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            return EQ.op0(args);
        }
    },
    NEQ("!=", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            if (a instanceof Number) {
                return (double) a != Double.parseDouble(String.valueOf(b));
            }
            if (a instanceof Boolean) {
                return (boolean) a != Boolean.parseBoolean(String.valueOf(b));
            }
            return !Objects.equals(a, b);
        }
    },
    NE2("<>", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            return NEQ.op0(args);
        }
    },
    GE(">=", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a >= b;
        }
    },
    GT(">", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a > b;
        }
    },
    LE("<=", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a <= b;
        }
    },
    LT("<", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a < b;
        }
    },

    LIKE("LIKE", 40, 2, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            String r = String.valueOf(args.pop());

            String pattern = String.valueOf(args.pop());
            int regexFlags = Pattern.MULTILINE;
            if (pattern.startsWith("/")) {
                int lastSlash = pattern.lastIndexOf('/');

                String patternFlags = pattern.substring(lastSlash).toLowerCase();
                regexFlags |= patternFlags.contains("i") ? Pattern.CASE_INSENSITIVE : 0;
                regexFlags |= patternFlags.contains("e") ? Pattern.DOTALL : 0;
                if (patternFlags.contains("s")) {
                    regexFlags &= ~Pattern.DOTALL;
                } else {
                    regexFlags |= Pattern.DOTALL;
                }

                pattern = pattern.substring(1, lastSlash);
            }

            final Pattern p = Pattern.compile(pattern, regexFlags);

            return (r != null) && p.matcher(r).matches();
        }
    },
    MATCH("MATCH", 40, 2, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            return LIKE.op0(args);
        }
    },
    REGEX("REGEX", 40, 2, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            return LIKE.op0(args);
        }
    },

    BOR("|", 105) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a | b;
        }
    },
    BXOR("#", 110) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a ^ b;
        }
    },
    BAND("&", 115) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a & b;
        }
    },
    BSL("<<", 120, 2, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a << b;
        }
    },
    BSR(">>", 120, 2, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a >> b;
        }
    },

    CAT("||", 125) {
        @Override
        protected Object op0(Deque<Object> args) {
            return args.stream().map(String::valueOf).collect(Collectors.joining());
        }
    },
    ADD("+", 125) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a + b;
        }
    },
    SUB("-", 125) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a - b;
        }
    },

    MUL("*", 130) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a * b;
        }
    },
    DIV("/", 130) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a / b;
        }
    },
    MOD("%", 130) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a % b;
        }
    },

    ABS("@@", 135, 1, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            return Math.abs(popDouble(args));
        }
    },
    EXP("^", 135, 2, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return Math.pow(a, b);
        }
    },

    BNOT("~", 140, 1, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            return ~popLong(args);
        }
    };

    private static long popLong(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Number) {
            return ((Number) a).longValue();
        }
        return (long) Double.parseDouble(String.valueOf(a));
    }

    private static double popDouble(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Number) {
            return ((Number) a).doubleValue();
        }
        return Double.parseDouble(String.valueOf(a));
    }

    private static boolean popBoolean(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Boolean) {
            return (Boolean) a;
        }
        return Boolean.parseBoolean(String.valueOf(a));
    }

    private final String op;
    public final int prio;
    public int ariness = 2;
    public boolean rightAssoc = false;

    Operator(String op, int prio) {
        this.op = op;
        this.prio = prio;
    }

    Operator(String op, int prio, int ariness, boolean rightAssoc) {
        this.op = op;
        this.prio = prio;
        this.ariness = ariness;
        this.rightAssoc = rightAssoc;
    }

    static Operator get(String op) {
        for (Operator eo : Operator.values()) {
            if (eo.op.equals(op)) {
                return eo;
            }
        }

        return null;
    }

    public Object op(Deque<Object> args) {
        for (Object a : args) {
            if (a == null) {
                return null;
            }
        }

        return op0(args);
    }

    protected abstract Object op0(Deque<Object> args);

    public static boolean bool(BiFunction<Object, String, Object> propGetter, Object props, List<Set<Object>> subs, List<Expressions.ExprItem<?>> item) {
        if (item.isEmpty()) {
            return true;
        }

        Object r = eval(propGetter, props, subs, item);
        if (r == null) {
            return false;
        }

        return Boolean.parseBoolean(String.valueOf(r));
    }

    public static Object eval(BiFunction<Object, String, Object> propGetter, Object props, List<Set<Object>> subs, List<Expressions.ExprItem<?>> item) {
        if (item.isEmpty()) {
            return null;
        }

        Deque<Object> stack = new LinkedList<>();
        Deque<Object> top = null;
        for (Expressions.ExprItem<?> ei : item) {
            // these all push to expression stack
            if (ei instanceof Expressions.PropItem) {
                stack.push(((Expressions.PropItem) ei).get(propGetter, props));
                continue;
            }
            if (ei instanceof Expressions.StringItem) {
                stack.push(((Expressions.StringItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.NumericItem) {
                stack.push(((Expressions.NumericItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.NullItem) {
                stack.push(((Expressions.NullItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.BoolItem) {
                stack.push(((Expressions.BoolItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.OpItem) {
                stack.push(((Expressions.OpItem) ei).eval(top));
                continue;
            }
            if (ei instanceof Expressions.IsExpr) {
                stack.push(((Expressions.IsExpr) ei).eval(top.pop()));
                continue;
            }
            if (ei instanceof Expressions.InExpr) {
                stack.push(((Expressions.InExpr) ei).eval(top.pop()));
                continue;
            }
            if (ei instanceof Expressions.BetweenExpr) {
                stack.push(((Expressions.BetweenExpr) ei).eval(top.pop()));
                continue;
            }
            if (ei instanceof Expressions.SubQueryExpr) {
                stack.push(((Expressions.SubQueryExpr) ei).eval(subs, top.pop()));
                continue;
            }
            // and this one pops from it
            if (ei instanceof Expressions.StackGetter) {
                top = ((Expressions.StackGetter) ei).get(stack);
                continue;
            }
        }

        if (stack.size() != 1) {
            throw new RuntimeException("Invalid SELECT item expression");
        }

        return stack.pop();
    }
}
