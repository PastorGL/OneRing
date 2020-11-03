package ash.nazg.spatial.functions;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static ash.nazg.spatial.functions.QueryParser.CLOSE_PAR;
import static ash.nazg.spatial.functions.QueryParser.OPEN_PAR;

public class QueryListenerImpl extends QueryBaseListener {
    private final List<Expressions.QueryExpr> where = new ArrayList<>();
    private final List<String> what = new ArrayList<>();
    private Long limitRecords = null;
    private Double limitPercent = null;

    private final Deque<ParseTree> whereOpStack = new LinkedList<>();
    private final List<ParseTree> predExpStack = new ArrayList<>();

    @Override
    public void exitWhat_expr(QueryParser.What_exprContext ctx) {
/*        if (ctx.STAR() != null) {
            what.add("*");
        }

        if (ctx.property_name() != null) {
            ctx.property_name().forEach(e ->
                    what.add(e.IDENTIFIER().stream().map(ParseTree::getText).collect(Collectors.joining("."))));
        }*/

        if (ctx.spatial_object() != null) {
            what.add(ctx.spatial_object().getText());
        }
    }

    @Override
    public void exitLimit_expr(QueryParser.Limit_exprContext ctx) {
        if (ctx.INTEGER_LITERAL() != null) {
            limitRecords = new Long(ctx.INTEGER_LITERAL().getText());
            if (limitRecords <= 0) {
                throw new RuntimeException("Record number in LIMIT clause can't be 0 or less");
            }
        }

        if (ctx.PERCENT() != null) {
            limitPercent = new Double(ctx.NUMERIC_LITERAL().getText());
            if ((limitPercent <= 0) || (limitPercent > 100)) {
                throw new RuntimeException("Percentage in LIMIT clause can't be 0 or less and more than 100");
            }
        }
    }

    @Override
    public void exitWhere_expr(QueryParser.Where_exprContext ctx) {
        int i = 0;
        try {
            // doing Shunting Yard
            List<ParseTree> children = ctx.children;
            for (; i < children.size(); i++) {
                ParseTree child = children.get(i);
                if (child instanceof QueryParser.Logic_opContext) {
                    QueryParser.Logic_opContext logic = (QueryParser.Logic_opContext) child;
                    while (!whereOpStack.isEmpty() && isHigherOp(logic, whereOpStack.peek())) {
                        predExpStack.add(whereOpStack.pop());
                    }
                    whereOpStack.push(logic);
                } else if (child instanceof TerminalNode) {
                    TerminalNode paren = (TerminalNode) child;

                    int type = paren.getSymbol().getType();
                    if (type == OPEN_PAR) {
                        whereOpStack.push(paren);
                    } else {
                        ParseTree p;
                        while (!(((p = whereOpStack.peek()) instanceof TerminalNode) && ((TerminalNode) p).getSymbol().getType() != CLOSE_PAR)) {
                            predExpStack.add(whereOpStack.pop());
                        }
                        whereOpStack.pop();
                    }
                } else {
                    predExpStack.add(child);
                }
            }
        } catch (RuntimeException e) {
            throw new RuntimeException("Mismatched parentheses at query token #" + i, e);
        }

        while (!whereOpStack.isEmpty()) {
            predExpStack.add(whereOpStack.pop());
        }

        for (ParseTree whereExpr : predExpStack) {
            if (whereExpr instanceof QueryParser.Atomic_exprContext) {
                QueryParser.Atomic_exprContext atomicExpr = (QueryParser.Atomic_exprContext) whereExpr;

                QueryParser.Equality_opContext equalityOp = atomicExpr.equality_op();
                QueryParser.Comparison_opContext comparisonOp = atomicExpr.comparison_op();
                TerminalNode numericLiteral = atomicExpr.NUMERIC_LITERAL();

                QueryParser.Property_nameContext propertyName = atomicExpr.property_name();
                String propName = propertyName.getText();

                // column_name equality_op STRING_LITERAL
                TerminalNode stringLiteral = atomicExpr.STRING_LITERAL();
                if (equalityOp != null && stringLiteral != null) {
                    addPropGetter(propName);

                    String rv = stripStringQuotes(stringLiteral.getText());
                    if (equalityOp.EQ() != null || equalityOp.EQ2() != null) {
                        where.add(Expressions.stringEqual(rv));
                    }
                    if (equalityOp.NOT_EQ1() != null || equalityOp.NOT_EQ2() != null) {
                        where.add(Expressions.stringUnequal(rv));
                    }
                    continue;
                }

                // column_name equality_op NUMERIC_LITERAL
                if (equalityOp != null && numericLiteral != null) {
                    addPropGetter(propName);

                    Double rv = new Double(numericLiteral.getText());
                    if (equalityOp.EQ() != null || equalityOp.EQ2() != null) {
                        where.add(Expressions.numericEqual(rv));
                    }
                    if (equalityOp.NOT_EQ1() != null || equalityOp.NOT_EQ2() != null) {
                        where.add(Expressions.numericUnequal(rv));
                    }
                    continue;
                }

                // column_name comparison_op NUMERIC_LITERAL
                if (comparisonOp != null && numericLiteral != null) {
                    addPropGetter(propName);

                    Double rv = new Double(numericLiteral.getText());
                    if (comparisonOp.GT() != null) {
                        where.add(Expressions.numericGreater(rv));
                    }
                    if (comparisonOp.GT_EQ() != null) {
                        where.add(Expressions.numericGreaterEq(rv));
                    }
                    if (comparisonOp.LT() != null) {
                        where.add(Expressions.numericLess(rv));
                    }
                    if (comparisonOp.LT_EQ() != null) {
                        where.add(Expressions.numericLessEq(rv));
                    }
                    continue;
                }

                // column_name regex_op STRING_LITERAL
                if (atomicExpr.regex_op() != null && stringLiteral != null) {
                    String pattern = stripStringQuotes(stringLiteral.getText());
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

                    addPropGetter(propName);

                    where.add(Expressions.stringRegex(regexFlags, pattern));
                }

                if (atomicExpr.K_NULL() != null) {
                    addPropGetter(propName);

                    if (atomicExpr.K_NOT() != null) {
                        where.add(Expressions.isNotNull());
                    } else {
                        where.add(Expressions.isNull());
                    }
                }

                continue;
            }

            if (whereExpr instanceof QueryParser.Logic_opContext) {
                QueryParser.Logic_opContext logicOp = (QueryParser.Logic_opContext) whereExpr;

                if (logicOp.K_NOT() != null) {
                    addStackGetter(1);

                    where.add(Expressions.not());
                }
                if (logicOp.K_AND() != null) {
                    addStackGetter(2);

                    where.add(Expressions.and());
                }
                if (logicOp.K_OR() != null) {
                    addStackGetter(2);

                    where.add(Expressions.or());
                }
            }
        }
    }

    private boolean isHigherOp(QueryParser.Logic_opContext ctx1, ParseTree ctx2) {
        if (!(ctx2 instanceof QueryParser.Logic_opContext)) {
            return false;
        }

        QueryParser.Logic_opContext other = (QueryParser.Logic_opContext) ctx2;

        // NOT > *
        if (other.K_NOT() != null) {
            return true;
        }

        // NOT > AND > *
        if ((other.K_AND() != null) && (ctx1.K_NOT() == null)) {
            return true;
        }

        // * >= OR
        if ((other.K_OR() != null) && (ctx1.K_OR() != null)) {
            return true;
        }

        return false;
    }

    private String stripStringQuotes(String sqlString) {
        if (sqlString == null) {
            return null;
        }
        String string = sqlString;
        // escape character : '
        if ((string.charAt(0) == '\'') && (string.charAt(string.length() - 1) == '\'')) {
            string = string.substring(1, string.length() - 1);
        }
        return string;
    }

    private void addPropGetter(String prop) {
        where.add(Expressions.propGetter(prop));
    }

    private void addStackGetter(int num) {
        where.add(Expressions.stackGetter(num));
    }

    public List<Expressions.QueryExpr> getQuery() {
        return where;
    }

    public List<String> getWhat() {
        return what;
    }

    public Long getLimitRecords() {
        return limitRecords;
    }

    public Double getLimitPercent() {
        return limitPercent;
    }
}
