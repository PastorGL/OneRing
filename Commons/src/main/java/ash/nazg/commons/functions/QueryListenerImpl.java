/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.functions;

import ash.nazg.commons.operations.JoinSpec;
import ash.nazg.commons.operations.UnionSpec;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;
import java.util.stream.Collectors;

public class QueryListenerImpl extends QueryBaseListener {
    private List<String> fromSet = new ArrayList<>();
    private JoinSpec join = null;
    private UnionSpec union = null;
    private boolean starFrom;

    private final List<List<Expressions.ExprItem<?>>> items = new ArrayList<>();
    private boolean starItems;
    private final List<List<Expressions.ExprItem<?>>> subItems = new ArrayList<>();

    private final List<Expressions.ExprItem<?>> predicates = new ArrayList<>();
    private final List<List<Expressions.ExprItem<?>>> subPreds = new ArrayList<>();

    private Long limitRecords = null;
    private Double limitPercent = null;

    private List<Expressions.ExprItem<?>> curItems;
    private List<Expressions.ExprItem<?>> curPreds;

    private static double parseNumber(String numberText) {
        if (numberText.endsWith("L")) {
            return Long.parseLong(numberText.substring(0, numberText.length() - 1));
        }
        if (numberText.toLowerCase().startsWith("0x")) {
            return Long.parseUnsignedLong(numberText.substring(2), 16);
        }
        return Double.parseDouble(numberText);
    }

    private static String stripStringQuotes(String sqlString) {
        if (sqlString == null) {
            return null;
        }
        String string = sqlString;
        // SQL quoting character : '
        if ((string.charAt(0) == '\'') && (string.charAt(string.length() - 1) == '\'')) {
            string = string.substring(1, string.length() - 1);
        }
        return string.replace("''", "'");
    }

    @Override
    public void exitFrom_set(QueryParser.From_setContext ctx) {
        if (ctx.join_op() != null) {
            if (ctx.join_op().K_LEFT() != null) {
                join = JoinSpec.LEFT;
            } else if (ctx.join_op().K_RIGHT() != null) {
                join = JoinSpec.RIGHT;
            } else if (ctx.join_op().K_OUTER() != null) {
                join = JoinSpec.OUTER;
            } else {
                join = JoinSpec.INNER;
            }
        }

        if (ctx.union_op() != null) {
            if (ctx.STAR() != null) {
                starFrom = true;
            }

            if (ctx.union_op().K_XOR() != null) {
                union = UnionSpec.XOR;
            } else if (ctx.union_op().K_AND() != null) {
                union = UnionSpec.AND;
            } else {
                union = UnionSpec.CONCAT;
            }
        }

        fromSet = ctx.IDENTIFIER().stream().map(TerminalNode::getText).collect(Collectors.toList());
    }

    @Override
    public void exitExpression(QueryParser.ExpressionContext ctx) {
        List<Expressions.ExprItem<?>> item = new ArrayList<>();

        Deque<ParseTree> exprOpStack = new LinkedList<>();
        List<ParseTree> predExpStack = new ArrayList<>();
        int i = 0;
        // doing Shunting Yard
        for (; i < ctx.children.size(); i++) {
            ParseTree child = ctx.children.get(i);

            if (child instanceof TerminalNode) {
                TerminalNode tn = (TerminalNode) child;
                int tt = tn.getSymbol().getType();
                if ((tt == QueryLexicon.NUMERIC_LITERAL) || (tt == QueryLexicon.STRING_LITERAL) || (tt == QueryLexicon.K_NULL)) {
                    predExpStack.add(child);
                    continue;
                }
            }

            if (child instanceof QueryParser.Expression_opContext) {
                while (!exprOpStack.isEmpty()) {
                    ParseTree peek = exprOpStack.peek();

                    if (peek instanceof TerminalNode) {
                        TerminalNode tn = (TerminalNode) peek;
                        int tt = tn.getSymbol().getType();
                        if (tt == QueryLexicon.OPEN_PAR) {
                            break;
                        }
                    }
                    if (isHigher(child, peek)) {
                        predExpStack.add(exprOpStack.pop());
                    } else {
                        break;
                    }
                }

                exprOpStack.push(child);
                continue;
            }

            if (child instanceof TerminalNode) {
                TerminalNode tn = (TerminalNode) child;
                int tt = tn.getSymbol().getType();
                if (tt == QueryLexicon.OPEN_PAR) {
                    exprOpStack.add(child);
                    continue;
                }

                if (tt == QueryLexicon.CLOSE_PAR) {
                    while (true) {
                        if (exprOpStack.isEmpty()) {
                            throw new RuntimeException("Mismatched parentheses at query expression token #" + i);
                        }
                        ParseTree pop = exprOpStack.pop();
                        if (!(pop instanceof TerminalNode)) {
                            predExpStack.add(pop);
                        } else {
                            break;
                        }
                    }
                    continue;
                }
            }

            // property_name
            predExpStack.add(child);
        }

        while (!exprOpStack.isEmpty()) {
            predExpStack.add(exprOpStack.pop());
        }

        for (ParseTree exprItem : predExpStack) {
            if (exprItem instanceof QueryParser.Property_nameContext) {
                QueryParser.Property_nameContext propName = (QueryParser.Property_nameContext) exprItem;

                item.add(Expressions.propItem(propName.getText()));
                continue;
            }

            if (exprItem instanceof QueryParser.Expression_opContext) {
                QueryParser.Expression_opContext opContext = (QueryParser.Expression_opContext) exprItem;

                Operator eo = Operator.get(opContext.getText());
                if (eo == null) {
                    throw new RuntimeException("Unknown operator token " + opContext.getText());
                } else {
                    item.add(Expressions.stackGetter(eo.ariness));
                    item.add(Expressions.opItem(eo));
                }
                continue;
            }

            TerminalNode tn = (TerminalNode) exprItem;
            int type = tn.getSymbol().getType();
            if (type == QueryLexicon.NUMERIC_LITERAL) {
                item.add(Expressions.numericItem(parseNumber(tn.getText())));
                continue;
            }
            if (type == QueryLexicon.STRING_LITERAL) {
                item.add(Expressions.stringItem(stripStringQuotes(tn.getText())));
                continue;
            }
            if (type == QueryLexicon.K_NULL) {
                item.add(Expressions.nullItem());
                continue;
            }
        }

        items.add(item);
    }

    @Override
    public void exitWhat_expr(QueryParser.What_exprContext ctx) {
        if (ctx.STAR() != null) {
            starItems = true;
            return;
        }

        if (ctx.spatial_object() != null) {
            items.add(Collections.singletonList(Expressions.spatialItem(ctx.spatial_object().getText())));
            return;
        }
    }

    @Override
    public void exitLimit_expr(QueryParser.Limit_exprContext ctx) {
        if (ctx.PERCENT() != null) {
            limitPercent = parseNumber(ctx.NUMERIC_LITERAL().getText());
            if ((limitPercent <= 0) || (limitPercent > 100)) {
                throw new RuntimeException("Percentage in LIMIT clause can't be 0 or less and more than 100");
            }
        } else {
            limitRecords = (long) parseNumber(ctx.NUMERIC_LITERAL().getText());
            if (limitRecords <= 0) {
                throw new RuntimeException("Record number in LIMIT clause can't be 0 or less");
            }
        }
    }

    @Override
    public void enterSub_query(QueryParser.Sub_queryContext ctx) {
        curItems = new ArrayList<>();
        subItems.add(curItems);
        curPreds = new ArrayList<>();
        subPreds.add(curPreds);
    }

    @Override
    public void enterSelect_stmt(QueryParser.Select_stmtContext ctx) {
        curPreds = predicates;
    }

    @Override
    public void exitSub_query(QueryParser.Sub_queryContext ctx) {
        curItems.add(Expressions.propItem(ctx.property_name().IDENTIFIER().stream().map(ParseTree::getText).collect(Collectors.joining("."))));
        curPreds = predicates;
    }

    @Override
    public void exitWhere_expr(QueryParser.Where_exprContext ctx) {
        Deque<ParseTree> whereOpStack = new LinkedList<>();
        List<ParseTree> predExpStack = new ArrayList<>();
        int i = 0;
        // doing Shunting Yard
        for (; i < ctx.children.size(); i++) {
            ParseTree child = ctx.children.get(i);

            if ((child instanceof QueryParser.Expression_opContext)
                    || (child instanceof QueryParser.Comparison_opContext)
                    || (child instanceof QueryParser.Bool_opContext)
                    || (child instanceof QueryParser.In_opContext)
                    || (child instanceof QueryParser.Is_opContext)
                    || (child instanceof QueryParser.Between_opContext)) {
                while (!whereOpStack.isEmpty()) {
                    ParseTree peek = whereOpStack.peek();

                    if (peek instanceof TerminalNode) {
                        TerminalNode tn = (TerminalNode) peek;
                        int tt = tn.getSymbol().getType();
                        if (tt == QueryLexicon.OPEN_PAR) {
                            break;
                        }
                    }
                    if (isHigher(child, peek)) {
                        predExpStack.add(whereOpStack.pop());
                    } else {
                        break;
                    }
                }

                whereOpStack.push(child);
                continue;
            }

            if (child instanceof TerminalNode) {
                TerminalNode tn = (TerminalNode) child;
                int tt = tn.getSymbol().getType();
                if (tt == QueryLexicon.OPEN_PAR) {
                    whereOpStack.add(child);
                    continue;
                }

                if (tt == QueryLexicon.CLOSE_PAR) {
                    while (true) {
                        if (whereOpStack.isEmpty()) {
                            throw new RuntimeException("Mismatched parentheses at query token #" + i);
                        }
                        ParseTree pop = whereOpStack.pop();
                        if (!(pop instanceof TerminalNode)) {
                            predExpStack.add(pop);
                        } else {
                            break;
                        }
                    }
                    continue;
                }
            }

            // expression
            predExpStack.add(child);
        }

        while (!whereOpStack.isEmpty()) {
            predExpStack.add(whereOpStack.pop());
        }

        int subQ = 0;
        for (ParseTree exprItem : predExpStack) {
            if (exprItem instanceof QueryParser.Property_nameContext) {
                QueryParser.Property_nameContext propName = (QueryParser.Property_nameContext) exprItem;

                curPreds.add(Expressions.propItem(propName.getText()));
                continue;
            }

            if (exprItem instanceof QueryParser.Between_opContext) {
                QueryParser.Between_opContext between = (QueryParser.Between_opContext) exprItem;

                curPreds.add(Expressions.stackGetter(1));

                double l = parseNumber(between.NUMERIC_LITERAL(0).getText());
                double r = parseNumber(between.NUMERIC_LITERAL(1).getText());
                curPreds.add((between.K_NOT() == null)
                        ? Expressions.between(l, r)
                        : Expressions.notBetween(l, r)
                );

                continue;
            }

            if (exprItem instanceof QueryParser.In_opContext) {
                QueryParser.In_opContext inCtx = (QueryParser.In_opContext) exprItem;
                boolean not = inCtx.K_NOT() != null;

                // column_name NOT? IN ( subquery )
                if (inCtx.sub_query() != null) {
                    curPreds.add(Expressions.stackGetter(1));
                    curPreds.add(not ? Expressions.notInSub(subQ) : Expressions.subIn(subQ));

                    subQ++;
                    continue;
                }

                // column_name NOT? IN ( STRING_LITERAL,... | NUMERIC_LITERAL,... )
                if (!inCtx.NUMERIC_LITERAL().isEmpty()) {
                    Set<Double> rv = inCtx.NUMERIC_LITERAL().stream().map(ParseTree::getText).map(QueryListenerImpl::parseNumber).collect(Collectors.toSet());

                    curPreds.add(Expressions.stackGetter(1));

                    curPreds.add(not ? Expressions.notInNum(rv) : Expressions.inNum(rv));
                    continue;
                }

                if (!inCtx.STRING_LITERAL().isEmpty()) {
                    Set<String> rv = inCtx.STRING_LITERAL().stream().map(ParseTree::getText).map(QueryListenerImpl::stripStringQuotes).collect(Collectors.toSet());

                    curPreds.add(Expressions.stackGetter(1));

                    curPreds.add(not ? Expressions.notIn(rv) : Expressions.in(rv));
                    continue;
                }
            }

            // column_name IS NOT? NULL
            if (exprItem instanceof QueryParser.Is_opContext) {
                curPreds.add(Expressions.stackGetter(1));

                curPreds.add((((QueryParser.Is_opContext) exprItem).K_NOT() == null) ? Expressions.isNull() : Expressions.nonNull());

                continue;
            }

            if ((exprItem instanceof QueryParser.Expression_opContext)
                    || (exprItem instanceof QueryParser.Comparison_opContext)
                    || (exprItem instanceof QueryParser.Bool_opContext)) {
                Operator eo = Operator.get(exprItem.getText());
                if (eo == null) {
                    throw new RuntimeException("Unknown operator token " + exprItem.getText());
                } else {
                    curPreds.add(Expressions.stackGetter(eo.ariness));
                    curPreds.add(Expressions.opItem(eo));
                }

                continue;
            }

            TerminalNode tn = (TerminalNode) exprItem;
            int type = tn.getSymbol().getType();
            if (type == QueryLexicon.NUMERIC_LITERAL) {
                curPreds.add(Expressions.numericItem(parseNumber(tn.getText())));
                continue;
            }
            if (type == QueryLexicon.STRING_LITERAL) {
                curPreds.add(Expressions.stringItem(stripStringQuotes(tn.getText())));
                continue;
            }
            if (type == QueryLexicon.K_NULL) {
                curPreds.add(Expressions.nullItem());
                continue;
            }
            if ((type == QueryLexicon.K_TRUE) || (type == QueryLexicon.K_FALSE)) {
                curPreds.add(Expressions.boolItem(Boolean.parseBoolean(tn.getText())));
                continue;
            }
        }
    }

    private boolean isHigher(ParseTree o1, ParseTree o2) {
        Operator first = Operator.get(o1.getText());
        if (o1 instanceof QueryParser.In_opContext) {
            first = Operator.IN;
        }
        if (o1 instanceof QueryParser.Is_opContext) {
            first = Operator.IS;
        }
        if (o1 instanceof QueryParser.Between_opContext) {
            first = Operator.BETWEEN;
        }

        Operator second = Operator.get(o2.getText());
        if (o2 instanceof QueryParser.In_opContext) {
            second = Operator.IN;
        }
        if (o2 instanceof QueryParser.Is_opContext) {
            second = Operator.IS;
        }
        if (o2 instanceof QueryParser.Between_opContext) {
            second = Operator.BETWEEN;
        }

        return ((second.prio - first.prio) > 0) || ((first.prio == second.prio) && !first.rightAssoc);
    }

    public List<String> getFromSet() {
        return fromSet;
    }

    public boolean isStarFrom() {
        return starFrom;
    }

    public JoinSpec getJoinSpec() {
        return join;
    }

    public UnionSpec getUnion() {
        return union;
    }

    public List<Expressions.ExprItem<?>> getQuery() {
        return predicates;
    }

    public List<List<Expressions.ExprItem<?>>> getSubQueries() {
        return subPreds;
    }

    public List<List<Expressions.ExprItem<?>>> getItems() {
        return items;
    }

    public List<List<Expressions.ExprItem<?>>> getSubItems() {
        return subItems;
    }

    public boolean isStarItems() {
        return starItems;
    }

    public Long getLimitRecords() {
        return limitRecords;
    }

    public Double getLimitPercent() {
        return limitPercent;
    }
}
