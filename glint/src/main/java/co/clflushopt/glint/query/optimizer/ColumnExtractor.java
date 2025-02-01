package co.clflushopt.glint.query.optimizer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import co.clflushopt.glint.query.logical.expr.AggregateExpr;
import co.clflushopt.glint.query.logical.expr.AliasExpr;
import co.clflushopt.glint.query.logical.expr.BinaryExpr;
import co.clflushopt.glint.query.logical.expr.CastExpr;
import co.clflushopt.glint.query.logical.expr.ColumnExpr;
import co.clflushopt.glint.query.logical.expr.ColumnIndex;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;

/**
 * The column extractor extracts nbamed columns from a logical plan.
 *
 * ColumnExtractor
 */
public class ColumnExtractor {

    /**
     * Extracts all the named columns from the logical plan.
     *
     * @param plan
     * @param expressions
     * @return
     */
    public static Set<String> extractColumns(LogicalPlan plan, List<LogicalExpr> expressions) {
        Set<String> columns = new HashSet<>();
        for (LogicalExpr expression : expressions) {
            columns.addAll(extractColumns(plan, expression));
        }
        return columns;
    }

    /**
     * Extracts the named columns from the logical plan.
     *
     * @param plan the logical plan.
     * @return the named columns.
     */
    public static Set<String> extractColumns(LogicalPlan plan, LogicalExpr expression) {
        Set<String> columns = new HashSet<>();
        if (expression instanceof ColumnExpr) {
            columns.add(((ColumnExpr) expression).getName());
        }
        if (expression instanceof ColumnIndex) {
            // Extract the column name using the index and the logical plan schema.
            ColumnIndex columnIndex = (ColumnIndex) expression;
            columns.add(plan.getSchema().getFields().get(columnIndex.getIndex()).name());
        }
        if (expression instanceof AggregateExpr) {
            columns.addAll(extractColumns(plan, ((AggregateExpr) expression).getExpr()));
        }
        if (expression instanceof BinaryExpr) {
            columns.addAll(extractColumns(plan, ((BinaryExpr) expression).getLhs()));
            columns.addAll(extractColumns(plan, ((BinaryExpr) expression).getRhs()));
        }
        if (expression instanceof AliasExpr) {
            columns.addAll(extractColumns(plan, ((AliasExpr) expression).getExpr()));
        }
        if (expression instanceof CastExpr) {
            columns.addAll(extractColumns(plan, ((CastExpr) expression).getExpr()));
        }

        return columns;
    }

}
