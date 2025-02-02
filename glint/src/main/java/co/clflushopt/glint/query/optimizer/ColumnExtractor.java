package co.clflushopt.glint.query.optimizer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr;
import co.clflushopt.glint.query.logical.expr.LogicalAliasExpr;
import co.clflushopt.glint.query.logical.expr.LogicalBinaryExpr;
import co.clflushopt.glint.query.logical.expr.LogicalCastExpr;
import co.clflushopt.glint.query.logical.expr.LogicalColumnExpr;
import co.clflushopt.glint.query.logical.expr.LogicalColumnIndex;
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
        if (expression instanceof LogicalColumnExpr) {
            columns.add(((LogicalColumnExpr) expression).getName());
        }
        if (expression instanceof LogicalColumnIndex) {
            // Extract the column name using the index and the logical plan schema.
            LogicalColumnIndex columnIndex = (LogicalColumnIndex) expression;
            columns.add(plan.getSchema().getFields().get(columnIndex.getIndex()).name());
        }
        if (expression instanceof LogicalAggregateExpr) {
            columns.addAll(extractColumns(plan, ((LogicalAggregateExpr) expression).getExpr()));
        }
        if (expression instanceof LogicalBinaryExpr) {
            columns.addAll(extractColumns(plan, ((LogicalBinaryExpr) expression).getLhs()));
            columns.addAll(extractColumns(plan, ((LogicalBinaryExpr) expression).getRhs()));
        }
        if (expression instanceof LogicalAliasExpr) {
            columns.addAll(extractColumns(plan, ((LogicalAliasExpr) expression).getExpr()));
        }
        if (expression instanceof LogicalCastExpr) {
            columns.addAll(extractColumns(plan, ((LogicalCastExpr) expression).getExpr()));
        }

        return columns;
    }

}
