package co.clflushopt.glint.query.planner;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.dataframe.DataFrame;
import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr;
import co.clflushopt.glint.query.logical.expr.LogicalAliasExpr;
import co.clflushopt.glint.query.logical.expr.LogicalBinaryExpr;
import co.clflushopt.glint.query.logical.expr.LogicalBooleanExpr;
import co.clflushopt.glint.query.logical.expr.LogicalCastExpr;
import co.clflushopt.glint.query.logical.expr.LogicalColumnExpr;
import co.clflushopt.glint.query.logical.expr.LogicalColumnIndex;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.expr.LogicalLiteralDouble;
import co.clflushopt.glint.query.logical.expr.LogicalLiteralLong;
import co.clflushopt.glint.query.logical.expr.LogicalLiteralString;
import co.clflushopt.glint.query.logical.expr.LogicalMathExpr;
import co.clflushopt.glint.query.logical.expr.LogicalScalarExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.sql.SqlAliasExpr;
import co.clflushopt.glint.sql.SqlBinaryExpr;
import co.clflushopt.glint.sql.SqlCast;
import co.clflushopt.glint.sql.SqlDouble;
import co.clflushopt.glint.sql.SqlExpression;
import co.clflushopt.glint.sql.SqlFunction;
import co.clflushopt.glint.sql.SqlIdentifier;
import co.clflushopt.glint.sql.SqlLong;
import co.clflushopt.glint.sql.SqlSelectStmt;
import co.clflushopt.glint.sql.SqlString;
import co.clflushopt.glint.types.ArrowTypes;

/**
 * The logical planner builds logical plans from a SQL AST.
 *
 */
public class LogicalPlanner {
    private final Logger logger = Logger.getLogger(LogicalPlanner.class.getName());

    public LogicalPlanner() {
    }

    public LogicalPlan plan() {
        return null;
    }

    private LogicalExpr createLogicalExpr(SqlExpression expr, DataFrame input) {
        if (expr instanceof SqlIdentifier e) {
            return new LogicalColumnExpr(e.getId());
        }
        if (expr instanceof SqlString e) {
            return new LogicalLiteralString(e.getValue());
        }
        if (expr instanceof SqlLong e) {
            return new LogicalLiteralLong(e.getValue());
        }
        if (expr instanceof SqlDouble e) {
            return new LogicalLiteralDouble(e.getValue());
        }
        if (expr instanceof SqlAliasExpr e) {
            var aliased = createLogicalExpr(e.getExpr(), input);
            var alias = e.getAlias().getId();
            return new LogicalAliasExpr(aliased, alias);
        }
        if (expr instanceof SqlCast e) {
            var castee = createLogicalExpr(e.getExpr(), input);
            var type = arrowTypeFromString(e.getDataType().getId());
            return new LogicalCastExpr(castee, type);
        }
        if (expr instanceof SqlFunction e) {
            var arg = createLogicalExpr(e.getArgs().getFirst(), input);

            switch (e.getId()) {
            case "MIN":
                return LogicalAggregateExpr.min(arg);
            case "MAX":
                return LogicalAggregateExpr.max(arg);
            case "AVG":
                return LogicalAggregateExpr.avg(arg);
            case "COUNT":
                return LogicalAggregateExpr.count(arg);
            default:
                throw new RuntimeException("Unknown function " + e.getId());
            }
        }
        if (expr instanceof SqlBinaryExpr e) {
            var l = createLogicalExpr(e.getLeft(), input);
            var r = createLogicalExpr(e.getRight(), input);
            switch (e.getOperator()) {
            case "=":
                return LogicalBooleanExpr.Eq(l, r);
            case "!=":
                return LogicalBooleanExpr.Neq(l, r);
            case ">":
                return LogicalBooleanExpr.Gt(l, r);
            case ">=":
                return LogicalBooleanExpr.Gte(l, r);
            case "<":
                return LogicalBooleanExpr.Lt(l, r);
            case "<=":
                return LogicalBooleanExpr.Lte(l, r);
            case "AND":
                return LogicalBooleanExpr.And(l, r);
            case "OR":
                return LogicalBooleanExpr.Or(l, r);
            case "+":
                return LogicalMathExpr.Add(l, r);
            case "-":
                return LogicalMathExpr.Sub(l, r);
            case "*":
                return LogicalMathExpr.Mul(l, r);
            case "/":
                return LogicalMathExpr.Div(l, r);
            default:
                break;
            }
        }

        throw new RuntimeException("Cannot create logical expression for " + expr);
    }

    public DataFrame createDataFrame(SqlSelectStmt select, Map<String, DataFrame> tables)
            throws SQLException {
        // Get reference to data source
        DataFrame table = tables.get(select.getTableName());
        if (table == null) {
            throw new SQLException("No table named '" + select.getTableName() + "'");
        }

        // Translate projection SQL expressions into logical expressions
        List<LogicalExpr> projectionExpr = select.getProjection().stream()
                .map(expr -> createLogicalExpr(expr, table)).collect(Collectors.toList());

        // Build list of columns referenced in projection
        List<String> columnNamesInProjection = getReferencedColumns(projectionExpr);

        // Count aggregate expressions
        long aggregateExprCount = projectionExpr.stream().filter(this::isAggregateExpr).count();

        if (aggregateExprCount == 0 && !select.getGroupBy().isEmpty()) {
            throw new SQLException("GROUP BY without aggregate expressions is not supported");
        }

        // Check if filter references columns not in projection
        List<String> columnNamesInSelection = getReferencedColumnsInSelection(select, table);

        DataFrame plan = table;

        if (aggregateExprCount == 0) {
            return planNonAggregateQuery(select, plan, projectionExpr, columnNamesInSelection,
                    columnNamesInProjection);
        } else {
            List<LogicalExpr> projection = new ArrayList<>();
            List<LogicalAggregateExpr> aggrExpr = new ArrayList<>();
            int numGroupCols = select.getGroupBy().size();
            int groupCount = 0;

            for (LogicalExpr expr : projectionExpr) {
                if (expr instanceof LogicalAggregateExpr e) {
                    projection.add(new LogicalColumnIndex(numGroupCols + aggrExpr.size()));
                    aggrExpr.add(e);
                } else if (expr instanceof LogicalAliasExpr) {
                    var alias = (LogicalAliasExpr) expr;
                    projection.add(new LogicalAliasExpr(
                            new LogicalColumnIndex(numGroupCols + aggrExpr.size()),
                            alias.getAlias()));
                    aggrExpr.add((LogicalAggregateExpr) alias.getExpr());
                } else {
                    projection.add(new LogicalColumnIndex(groupCount));
                    groupCount++;
                }
            }

            plan = planAggregateQuery(projectionExpr, select, columnNamesInSelection, plan,
                    aggrExpr);
            plan = plan.project(projection);

            if (select.getHaving() != null) {
                plan = plan.filter(createLogicalExpr(select.getHaving(), plan));
            }

            return plan;
        }
    }

    private boolean isAggregateExpr(LogicalExpr expr) {
        if (expr instanceof LogicalAggregateExpr) {
            return true;
        }

        if (expr instanceof LogicalAliasExpr e) {
            return isAggregateExpr(e.getExpr());
        }

        // Handle binary expressions that might contain aggregate functions
        if (expr instanceof LogicalBinaryExpr e) {
            return isAggregateExpr(e.getLhs()) || isAggregateExpr(e.getRhs());
        }

        // Handle CAST expressions
        if (expr instanceof LogicalCastExpr e) {
            return isAggregateExpr(e.getExpr());
        }

        // Handle function calls that might contain aggregate functions as arguments
        if (expr instanceof LogicalScalarExpr e) {
            return e.getArgs().stream().anyMatch(this::isAggregateExpr);
        }

        return false;
    }

    private DataFrame planNonAggregateQuery(SqlSelectStmt select, DataFrame df,
            List<LogicalExpr> projectionExpr, List<String> columnNamesInSelection,
            List<String> columnNamesInProjection) {

        DataFrame plan = df;
        if (select.getSelection() == null) {
            return plan.project(projectionExpr);
        }

        HashSet<String> missing = new HashSet<>(columnNamesInSelection);
        missing.removeAll(columnNamesInProjection);

        // Add more detailed logging
        logger.info("Missing columns: " + missing);

        // If the selection only references outputs from the projection we can simply
        // apply
        // the filter expression to the DataFrame representing the projection
        if (missing.isEmpty()) {
            plan = plan.project(projectionExpr);
            plan = plan.filter(createLogicalExpr(select.getSelection(), plan));
        } else {
            // Because the selection references some columns that are not in the projection
            // output
            // we need to create an interim projection that has the additional columns and
            // then
            // we need to remove them after the selection has been applied
            int n = projectionExpr.size();

            // Create extended projection with missing columns
            List<LogicalExpr> extendedProjection = new ArrayList<>(projectionExpr);

            List<LogicalExpr> missingColumns = missing.stream().map(name -> {
                return new LogicalColumnExpr(name);
            }).collect(Collectors.toList());

            extendedProjection.addAll(missingColumns);

            plan = plan.project(extendedProjection);
            // Log schema after first projection
            plan = plan.filter(createLogicalExpr(select.getSelection(), plan));

            // Drop the columns that were added for the selection
            var finalPlan = plan;
            List<LogicalExpr> finalProjection = IntStream.range(0, n).mapToObj(
                    i -> new LogicalColumnExpr(finalPlan.getSchema().getFields().get(i).name()))
                    .collect(Collectors.toList());

            plan = plan.project(finalProjection);
        }

        return plan;
    }

    private DataFrame planAggregateQuery(List<LogicalExpr> projectionExpr, SqlSelectStmt select,
            List<String> columnNamesInSelection, DataFrame df,
            List<LogicalAggregateExpr> aggregateExpr) {

        DataFrame plan = df;

        // Filter out aggregate expressions
        List<LogicalExpr> projectionWithoutAggregates = projectionExpr.stream()
                .filter(expr -> !(expr instanceof LogicalAggregateExpr))
                .collect(Collectors.toList());

        if (select.getSelection() != null) {
            List<String> columnNamesInProjectionWithoutAggregates = getReferencedColumns(
                    projectionWithoutAggregates);

            HashSet<String> missing = new HashSet<>(columnNamesInSelection);
            missing.removeAll(columnNamesInProjectionWithoutAggregates);

            // If the selection only references outputs from the projection we can simply
            // apply
            // the filter expression to the DataFrame representing the projection
            if (missing.isEmpty()) {
                plan = plan.project(projectionWithoutAggregates);
                plan = plan.filter(createLogicalExpr(select.getSelection(), plan));
            } else {
                // Because the selection references some columns that are not in the projection
                // output
                // we need to create an interim projection that has the additional columns
                // and then we need to remove them after the selection has been applied
                List<LogicalExpr> extendedProjection = new ArrayList<>(projectionWithoutAggregates);
                extendedProjection.addAll(
                        missing.stream().map(LogicalColumnExpr::new).collect(Collectors.toList()));

                plan = plan.project(extendedProjection);
                plan = plan.filter(createLogicalExpr(select.getSelection(), plan));
            }
        }
        var finalPlan = plan;

        List<LogicalExpr> groupByExpr = select.getGroupBy().stream()
                .map(expr -> createLogicalExpr(expr, finalPlan)).collect(Collectors.toList());

        return plan.aggregate(groupByExpr, aggregateExpr);
    }

    private List<String> getReferencedColumnsInSelection(SqlSelectStmt stmt, DataFrame table) {
        var accumulator = new HashSet<String>();
        if (stmt.getSelection() != null) {
            var filter = createLogicalExpr(stmt.getSelection(), table);
            visitColumnReferences(filter, accumulator);

            // List of column names in the dataframe.
            var validColumnNames = table.getSchema().getFields().stream().map(field -> field.name())
                    .collect(Collectors.toList());
            accumulator.removeIf(name -> !validColumnNames.contains(name));
        }

        return new ArrayList<String>(accumulator);
    }

    private List<String> getReferencedColumns(List<LogicalExpr> expressions) {
        var accumulator = new HashSet<String>();
        expressions.forEach(expr -> visitColumnReferences(expr, accumulator));

        return new ArrayList<String>(accumulator);
    }

    private void visitColumnReferences(LogicalExpr expr, HashSet<String> columns) {
        if (expr instanceof LogicalColumnExpr col) {
            columns.add(col.getName());
        }
        if (expr instanceof LogicalAliasExpr alias) {
            visitColumnReferences(alias.getExpr(), columns);
        }
        if (expr instanceof LogicalBinaryExpr bin) {
            visitColumnReferences(bin.getLhs(), columns);
            visitColumnReferences(bin.getRhs(), columns);
        }
        if (expr instanceof LogicalAggregateExpr agg) {
            visitColumnReferences(agg.getExpr(), columns);
        }
    }

    private ArrowType arrowTypeFromString(String id) {
        switch (id) {
        case "DOUBLE", "double":
            return ArrowTypes.DoubleType;
        case "BOOL", "bool":
            return ArrowTypes.BooleanType;
        case "FLOAT", "float":
            return ArrowTypes.FloatType;
        case "INT64", "int64":
            return ArrowTypes.Int64Type;
        default:
            break;
        }

        throw new RuntimeException("Unknown datatype :" + id);
    }

}
