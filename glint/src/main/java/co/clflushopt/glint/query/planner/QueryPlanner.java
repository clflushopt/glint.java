package co.clflushopt.glint.query.planner;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr.Max;
import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr.Min;
import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr.Sum;
import co.clflushopt.glint.query.logical.expr.LogicalAliasExpr;
import co.clflushopt.glint.query.logical.expr.LogicalBinaryExpr;
import co.clflushopt.glint.query.logical.expr.LogicalCastExpr;
import co.clflushopt.glint.query.logical.expr.LogicalColumnExpr;
import co.clflushopt.glint.query.logical.expr.LogicalColumnIndex;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.expr.LogicalLiteralDouble;
import co.clflushopt.glint.query.logical.expr.LogicalLiteralLong;
import co.clflushopt.glint.query.logical.expr.LogicalLiteralString;
import co.clflushopt.glint.query.logical.plan.Aggregate;
import co.clflushopt.glint.query.logical.plan.Filter;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Projection;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.query.physical.expr.LiteralDoubleExpr;
import co.clflushopt.glint.query.physical.expr.LiteralLongExpr;
import co.clflushopt.glint.query.physical.expr.LiteralStringExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalAggregateExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr.AndExpression;
import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr.EqExpression;
import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr.GtExpression;
import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr.GteExpression;
import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr.LtExpression;
import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr.LteExpression;
import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr.NeqExpression;
import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr.OrExpression;
import co.clflushopt.glint.query.physical.expr.PhysicalCastExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalColumnExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalMaxExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalMinExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalSumExpr;
import co.clflushopt.glint.query.physical.plan.FilterOperator;
import co.clflushopt.glint.query.physical.plan.HashAggregateOperator;
import co.clflushopt.glint.query.physical.plan.PhysicalPlan;
import co.clflushopt.glint.query.physical.plan.ProjectionOperator;
import co.clflushopt.glint.query.physical.plan.ScanOperator;
import co.clflushopt.glint.types.Schema;

/**
 * QueryPlanner is the entity that takes a logical plan and converts it into a
 * physical plan.
 *
 * QueryPlanner
 */
public class QueryPlanner {

    /**
     * Create a physical plan from a logical plan.
     */
    public static PhysicalPlan createPhysicalPlan(LogicalPlan plan) {
        if (plan instanceof Scan) {
            Scan scan = (Scan) plan;
            return new ScanOperator(scan.getDataSource(), scan.getProjections());
        } else if (plan instanceof Filter) {
            Filter selection = (Filter) plan;
            PhysicalPlan input = createPhysicalPlan(selection.getInput());
            PhysicalExpr filterExpr = createPhysicalExpr(selection.getExpr(), selection.getInput());
            return new FilterOperator(input, filterExpr);
        } else if (plan instanceof Projection) {
            Projection projection = (Projection) plan;
            PhysicalPlan input = createPhysicalPlan(projection.getInput());
            List<PhysicalExpr> projectionExpr = projection.getExpr().stream()
                    .map(expr -> createPhysicalExpr(expr, projection.getInput()))
                    .collect(Collectors.toList());
            Schema projectionSchema = new Schema(projection.getExpr().stream()
                    .map(expr -> expr.toField(projection.getInput())).collect(Collectors.toList()));
            return new ProjectionOperator(input, projectionSchema, projectionExpr);
        } else if (plan instanceof Aggregate) {
            Aggregate agg = (Aggregate) plan;
            PhysicalPlan input = createPhysicalPlan(agg.getInput());
            List<PhysicalExpr> groupExpr = agg.getGroupExpr().stream()
                    .map(expr -> createPhysicalExpr(expr, agg.getInput()))
                    .collect(Collectors.toList());
            List<PhysicalAggregateExpr> aggregateExpr = agg.getAggregateExpr().stream()
                    .map(expr -> {
                        if (expr instanceof Max) {
                            return new PhysicalMaxExpr(
                                    createPhysicalExpr(((Max) expr).getExpr(), agg.getInput()));
                        } else if (expr instanceof Min) {
                            return new PhysicalMinExpr(
                                    createPhysicalExpr(((Min) expr).getExpr(), agg.getInput()));
                        } else if (expr instanceof Sum) {
                            return new PhysicalSumExpr(
                                    createPhysicalExpr(((Sum) expr).getExpr(), agg.getInput()));
                        } else {
                            throw new IllegalStateException(
                                    "Unsupported aggregate function: " + expr);
                        }
                    }).collect(Collectors.toList());
            return new HashAggregateOperator(input, groupExpr, aggregateExpr, plan.getSchema());
        }
        throw new IllegalStateException("Unsupported logical plan: " + plan.getClass());
    }

    /**
     * Create a physical expression from a logical expression.
     */
    public static PhysicalExpr createPhysicalExpr(LogicalExpr expr, LogicalPlan input) {
        if (expr instanceof LogicalLiteralLong) {
            return new LiteralLongExpr(((LogicalLiteralLong) expr).getValue());
        } else if (expr instanceof LogicalLiteralDouble) {
            return new LiteralDoubleExpr(((LogicalLiteralDouble) expr).getValue());
        } else if (expr instanceof LogicalLiteralString) {
            return new LiteralStringExpr(((LogicalLiteralString) expr).getValue());
        } else if (expr instanceof LogicalColumnIndex) {
            return new PhysicalColumnExpr(((LogicalColumnIndex) expr).getIndex());
        } else if (expr instanceof LogicalAliasExpr) {
            // note that there is no physical expression for an alias since the alias
            // only affects the name used in the planning phase and not how the aliased
            // expression is executed
            return createPhysicalExpr(((LogicalAliasExpr) expr).getExpr(), input);
        } else if (expr instanceof LogicalColumnExpr) {
            LogicalColumnExpr column = (LogicalColumnExpr) expr;
            int i = IntStream.range(0, input.getSchema().getFields().size()).filter(
                    idx -> input.getSchema().getFields().get(idx).name().equals(column.getName()))
                    .findFirst().getAsInt();
            return new PhysicalColumnExpr(i);
        } else if (expr instanceof LogicalCastExpr) {
            LogicalCastExpr cast = (LogicalCastExpr) expr;
            return new PhysicalCastExpr(createPhysicalExpr(cast.getExpr(), input),
                    cast.getDataType());
        } else if (expr instanceof LogicalBinaryExpr) {
            LogicalBinaryExpr binary = (LogicalBinaryExpr) expr;
            PhysicalExpr l = createPhysicalExpr(binary.getLhs(), input);
            PhysicalExpr r = createPhysicalExpr(binary.getRhs(), input);

            if (binary.getOperator().equals("=")) {
                return new EqExpression(l, r);
            }
            if (binary.getOperator().equals("!=")) {
                return new NeqExpression(l, r);
            }
            if (binary.getOperator().equals(">")) {
                return new GtExpression(l, r);
            }
            if (binary.getOperator().equals(">=")) {
                return new GteExpression(l, r);
            }
            if (binary.getOperator().equals("<")) {
                return new LtExpression(l, r);
            }
            if (binary.getOperator().equals("<=")) {
                return new LteExpression(l, r);
            }
            if (binary.getOperator().equals("and")) {
                return new AndExpression(l, r);
            }
            if (binary.getOperator().equals("or")) {
                return new OrExpression(l, r);
            }
            throw new IllegalStateException(
                    "Unsupported binary expression: " + binary + " " + binary.getOperator());

        }
        throw new IllegalStateException("Unsupported logical expression: " + expr);
    }
}
