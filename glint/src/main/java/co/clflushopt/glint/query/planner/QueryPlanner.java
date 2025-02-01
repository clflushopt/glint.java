package co.clflushopt.glint.query.planner;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import co.clflushopt.glint.query.logical.expr.AggregateExpr.Max;
import co.clflushopt.glint.query.logical.expr.AggregateExpr.Min;
import co.clflushopt.glint.query.logical.expr.AggregateExpr.Sum;
import co.clflushopt.glint.query.logical.expr.AliasExpr;
import co.clflushopt.glint.query.logical.expr.BinaryExpr;
import co.clflushopt.glint.query.logical.expr.CastExpr;
import co.clflushopt.glint.query.logical.expr.ColumnExpr;
import co.clflushopt.glint.query.logical.expr.ColumnIndex;
import co.clflushopt.glint.query.logical.expr.LiteralDouble;
import co.clflushopt.glint.query.logical.expr.LiteralLong;
import co.clflushopt.glint.query.logical.expr.LiteralString;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.Aggregate;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Projection;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.query.logical.plan.Selection;
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
        } else if (plan instanceof Selection) {
            Selection selection = (Selection) plan;
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
            return new HashAggregateOperator(input, groupExpr, aggregateExpr, agg.getSchema());
        }
        throw new IllegalStateException("Unsupported logical plan: " + plan.getClass());
    }

    /**
     * Create a physical expression from a logical expression.
     */
    public static PhysicalExpr createPhysicalExpr(LogicalExpr expr, LogicalPlan input) {
        if (expr instanceof LiteralLong) {
            return new LiteralLongExpr(((LiteralLong) expr).getValue());
        } else if (expr instanceof LiteralDouble) {
            return new LiteralDoubleExpr(((LiteralDouble) expr).getValue());
        } else if (expr instanceof LiteralString) {
            return new LiteralStringExpr(((LiteralString) expr).getValue());
        } else if (expr instanceof ColumnIndex) {
            return new PhysicalColumnExpr(((ColumnIndex) expr).getIndex());
        } else if (expr instanceof AliasExpr) {
            // note that there is no physical expression for an alias since the alias
            // only affects the name used in the planning phase and not how the aliased
            // expression is executed
            return createPhysicalExpr(((AliasExpr) expr).getExpr(), input);
        } else if (expr instanceof ColumnExpr) {
            ColumnExpr column = (ColumnExpr) expr;
            int i = IntStream.range(0, input.getSchema().getFields().size()).filter(
                    idx -> input.getSchema().getFields().get(idx).name().equals(column.getName()))
                    .findFirst().getAsInt();
            return new PhysicalColumnExpr(i);
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            return new PhysicalCastExpr(createPhysicalExpr(cast.getExpr(), input),
                    cast.getDataType());
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr binary = (BinaryExpr) expr;
            PhysicalExpr l = createPhysicalExpr(binary.getLhs(), input);
            PhysicalExpr r = createPhysicalExpr(binary.getRhs(), input);

            if (binary.getOperator() == "eq") {
                return new EqExpression(l, r);
            }
            if (binary.getOperator() == "neq") {
                return new NeqExpression(l, r);
            }
            if (binary.getOperator() == "gt") {
                return new GtExpression(l, r);
            }
            if (binary.getOperator() == "gte") {
                return new GteExpression(l, r);
            }
            if (binary.getOperator() == "lt") {
                return new LtExpression(l, r);
            }
            if (binary.getOperator() == "lte") {
                return new LteExpression(l, r);
            }
            if (binary.getOperator() == "and") {
                return new AndExpression(l, r);
            }
            if (binary.getOperator() == "or") {
                return new OrExpression(l, r);
            }
            // TODO: Add PhysicalMathExpr
            throw new IllegalStateException("Unsupported binary expression: " + expr);

        }
        throw new IllegalStateException("Unsupported logical expression: " + expr);
    }
}
