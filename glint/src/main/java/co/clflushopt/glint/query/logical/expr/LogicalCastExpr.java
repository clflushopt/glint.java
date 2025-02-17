package co.clflushopt.glint.query.logical.expr;

import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Cast expression represents explicit type casts `CAST(i AS TYPE)`.
 *
 * CastExpr
 */
public class LogicalCastExpr implements LogicalExpr {
    private LogicalExpr expr;
    private ArrowType dataType;

    public LogicalCastExpr(LogicalExpr expr, ArrowType dataType) {
        this.expr = expr;
        this.dataType = dataType;
    }

    public LogicalExpr getExpr() {
        return this.expr;
    }

    public ArrowType getDataType() {
        return this.dataType;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(expr.toField(plan).name(), dataType);
    }

    @Override
    public String toString() {
        return String.format("CAST(%s AS %s)", this.expr, ArrowTypes.toString(dataType));
    }

}
