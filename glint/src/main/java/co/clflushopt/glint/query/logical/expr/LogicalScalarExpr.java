package co.clflushopt.glint.query.logical.expr;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.Field;

/**
 * Scalar functions that are callable and return a value.
 *
 * ScalarExpr
 */
public class LogicalScalarExpr implements LogicalExpr {
    private String name;
    private List<LogicalExpr> args;
    private ArrowType returnType;

    public LogicalScalarExpr(String name, List<LogicalExpr> args, ArrowType returnType) {
        this.name = name;
        this.args = args;
        this.returnType = returnType;
    }

    public List<LogicalExpr> getArgs() {
        return args;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(name, returnType);
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", name, args);
    }

}
