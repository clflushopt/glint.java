package co.clflushopt.glint.query.plan.logical;

import co.clflushopt.glint.types.Field;

/**
 * Aliased expression e.g `expr AS alias`.
 *
 * AliasExpr
 */
public class AliasExpr implements LogicalExpr {
    private String alias;
    private LogicalExpr expr;

    public AliasExpr(LogicalExpr expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(expr.toField(plan).name(), expr.toField(plan).dataType());
    }

    @Override
    public String toString() {
        return String.format("%s AS %s", this.expr, this.alias);
    }

}
