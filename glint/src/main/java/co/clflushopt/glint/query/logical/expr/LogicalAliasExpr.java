package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.Field;

/**
 * Aliased expression e.g `expr AS alias`.
 *
 * AliasExpr
 */
public class LogicalAliasExpr implements LogicalExpr {
    private String alias;
    private LogicalExpr expr;

    public LogicalAliasExpr(LogicalExpr expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    public LogicalExpr getExpr() {
        return this.expr;
    }

    public String getAlias() {
        return this.alias;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(alias, expr.toField(plan).dataType());
    }

    @Override
    public String toString() {
        return String.format("%s AS %s", this.expr, this.alias);
    }

}
