package co.clflushopt.glint.query.plan.logical;

import co.clflushopt.glint.types.Field;

/**
 * LogicalExpr is used to represent expressions such as column projection or
 * binary expressions and such.
 *
 */
public interface LogicalExpr {
    /**
     * Returns the field metadata that the logical expression will return.
     *
     * For example `LiteralString` will return a `Field` with `ArrowTypes.String`.
     *
     * @param plan
     * @return
     */
    public Field toField(LogicalPlan plan);
}
