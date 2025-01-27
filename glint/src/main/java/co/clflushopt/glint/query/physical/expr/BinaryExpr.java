package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.RecordBatch;

/**
 * Binary expressions, evaluation of any binary expression requires type
 * checking so `eval` method is overridden to typecheck in the abstract class
 * and overridden to do the actual evaluation in the concrete classes.
 *
 * BinaryExpr
 */
public abstract class BinaryExpr implements Expr {
    protected Expr left;
    protected Expr right;

    public BinaryExpr(Expr left, Expr right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public ColumnVector eval(RecordBatch input) {
        ColumnVector leftVector = left.eval(input);
        ColumnVector rightVector = right.eval(input);
        if (leftVector.getType() != rightVector.getType()) {
            throw new IllegalArgumentException("Type mismatch in binary expression");
        }
        return eval(leftVector, rightVector);
    }

    protected abstract ColumnVector eval(ColumnVector left, ColumnVector right);
}
