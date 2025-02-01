package co.clflushopt.glint.query.physical.expr;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.types.ArrowFieldVector;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.RecordBatch;

/**
 * Base class for boolean expression evaluation in the physical plan.
 */
public abstract class BooleanExpr implements Expr {
    protected final Expr left;
    protected final Expr right;

    protected BooleanExpr(Expr left, Expr right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public ColumnVector eval(RecordBatch input) {
        ColumnVector ll = left.eval(input);
        ColumnVector rr = right.eval(input);

        if (ll.getSize() != rr.getSize()) {
            throw new IllegalStateException("Size mismatch in boolean expression");
        }

        if (!ll.getType().equals(rr.getType())) {
            throw new IllegalStateException(
                    String.format("Cannot compare values of different type: %s != %s", ll.getType(),
                            rr.getType()));
        }

        return compare(ll, rr);
    }

    protected ColumnVector compare(ColumnVector left, ColumnVector right) {
        BitVector vector = new BitVector("v", new RootAllocator(Long.MAX_VALUE));
        vector.allocateNew();

        for (int i = 0; i < left.getSize(); i++) {
            boolean value = evaluate(left.getValue(i), right.getValue(i), left.getType());
            vector.set(i, value ? 1 : 0);
        }

        vector.setValueCount(left.getSize());
        return new ArrowFieldVector(vector);
    }

    protected abstract boolean evaluate(Object left, Object right, ArrowType type);

    private static boolean toBool(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() == 1;
        }
        throw new IllegalStateException("Cannot convert to boolean: " + value);
    }

    public static class AndExpression extends BooleanExpr {
        public AndExpression(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        protected boolean evaluate(Object left, Object right, ArrowType type) {
            return toBool(left) && toBool(right);
        }
    }

    public static class OrExpression extends BooleanExpr {
        public OrExpression(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        protected boolean evaluate(Object left, Object right, ArrowType type) {
            return toBool(left) || toBool(right);
        }
    }

    public static class EqExpression extends BooleanExpr {
        public EqExpression(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        protected boolean evaluate(Object left, Object right, ArrowType type) {
            if (type.equals(ArrowTypes.Int8Type)) {
                return ((Byte) left).equals((Byte) right);
            } else if (type.equals(ArrowTypes.Int16Type)) {
                return ((Short) left).equals((Short) right);
            } else if (type.equals(ArrowTypes.Int32Type)) {
                return ((Integer) left).equals((Integer) right);
            } else if (type.equals(ArrowTypes.Int64Type)) {
                return ((Long) left).equals((Long) right);
            } else if (type.equals(ArrowTypes.FloatType)) {
                return ((Float) left).equals((Float) right);
            } else if (type.equals(ArrowTypes.DoubleType)) {
                return ((Double) left).equals((Double) right);
            } else if (type.equals(ArrowTypes.StringType)) {
                return left.toString().equals(right.toString());
            }
            throw new IllegalStateException(
                    "Unsupported data type in comparison expression: " + type);
        }
    }

    public static class NeqExpression extends BooleanExpr {
        public NeqExpression(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        protected boolean evaluate(Object left, Object right, ArrowType type) {
            if (type.equals(ArrowTypes.Int8Type)) {
                return !((Byte) left).equals((Byte) right);
            } else if (type.equals(ArrowTypes.Int16Type)) {
                return !((Short) left).equals((Short) right);
            } else if (type.equals(ArrowTypes.Int32Type)) {
                return !((Integer) left).equals((Integer) right);
            } else if (type.equals(ArrowTypes.Int64Type)) {
                return !((Long) left).equals((Long) right);
            } else if (type.equals(ArrowTypes.FloatType)) {
                return !((Float) left).equals((Float) right);
            } else if (type.equals(ArrowTypes.DoubleType)) {
                return !((Double) left).equals((Double) right);
            } else if (type.equals(ArrowTypes.StringType)) {
                return !left.toString().equals(right.toString());
            }
            throw new IllegalStateException(
                    "Unsupported data type in comparison expression: " + type);
        }
    }

    public static class LtExpression extends BooleanExpr {
        public LtExpression(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        protected boolean evaluate(Object left, Object right, ArrowType type) {
            if (type.equals(ArrowTypes.Int8Type)) {
                return (Byte) left < (Byte) right;
            } else if (type.equals(ArrowTypes.Int16Type)) {
                return (Short) left < (Short) right;
            } else if (type.equals(ArrowTypes.Int32Type)) {
                return (Integer) left < (Integer) right;
            } else if (type.equals(ArrowTypes.Int64Type)) {
                return (Long) left < (Long) right;
            } else if (type.equals(ArrowTypes.FloatType)) {
                return (Float) left < (Float) right;
            } else if (type.equals(ArrowTypes.DoubleType)) {
                return (Double) left < (Double) right;
            } else if (type.equals(ArrowTypes.StringType)) {
                return left.toString().compareTo(right.toString()) < 0;
            }
            throw new IllegalStateException(
                    "Unsupported data type in comparison expression: " + type);
        }
    }

    public static class GtExpression extends BooleanExpr {
        public GtExpression(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        protected boolean evaluate(Object left, Object right, ArrowType type) {
            if (type.equals(ArrowTypes.Int8Type)) {
                return (Byte) left > (Byte) right;
            } else if (type.equals(ArrowTypes.Int16Type)) {
                return (Short) left > (Short) right;
            } else if (type.equals(ArrowTypes.Int32Type)) {
                return (Integer) left > (Integer) right;
            } else if (type.equals(ArrowTypes.Int64Type)) {
                return (Long) left > (Long) right;
            } else if (type.equals(ArrowTypes.FloatType)) {
                return (Float) left > (Float) right;
            } else if (type.equals(ArrowTypes.DoubleType)) {
                return (Double) left > (Double) right;
            } else if (type.equals(ArrowTypes.StringType)) {
                return left.toString().compareTo(right.toString()) > 0;
            }
            throw new IllegalStateException(
                    "Unsupported data type in comparison expression: " + type);
        }
    }

    public static class LteExpression extends BooleanExpr {
        public LteExpression(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        protected boolean evaluate(Object left, Object right, ArrowType type) {
            if (type.equals(ArrowTypes.Int8Type)) {
                return (Byte) left <= (Byte) right;
            } else if (type.equals(ArrowTypes.Int16Type)) {
                return (Short) left <= (Short) right;
            } else if (type.equals(ArrowTypes.Int32Type)) {
                return (Integer) left <= (Integer) right;
            } else if (type.equals(ArrowTypes.Int64Type)) {
                return (Long) left <= (Long) right;
            } else if (type.equals(ArrowTypes.FloatType)) {
                return (Float) left <= (Float) right;
            } else if (type.equals(ArrowTypes.DoubleType)) {
                return (Double) left <= (Double) right;
            } else if (type.equals(ArrowTypes.StringType)) {
                return left.toString().compareTo(right.toString()) <= 0;
            }

            throw new IllegalStateException(
                    "Unsupported data type in comparison expression: " + type);

        }
    }

    public static class GteExpression extends BooleanExpr {
        public GteExpression(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        protected boolean evaluate(Object left, Object right, ArrowType type) {
            if (type.equals(ArrowTypes.Int8Type)) {
                return (Byte) left >= (Byte) right;
            } else if (type.equals(ArrowTypes.Int16Type)) {
                return (Short) left >= (Short) right;
            } else if (type.equals(ArrowTypes.Int32Type)) {
                return (Integer) left >= (Integer) right;
            } else if (type.equals(ArrowTypes.Int64Type)) {
                return (Long) left >= (Long) right;
            } else if (type.equals(ArrowTypes.FloatType)) {
                return (Float) left >= (Float) right;
            } else if (type.equals(ArrowTypes.DoubleType)) {
                return (Double) left >= (Double) right;
            } else if (type.equals(ArrowTypes.StringType)) {
                return left.toString().compareTo(right.toString()) >= 0;
            }
            throw new IllegalStateException(
                    "Unsupported data type in comparison expression: " + type);

        }
    }
}