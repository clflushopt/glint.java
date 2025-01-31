package co.clflushopt.glint.query.physical.expr;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.ArrowVectorBuilder;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.FieldVectorFactory;
import co.clflushopt.glint.types.RecordBatch;

/**
 * Implementation of the `CAST` expression.
 *
 * CastExpr
 */
public class CastExpr implements Expr {
    private Expr expression;
    private ArrowType type;

    /**
     * Create a new `CAST` expression.
     *
     * @param expression the expression to cast.
     */
    public CastExpr(Expr expression, ArrowType type) {
        this.expression = expression;
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("CAST(%s AS %s)", expression, type);
    }

    @Override
    public ColumnVector eval(RecordBatch input) {
        ColumnVector value = expression.eval(input);
        FieldVector fieldVector = FieldVectorFactory.create(type, input.getRowSize());
        ArrowVectorBuilder builder = new ArrowVectorBuilder(fieldVector);

        if (type.equals(ArrowTypes.Int8Type)) {
            for (int i = 0; i < value.getSize(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.setValue(i, value);
                } else {
                    byte castValue;
                    if (vv instanceof byte[]) {
                        castValue = Byte.parseByte(new String((byte[]) vv));
                    } else if (vv instanceof String) {
                        castValue = Byte.parseByte((String) vv);
                    } else if (vv instanceof Number) {
                        castValue = ((Number) vv).byteValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Byte: " + vv);
                    }
                    builder.setValue(i, castValue);
                }
            }
        } else if (type.equals(ArrowTypes.Int16Type)) {
            for (int i = 0; i < value.getSize(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.setValue(i, null);
                } else {
                    short castValue;
                    if (vv instanceof byte[]) {
                        castValue = Short.parseShort(new String((byte[]) vv));
                    } else if (vv instanceof String) {
                        castValue = Short.parseShort((String) vv);
                    } else if (vv instanceof Number) {
                        castValue = ((Number) vv).shortValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Short: " + vv);
                    }
                    builder.setValue(i, castValue);
                }
            }
        } else if (type.equals(ArrowTypes.Int32Type)) {
            for (int i = 0; i < value.getSize(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.setValue(i, null);
                } else {
                    int castValue;
                    if (vv instanceof byte[]) {
                        castValue = Integer.parseInt(new String((byte[]) vv));
                    } else if (vv instanceof String) {
                        castValue = Integer.parseInt((String) vv);
                    } else if (vv instanceof Number) {
                        castValue = ((Number) vv).intValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Integer: " + vv);
                    }
                    builder.setValue(i, castValue);
                }
            }
        } else if (type.equals(ArrowTypes.Int64Type)) {
            for (int i = 0; i < value.getSize(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.setValue(i, null);
                } else {
                    long castValue;
                    if (vv instanceof byte[]) {
                        castValue = Long.parseLong(new String((byte[]) vv));
                    } else if (vv instanceof String) {
                        castValue = Long.parseLong((String) vv);
                    } else if (vv instanceof Number) {
                        castValue = ((Number) vv).longValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Long: " + vv);
                    }
                    builder.setValue(i, castValue);
                }
            }
        } else if (type.equals(ArrowTypes.FloatType)) {
            for (int i = 0; i < value.getSize(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.setValue(i, null);
                } else {
                    float castValue;
                    if (vv instanceof byte[]) {
                        castValue = Float.parseFloat(new String((byte[]) vv));
                    } else if (vv instanceof String) {
                        castValue = Float.parseFloat((String) vv);
                    } else if (vv instanceof Number) {
                        castValue = ((Number) vv).floatValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Float: " + vv);
                    }
                    builder.setValue(i, castValue);
                }
            }
        } else if (type.equals(ArrowTypes.DoubleType)) {
            for (int i = 0; i < value.getSize(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.setValue(i, null);
                } else {
                    double castValue;
                    if (vv instanceof byte[]) {
                        castValue = Double.parseDouble(new String((byte[]) vv));
                    } else if (vv instanceof String) {
                        castValue = Double.parseDouble((String) vv);
                    } else if (vv instanceof Number) {
                        castValue = ((Number) vv).doubleValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Double: " + vv);
                    }
                    builder.setValue(i, castValue);
                }
            }
        } else if (type.equals(ArrowTypes.StringType)) {
            for (int i = 0; i < value.getSize(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.setValue(i, null);
                } else {
                    builder.setValue(i, vv.toString());
                }
            }
        } else {
            throw new IllegalStateException("Cast to " + type + " is not supported");
        }

        builder.setValueCount(value.getSize());
        return builder.build();
    }

}
