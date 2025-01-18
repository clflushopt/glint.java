package co.clflushopt.glint.types;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

// Primitive types as native wrappers around `ArrowType`.
public final class ArrowTypes {
    // Boolean type.
    static final ArrowType BooleanType = new ArrowType.Bool();

    // Signed integers types.
    static final ArrowType Int8Type = new ArrowType.Int(8, true);
    static final ArrowType Int16Type = new ArrowType.Int(16, true);
    static final ArrowType Int32Type = new ArrowType.Int(32, true);
    static final ArrowType Int64Type = new ArrowType.Int(64, true);

    // Unsigned integers types.
    static final ArrowType UInt8Type = new ArrowType.Int(8, false);
    static final ArrowType UInt16Type = new ArrowType.Int(16, false);
    static final ArrowType UInt32Type = new ArrowType.Int(32, false);
    static final ArrowType UInt64Type = new ArrowType.Int(64, false);

    // Floating point numerics types.
    static final ArrowType FloatType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    static final ArrowType DoubleType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    // UTF-8 strings.
    static final ArrowType StringType = new ArrowType.Utf8();
}
