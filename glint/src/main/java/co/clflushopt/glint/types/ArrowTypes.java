package co.clflushopt.glint.types;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Wrapper around `Arrow.ArrowTypes` used to set the supported data types for
 * the columnar format.
 */
public final class ArrowTypes {
    /**
     * Boolean data type.
     */
    public static final ArrowType BooleanType = new ArrowType.Bool();

    /**
     * Signed integers.
     */
    public static final ArrowType Int8Type = new ArrowType.Int(8, true);
    public static final ArrowType Int16Type = new ArrowType.Int(16, true);
    public static final ArrowType Int32Type = new ArrowType.Int(32, true);
    public static final ArrowType Int64Type = new ArrowType.Int(64, true);

    /**
     * Unsigned integers.
     */
    public static final ArrowType UInt8Type = new ArrowType.Int(8, false);
    public static final ArrowType UInt16Type = new ArrowType.Int(16, false);
    public static final ArrowType UInt32Type = new ArrowType.Int(32, false);
    public static final ArrowType UInt64Type = new ArrowType.Int(64, false);

    /**
     * Floating point numbers.
     */
    public static final ArrowType FloatType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    public static final ArrowType DoubleType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    /**
     * Strings in UTF-8 encoding.
     */
    public static final ArrowType StringType = new ArrowType.Utf8();
}
