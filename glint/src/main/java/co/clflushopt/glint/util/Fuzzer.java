package co.clflushopt.glint.util;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.types.ArrowFieldVector;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class Fuzzer {
    private final Random rand;
    private final EnhancedRandom enhancedRandom;

    public Fuzzer() {
        this.rand = new Random(0);
        this.enhancedRandom = new EnhancedRandom(rand);
    }

    /**
     * Create a list of random values based on the provided data type
     */
    public List<Object> createValues(ArrowType arrowType, int n) {
        if (arrowType.equals(ArrowTypes.Int8Type)) {
            return IntStream.range(0, n).mapToObj(i -> enhancedRandom.nextByte())
                    .collect(Collectors.toList());
        } else if (arrowType.equals(ArrowTypes.Int16Type)) {
            return IntStream.range(0, n).mapToObj(i -> enhancedRandom.nextShort())
                    .collect(Collectors.toList());
        } else if (arrowType.equals(ArrowTypes.Int32Type)) {
            return IntStream.range(0, n).mapToObj(i -> enhancedRandom.nextInt())
                    .collect(Collectors.toList());
        } else if (arrowType.equals(ArrowTypes.Int64Type)) {
            return IntStream.range(0, n).mapToObj(i -> enhancedRandom.nextLong())
                    .collect(Collectors.toList());
        } else if (arrowType.equals(ArrowTypes.FloatType)) {
            return IntStream.range(0, n).mapToObj(i -> enhancedRandom.nextFloat())
                    .collect(Collectors.toList());
        } else if (arrowType.equals(ArrowTypes.DoubleType)) {
            return IntStream.range(0, n).mapToObj(i -> enhancedRandom.nextDouble())
                    .collect(Collectors.toList());
        } else if (arrowType.equals(ArrowTypes.StringType)) {
            return IntStream.range(0, n).mapToObj(i -> enhancedRandom.nextString(rand.nextInt(64)))
                    .collect(Collectors.toList());
        } else {
            throw new IllegalStateException("Unsupported type: " + arrowType);
        }
    }

    /**
     * Create a RecordBatch containing the specified values.
     */
    public RecordBatch createRecordBatch(Schema schema, List<List<Object>> columns) {
        int rowCount = columns.get(0).size();

        VectorSchemaRoot root = VectorSchemaRoot.create(schema.toArrow(),
                new RootAllocator(Long.MAX_VALUE));
        root.allocateNew();

        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < columns.size(); col++) {
                ValueVector v = root.getVector(col);
                Object value = columns.get(col).get(row);

                if (v instanceof BitVector) {
                    ((BitVector) v).set(row, (Boolean) value ? 1 : 0);
                } else if (v instanceof TinyIntVector) {
                    ((TinyIntVector) v).set(row, (Byte) value);
                } else if (v instanceof SmallIntVector) {
                    ((SmallIntVector) v).set(row, (Short) value);
                } else if (v instanceof IntVector) {
                    ((IntVector) v).set(row, (Integer) value);
                } else if (v instanceof BigIntVector) {
                    ((BigIntVector) v).set(row, (Long) value);
                } else if (v instanceof Float4Vector) {
                    ((Float4Vector) v).set(row, (Float) value);
                } else if (v instanceof Float8Vector) {
                    ((Float8Vector) v).set(row, (Double) value);
                } else if (v instanceof VarCharVector) {
                    ((VarCharVector) v).set(row, ((String) value).getBytes());
                } else {
                    throw new IllegalStateException(
                            "Unsupported vector type: " + v.getClass().getName());
                }
            }
        }
        root.setRowCount(rowCount);

        return new RecordBatch(schema, root.getFieldVectors().stream().map(ArrowFieldVector::new)
                .collect(Collectors.toList()));
    }

    /**
     * Helper class for generating enhanced random values
     */
    private static class EnhancedRandom {
        private final Random rand;
        private final String charPool;

        public EnhancedRandom(Random rand) {
            this.rand = rand;
            StringBuilder pool = new StringBuilder();
            for (char c = 'a'; c <= 'z'; c++)
                pool.append(c);
            for (char c = 'A'; c <= 'Z'; c++)
                pool.append(c);
            for (char c = '0'; c <= '9'; c++)
                pool.append(c);
            this.charPool = pool.toString();
        }

        public byte nextByte() {
            switch (rand.nextInt(5)) {
            case 0:
                return Byte.MIN_VALUE;
            case 1:
                return Byte.MAX_VALUE;
            case 2:
                return (byte) -0;
            case 3:
                return 0;
            case 4:
                return (byte) rand.nextInt();
            default:
                throw new IllegalStateException();
            }
        }

        public short nextShort() {
            switch (rand.nextInt(5)) {
            case 0:
                return Short.MIN_VALUE;
            case 1:
                return Short.MAX_VALUE;
            case 2:
                return (short) -0;
            case 3:
                return 0;
            case 4:
                return (short) rand.nextInt();
            default:
                throw new IllegalStateException();
            }
        }

        public int nextInt() {
            switch (rand.nextInt(5)) {
            case 0:
                return Integer.MIN_VALUE;
            case 1:
                return Integer.MAX_VALUE;
            case 2:
                return -0;
            case 3:
                return 0;
            case 4:
                return rand.nextInt();
            default:
                throw new IllegalStateException();
            }
        }

        public long nextLong() {
            switch (rand.nextInt(5)) {
            case 0:
                return Long.MIN_VALUE;
            case 1:
                return Long.MAX_VALUE;
            case 2:
                return -0L;
            case 3:
                return 0L;
            case 4:
                return rand.nextLong();
            default:
                throw new IllegalStateException();
            }
        }

        public double nextDouble() {
            switch (rand.nextInt(8)) {
            case 0:
                return Double.MIN_VALUE;
            case 1:
                return Double.MAX_VALUE;
            case 2:
                return Double.POSITIVE_INFINITY;
            case 3:
                return Double.NEGATIVE_INFINITY;
            case 4:
                return Double.NaN;
            case 5:
                return -0.0;
            case 6:
                return 0.0;
            case 7:
                return rand.nextDouble();
            default:
                throw new IllegalStateException();
            }
        }

        public float nextFloat() {
            switch (rand.nextInt(8)) {
            case 0:
                return Float.MIN_VALUE;
            case 1:
                return Float.MAX_VALUE;
            case 2:
                return Float.POSITIVE_INFINITY;
            case 3:
                return Float.NEGATIVE_INFINITY;
            case 4:
                return Float.NaN;
            case 5:
                return -0.0f;
            case 6:
                return 0.0f;
            case 7:
                return rand.nextFloat();
            default:
                throw new IllegalStateException();
            }
        }

        public String nextString(int len) {
            return rand.ints(len, 0, charPool.length())
                    .mapToObj(i -> String.valueOf(charPool.charAt(i)))
                    .collect(Collectors.joining());
        }
    }
}