package co.clflushopt.glint.query.functional;

/**
 * Implements an accumulator that computes the sum of an expression.
 *
 * SumAccumulator
 */
public class SumAccumulator implements Accumulator {
    private Double sum = 0.0;

    /**
     * Create a new sum accumulator.
     *
     */
    public SumAccumulator() {
        this.sum = 0.0;
    }

    @Override
    public void accumulate(Object value) {
        if (value != null) {
            if (value instanceof Double) {
                sum += (Double) value;
            } else if (value instanceof Long) {
                sum += (Long) value;
            } else if (value instanceof Integer) {
                sum += (Integer) value;
            } else if (value instanceof Float) {
                sum += (Float) value;
            } else if (value instanceof Short) {
                sum += (Short) value;
            } else if (value instanceof Byte) {
                sum += (Byte) value;
            } else {
                throw new UnsupportedOperationException("Unsupported type for SumAccumulator");
            }
        }
    }

    @Override
    public Object getResult() {
        return sum;
    }

}
