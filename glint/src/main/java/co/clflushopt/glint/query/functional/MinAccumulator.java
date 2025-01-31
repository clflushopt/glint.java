package co.clflushopt.glint.query.functional;

/**
 * Implementation of an accumulator that accumulates the minimum value.
 *
 * MinAccumulator
 */
public class MinAccumulator implements Accumulator {
    private Object value;

    /**
     * Create a new MinAccumulator.
     *
     */
    public MinAccumulator() {
        this.value = null;
    }

    /**
     * Accumulate a new value.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void accumulate(Object value) {
        if (value != null) {
            if (this.value == null || ((Comparable<Object>) value).compareTo(this.value) > 0) {
                this.value = value;
            } else {
                if (value instanceof Double) {
                    this.value = Math.min((Double) this.value, (Double) value);
                } else if (value instanceof Long) {
                    this.value = Math.min((Long) this.value, (Long) value);
                } else if (value instanceof Integer) {
                    this.value = Math.min((Integer) this.value, (Integer) value);
                } else if (value instanceof Float) {
                    this.value = Math.min((Float) this.value, (Float) value);
                } else if (value instanceof Short) {
                    this.value = Math.min((Short) this.value, (Short) value);
                } else if (value instanceof Byte) {
                    this.value = Math.min((Byte) this.value, (Byte) value);
                } else if (value instanceof String) {
                    this.value = ((String) this.value).compareTo((String) value) < 0 ? this.value
                            : value;
                } else {
                    throw new UnsupportedOperationException("Unsupported type for MinAccumulator");
                }
            }
        }

    }

    /**
     * Returns the result of the accumulation.
     *
     */
    @Override
    public Object getResult() {
        return value;
    }

}
