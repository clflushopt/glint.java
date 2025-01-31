package co.clflushopt.glint.query.functional;

/**
 * Accumaulator interface defines a function that accumulates values streamed to
 * it.
 *
 * Accumulator
 */
public interface Accumulator {

    /**
     * Accumulate the value.
     *
     * @param value the value to accumulate.
     */
    public void accumulate(Object value);

    /**
     * Get the result of the accumulation.
     *
     * @return the result of the accumulation.
     */
    public Object getResult();

}
