package co.clflushopt.glint.util;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class IndexedStream {
    public static class IndexedValue<T> {
        private final int index;
        private final T value;

        public IndexedValue(int index, T value) {
            this.index = index;
            this.value = value;
        }

        /**
         * Get the indexed value's index.
         * 
         * @return `int` the index assigned to this value when constructed.
         */
        public int getIndex() {
            return index;
        }

        /**
         * Get the value being indexed.
         * 
         * @return
         */
        public T getValue() {
            return value;
        }
    }

    /**
     * `withIndex` allows you to create an indexed stream that can be composed with
     * further methods like `map` or `forEach`.
     * 
     * @param <T>
     * @param list
     * @return Stream of indexed values.
     */
    public static <T> Stream<IndexedValue<T>> withIndex(List<T> list) {
        return IntStream.range(0, list.size()).mapToObj(i -> new IndexedValue<>(i, list.get(i)));
    }

}
