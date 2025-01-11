package co.clflushopt.glint.storage;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

/**
 * Unit tests for the main memory storage backed.
 */
public class MemoryStoreTest {
    /**
     * Ensure that we can read and write values.
     */
    @Test
    public void canPutandGetValues() {

        var store = new MemoryStore();

        List<String> keys = Lists.newArrayList(
                "Alice",
                "Bob",
                "Eve",
                "Mallory");

        List<String> values = Lists.newArrayList(
                "likes to write compilers but dislikes LLVM",
                "doesn't like compilers but likes PLT",
                "can only write ANSI SQL",
                "tends to be terrible at partitioning without hints");

        values.forEach((value -> System.out.println(value.getBytes(StandardCharsets.UTF_8))));

        Streams.forEachPair(keys.stream(), values.stream(),
                (key, value) -> store.put(key, value));

        assertEquals(store.size(), 4);

        var expected = keys.stream()
                .map(key -> new String(store.get(key)))
                .collect(Collectors.toList());

        // Assert equality
        assertEquals(values, expected);
    }

}
