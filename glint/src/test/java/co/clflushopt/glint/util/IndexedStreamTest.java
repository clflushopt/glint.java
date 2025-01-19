package co.clflushopt.glint.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

public class IndexedStreamTest {

    @Test
    public void withIndex_EmptyList_ReturnsEmptyStream() {
        List<String> input = List.of();
        List<IndexedStream.IndexedValue<String>> result = IndexedStream.withIndex(input)
                .collect(Collectors.toList());

        assertTrue(result.isEmpty());
    }

    @Test
    public void withIndex_SingleElement_ReturnsOneIndexedgetValue() {
        List<String> input = List.of("test");
        List<IndexedStream.IndexedValue<String>> result = IndexedStream.withIndex(input)
                .collect(Collectors.toList());

        assertEquals(1, result.size());
        assertEquals(0, result.get(0).getIndex());
        assertEquals("test", result.get(0).getValue());
    }

    @Test
    public void withIndex_MultipleElements_ReturnsCorrectIndexesAndValues() {
        List<String> input = Arrays.asList("a", "b", "c");
        List<IndexedStream.IndexedValue<String>> result = IndexedStream.withIndex(input)
                .collect(Collectors.toList());

        assertEquals(3, result.size());

        // Check first element
        assertEquals(0, result.get(0).getIndex());
        assertEquals("a", result.get(0).getValue());

        // Check middle element
        assertEquals(1, result.get(1).getIndex());
        assertEquals("b", result.get(1).getValue());

        // Check last element
        assertEquals(2, result.get(2).getIndex());
        assertEquals("c", result.get(2).getValue());
    }

    @Test
    public void withIndex_WorksWithDifferentTypes() {
        List<Integer> input = Arrays.asList(10, 20, 30);
        List<IndexedStream.IndexedValue<Integer>> result = IndexedStream.withIndex(input)
                .collect(Collectors.toList());

        assertEquals(3, result.size());
        assertEquals(10, result.get(0).getValue().intValue());
        assertEquals(20, result.get(1).getValue().intValue());
        assertEquals(30, result.get(2).getValue().intValue());
    }

    @Test
    public void withIndex_ListModification_ReflectsOriginalList() {
        List<String> input = Arrays.asList("a", "b", "c");
        List<IndexedStream.IndexedValue<String>> result = IndexedStream.withIndex(input)
                .collect(Collectors.toList());

        // Modify original list
        input.set(1, "modified");

        // Check that the stream already collected the original values
        assertEquals("b", result.get(1).getValue());

        // New stream should reflect modified list
        List<IndexedStream.IndexedValue<String>> newResult = IndexedStream.withIndex(input)
                .collect(Collectors.toList());
        assertEquals("modified", newResult.get(1).getValue());
    }

    @Test(expected = NullPointerException.class)
    public void withIndex_NullList_ThrowsNullPointerException() {
        IndexedStream.withIndex(null).collect(Collectors.toList());
    }

    @Test
    public void withIndex_ListWithNullElements_HandlesNullsCorrectly() {
        List<String> input = Arrays.asList("a", null, "c");
        List<IndexedStream.IndexedValue<String>> result = IndexedStream.withIndex(input)
                .collect(Collectors.toList());

        assertEquals(3, result.size());
        assertEquals("a", result.get(0).getValue());
        assertNull(result.get(1).getValue());
        assertEquals("c", result.get(2).getValue());
    }
}
