package co.clflushopt.glint.types;

import static org.junit.Assert.assertEquals;

import java.util.stream.IntStream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.Test;

public class ArrowTypesTest {

    @Test
    public void canBuildArrowVectors() {
        var allocator = new RootAllocator();
        var fieldVec = new IntVector("my_vector", allocator);
        var size = 53;

        // Arrow requires explicitely allocating and setting number of
        // vector elements.
        fieldVec.allocateNew(size);
        IntStream.range(0, size).forEach(i -> {
            fieldVec.set(i, i);
        });
        IntStream.range(0, size).forEach(i -> {
            assertEquals(fieldVec.get(i), i);
        });
        fieldVec.close();
    }

}
