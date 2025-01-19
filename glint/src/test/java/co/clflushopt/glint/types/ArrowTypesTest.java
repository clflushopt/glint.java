package co.clflushopt.glint.types;

import static org.junit.Assert.assertEquals;

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

        for (int i = 0; i < size; i++) {
            fieldVec.set(i, i);
        }

        fieldVec.setValueCount(size);
        for (int i = 0; i < size; i++) {
            var expected = i;
            var actual = fieldVec.get(i);
            assertEquals(expected, actual);
        }

        fieldVec.close();
    }

}
