package co.clflushopt.glint.query.physical;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;

import co.clflushopt.glint.types.ArrowFieldVector;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class TestUtils {
    public static List<Field> createTestSchema() {
        return Arrays.asList(new Field("id", ArrowTypes.Int32Type),
                new Field("name", ArrowTypes.StringType), new Field("age", ArrowTypes.Int32Type));
    }

    public static RecordBatch createTestBatch() {
        Schema schema = new Schema(createTestSchema());

        // Create vectors
        IntVector idVector = new IntVector("id", new RootAllocator(Long.MAX_VALUE));
        VarCharVector nameVector = new VarCharVector("name", new RootAllocator(Long.MAX_VALUE));
        IntVector ageVector = new IntVector("age", new RootAllocator(Long.MAX_VALUE));

        // Add data
        idVector.allocateNew(3);
        nameVector.allocateNew(3);
        ageVector.allocateNew(3);

        idVector.set(0, 1);
        nameVector.set(0, "Alice".getBytes());
        ageVector.set(0, 25);
        idVector.set(1, 2);
        nameVector.set(1, "Bob".getBytes());
        ageVector.set(1, 30);
        idVector.set(2, 3);
        nameVector.set(2, "Charlie".getBytes());
        ageVector.set(2, 35);

        idVector.setValueCount(3);
        nameVector.setValueCount(3);
        ageVector.setValueCount(3);

        List<ColumnVector> columns = Arrays.asList(new ArrowFieldVector(idVector),
                new ArrowFieldVector(nameVector), new ArrowFieldVector(ageVector));

        return new RecordBatch(schema, columns);
    }
}
