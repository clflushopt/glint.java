package co.clflushopt.glint.types;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class SchemaTest {

    @Test
    public void shouldNotMutateFields() {
        var fields = List.of(new Field("id", 0, ArrowTypes.Int64Type),
                new Field("name", 1, ArrowTypes.StringType),
                new Field("department", 2, ArrowTypes.StringType),
                new Field("on_vacation", 3, ArrowTypes.BooleanType),
                new Field("salary", 4, ArrowTypes.Int64Type));

        var schema = new Schema(fields);

        assertEquals(schema.getFields(), fields);
    }
}
