package co.clflushopt.glint.types;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class SchemaTest {

    @Test
    public void shouldNotMutateFields() {
        var fields = List.of(new Field("id", ArrowTypes.Int64Type),
                new Field("name", ArrowTypes.StringType),
                new Field("department", ArrowTypes.StringType),
                new Field("on_vacation", ArrowTypes.BooleanType),
                new Field("salary", ArrowTypes.Int64Type));

        var schema = new Schema(fields);

        assertEquals(schema.getFields(), fields);
    }
}
