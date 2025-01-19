package co.clflushopt.glint.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Optional;

import org.junit.Test;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

public class CsvDataSourceTest {

    @Test
    public void canProcessSimpleCsvFilesWithoutHeaders() {
        String filename = "./testdata/employee_no_header.csv";
        List<Field> expectedFields = List.of(new Field("field_0", ArrowTypes.StringType),
                new Field("field_1", ArrowTypes.StringType),
                new Field("field_2", ArrowTypes.StringType),
                new Field("field_3", ArrowTypes.StringType),
                new Field("field_4", ArrowTypes.StringType),
                new Field("field_5", ArrowTypes.StringType));
        Schema expectedSchema = new Schema(expectedFields);

        // Try building the schema from the datasource.
        CsvDataSource source = null;
        Optional<Schema> schema = Optional.ofNullable(null);

        try {
            source = new CsvDataSource(filename, schema, false, 1);
        } catch (Exception e) {
            fail("shit happens: " + e.fillInStackTrace());
        }

        // Actual assertions.
        assertNotNull("source was not properly populated", source);
        assertEquals("inferred schema doesn't match expected one", expectedSchema.getFields(),
                source.getSchema().getFields());
    }

    @Test
    public void canInferColumnNamesFromHeaders() {
        String filename = "./testdata/employee.csv";
        List<Field> expectedFields = List.of(new Field("id", ArrowTypes.StringType),
                new Field("first_name", ArrowTypes.StringType),
                new Field("last_name", ArrowTypes.StringType),
                new Field("state", ArrowTypes.StringType),
                new Field("job_title", ArrowTypes.StringType),
                new Field("salary", ArrowTypes.StringType));
        Schema expectedSchema = new Schema(expectedFields);

        // Try building the schema from the datasource.
        CsvDataSource source = null;
        Optional<Schema> schema = Optional.ofNullable(null);

        try {
            source = new CsvDataSource(filename, schema, true, 1);
        } catch (Exception e) {
            fail("shit happens: " + e.fillInStackTrace());
        }

        // Actual assertions.
        assertNotNull("source was not properly populated", source);
        assertEquals("inferred schema doesn't match expected one", expectedSchema.getFields(),
                source.getSchema().getFields());
    }
}
