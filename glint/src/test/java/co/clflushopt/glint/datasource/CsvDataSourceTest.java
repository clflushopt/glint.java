package co.clflushopt.glint.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.Iterables;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class CsvDataSourceTest {

    @Test
    public void canProcessSimpleCsvFilesWithoutHeaders() {
        String filename = "./testdata/employee_no_header.csv";
        List<Field> expectedFields = List.of(new Field("field_0", ArrowTypes.StringType),
                new Field("field_1", ArrowTypes.StringType), // Changed from Int64Type
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
                new Field("first_name", ArrowTypes.StringType), // Changed from Int64Type
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

    @Test
    public void canBuildDatasourceFromSchema() {
        String filename = "./testdata/employee.csv";
        List<Field> expectedFields = List.of(new Field("id", ArrowTypes.StringType),
                new Field("first_name", ArrowTypes.StringType), // Changed from Int64Type
                new Field("last_name", ArrowTypes.StringType),
                new Field("state", ArrowTypes.StringType),
                new Field("job_title", ArrowTypes.StringType),
                new Field("salary", ArrowTypes.StringType));
        Schema expectedSchema = new Schema(expectedFields);

        // Try building the schema from the datasource.
        CsvDataSource source = null;
        Optional<Schema> schema = Optional.of(expectedSchema);

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

    @Test
    public void canProcessSimpleCSvFileWithHeaderAndSchema() {
        String filename = "./testdata/employee.csv";
        List<Field> expectedFields = List.of(new Field("id", ArrowTypes.Int64Type),
                new Field("first_name", ArrowTypes.StringType), // Changed from Int64Type
                new Field("last_name", ArrowTypes.StringType),
                new Field("state", ArrowTypes.StringType),
                new Field("job_title", ArrowTypes.StringType),
                new Field("salary", ArrowTypes.Int64Type));
        Schema expectedSchema = new Schema(expectedFields);
        Optional<Schema> schema = Optional.of(expectedSchema);

        // Create the datasource
        CsvDataSource source = null;
        try {
            source = new CsvDataSource(filename, schema, true, 2); // batch size
        } catch (Exception e) {
            fail(e.getMessage());
        }
        ;
        // of 2
        assertNotNull("source was not properly populated", source);
        assertEquals("inferred schema doesn't match expected one", expectedSchema.getFields(),
                source.getSchema().getFields());

        // Test the iterator functionality
        Iterator<RecordBatch> batchIterator = source.scan(List.of()).iterator();

        // First batch should contain first two rows
        assertTrue("Should have first batch", batchIterator.hasNext());
        RecordBatch batch1 = batchIterator.next();
        assertEquals("First batch should have 2 rows", 2, batch1.getRowCount());

        // Verify first row
        assertEquals(1L, getLongValue(batch1, "id", 0));
        assertEquals("Bill", getStringValue(batch1, "first_name", 0));
        assertEquals("Hopkins", getStringValue(batch1, "last_name", 0));
        assertEquals("CA", getStringValue(batch1, "state", 0));
        assertEquals("Manager", getStringValue(batch1, "job_title", 0));
        assertEquals(12000L, getLongValue(batch1, "salary", 0));

        // Verify second row
        assertEquals(2L, getLongValue(batch1, "id", 1));
        assertEquals("Gregg", getStringValue(batch1, "first_name", 1));
        assertEquals("Langford", getStringValue(batch1, "last_name", 1));
        assertEquals("CO", getStringValue(batch1, "state", 1));
        assertEquals("Driver", getStringValue(batch1, "job_title", 1));
        assertEquals(10000L, getLongValue(batch1, "salary", 1));

        // Second batch should contain remaining two rows
        assertTrue("Should have second batch", batchIterator.hasNext());
        RecordBatch batch2 = batchIterator.next();
        assertEquals("Second batch should have 2 rows", 2, batch2.getRowCount());

        // Verify third row
        assertEquals(3L, getLongValue(batch2, "id", 0));
        assertEquals("John", getStringValue(batch2, "first_name", 0));
        assertEquals("Travis", getStringValue(batch2, "last_name", 0));
        assertEquals("CO", getStringValue(batch2, "state", 0));
        assertEquals("Manager, Software", getStringValue(batch2, "job_title", 0));
        assertEquals(11500L, getLongValue(batch2, "salary", 0));

        // Verify fourth row (with empty state)
        assertEquals(4L, getLongValue(batch2, "id", 1));
        assertEquals("Von", getStringValue(batch2, "first_name", 1));
        assertEquals("Mill", getStringValue(batch2, "last_name", 1));
        assertEquals("", getStringValue(batch2, "state", 1));
        assertEquals("Defensive End", getStringValue(batch2, "job_title", 1));
        assertEquals(11500L, getLongValue(batch2, "salary", 1));

        // Verify no more batches
        assertFalse("Should not have more batches", batchIterator.hasNext());
    }

    // Helper methods to get values from the batch
    private String getStringValue(RecordBatch batch, String columnName, int rowIndex) {
        var index = Iterables.indexOf(batch.getSchema().getFields(), f -> f.name() == columnName);
        return (String) batch.getField(index).getValue(rowIndex);
    }

    private long getLongValue(RecordBatch batch, String columnName, int rowIndex) {
        var index = Iterables.indexOf(batch.getSchema().getFields(), f -> f.name() == columnName);
        return (Long) batch.getField(index).getValue(rowIndex);
    }
}
