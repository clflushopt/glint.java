package co.clflushopt.glint.query.logical.expr;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import co.clflushopt.glint.datasource.CsvDataSource;
import co.clflushopt.glint.query.logical.plan.Filter;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Projection;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

public class LogicalPlanTest {

        @Test
        public void canProperlyFormatLogicalPlans() throws FileNotFoundException {
                String filename = "./testdata/employee.csv";
                List<Field> expectedFields = List.of(new Field("id", ArrowTypes.Int64Type),
                                new Field("first_name", ArrowTypes.StringType), // Changed from
                                                                                // Int64Type
                                new Field("last_name", ArrowTypes.StringType),
                                new Field("state", ArrowTypes.StringType),
                                new Field("job_title", ArrowTypes.StringType),
                                new Field("salary", ArrowTypes.Int64Type));
                Schema expectedSchema = new Schema(expectedFields);
                Optional<Schema> schema = Optional.of(expectedSchema);

                // Create the datasource
                CsvDataSource source = new CsvDataSource(filename, schema, true, 2); // batch size

                var scan = new Scan(filename, source, List.of());
                var filter = LogicalBooleanExpr.Eq(new LogicalColumnExpr("state"),
                                new LogicalLiteralString("CO"));
                var selection = new Filter(scan, filter);
                var plan = new Projection(selection,
                                List.of(new LogicalColumnExpr("id"),
                                                new LogicalColumnExpr("first_name"),
                                                new LogicalColumnExpr("last_name")));

                assertEquals("""
                                Projection: #id, #first_name, #last_name
                                	Filter: #state = 'CO'
                                		Scan: ./testdata/employee.csv [projection=None]
                                                """, LogicalPlan.format(plan, 1));
        }
}
