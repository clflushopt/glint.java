package co.clflushopt.glint.query.planner;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import co.clflushopt.glint.dataframe.DataFrame;
import co.clflushopt.glint.dataframe.DataFrameImpl;
import co.clflushopt.glint.datasource.CsvDataSource;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.sql.Parser;
import co.clflushopt.glint.sql.SqlExpression;
import co.clflushopt.glint.sql.SqlSelectStmt;
import co.clflushopt.glint.sql.TokenStream;
import co.clflushopt.glint.sql.Tokenizer;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

public class LogicalPlannerTest {
    private final String dir = "./testdata";
    private final String employeeCsv = new File(dir, "employee.csv").getAbsolutePath();

    @Test
    public void testSimpleSelect() {
        LogicalPlan plan = plan("SELECT state FROM employee");
        assertEquals("Projection: #state\n" + "\tScan:  [projection=None]\n",
                LogicalPlan.format(plan));
    }

    @Test
    public void testSelectWithFilter() {
        LogicalPlan plan = plan("SELECT state FROM employee WHERE state = 'CA'");
        assertEquals("Filter: #state = 'CA'\n" + "\tProjection: #state\n"
                + "\t\tScan:  [projection=None]\n", LogicalPlan.format(plan));
    }

    @Test
    public void testSelectWithFilterNotInProjection() {
        LogicalPlan plan = plan("SELECT last_name FROM employee WHERE state = 'CA'");
        assertEquals("Projection: #last_name\n" + "\tFilter: #state = 'CA'\n"
                + "\t\tProjection: #last_name, #state\n" + "\t\t\tScan:  [projection=None]\n",
                LogicalPlan.format(plan));
    }

    @Test
    public void testSelectFilterOnProjection() {
        LogicalPlan plan = plan("SELECT last_name AS foo FROM employee WHERE foo = 'Einstein'");
        assertEquals("Filter: #foo = 'Einstein'\n" + "\tProjection: #last_name AS foo\n"
                + "\t\tScan:  [projection=None]\n", LogicalPlan.format(plan));
    }

    @Test
    public void testSelectFilterOnProjectionAndNot() {
        LogicalPlan plan = plan("SELECT last_name AS foo " + "FROM employee "
                + "WHERE foo = 'Einstein' AND state = 'CA'");
        assertEquals("Projection: #foo\n" + "\tFilter: #foo = 'Einstein' AND #state = 'CA'\n"
                + "\t\tProjection: #last_name AS foo, #state\n"
                + "\t\t\tScan:  [projection=None]\n", LogicalPlan.format(plan));
    }

    @Test
    public void testPlanAggregateQuery() {
        LogicalPlan plan = plan("SELECT state, MAX(salary) FROM employee GROUP BY state");
        assertEquals("Projection: #0, #1\n"
                + "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n"
                + "\t\tScan:  [projection=None]\n", LogicalPlan.format(plan));
    }

    @Test
    public void testPlanAggregateQueryWithHaving() {
        LogicalPlan plan = plan(
                "SELECT state, MAX(salary) FROM employee GROUP BY state HAVING MAX(salary) > 10");
        assertEquals("Filter: MAX(#salary) > 10\n" + "\tProjection: #0, #1\n"
                + "\t\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n"
                + "\t\t\tScan:  [projection=None]\n", LogicalPlan.format(plan));
    }

    @Test
    public void testPlanAggregateQueryAggrFirst() {
        LogicalPlan plan = plan("SELECT MAX(salary), state FROM employee GROUP BY state");
        assertEquals("Projection: #1, #0\n"
                + "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n"
                + "\t\tScan:  [projection=None]\n", LogicalPlan.format(plan));
    }

    @Test
    public void testPlanAggregateQueryWithFilter() {
        LogicalPlan plan = plan(
                "SELECT state, MAX(salary) FROM employee WHERE salary > 50000 GROUP BY state");
        assertEquals("Projection: #0, #1\n"
                + "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n"
                + "\t\tFilter: #salary > 50000\n" + "\t\t\tProjection: #state, #salary\n"
                + "\t\t\t\tScan:  [projection=None]\n", LogicalPlan.format(plan));
    }

    @Test
    public void testPlanAggregateQueryWithCast() {
        LogicalPlan plan = plan(
                "SELECT state, MAX(CAST(salary AS double)) FROM employee GROUP BY state");
        assertEquals("Projection: #0, #1\n"
                + "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(CAST(#salary AS DOUBLE))]\n"
                + "\t\tScan:  [projection=None]\n", LogicalPlan.format(plan));
    }

    private LogicalPlan plan(String sql) {
        System.out.println("parse() " + sql);

        TokenStream tokens = new Tokenizer(sql).tokenize();
        System.out.println(tokens);

        SqlExpression parsedQuery = new Parser(tokens).parse();
        System.out.println(parsedQuery);
        LogicalPlan nullPlan = null;

        List<Field> expectedFields = List.of(new Field("id", ArrowTypes.StringType),
                new Field("first_name", ArrowTypes.StringType), // Changed from Int64Type
                new Field("last_name", ArrowTypes.StringType),
                new Field("state", ArrowTypes.StringType),
                new Field("job_title", ArrowTypes.StringType),
                new Field("salary", ArrowTypes.StringType));
        Schema expectedSchema = new Schema(expectedFields);
        try {
            Map<String, DataFrame> tables = Collections.singletonMap("employee",
                    new DataFrameImpl(new Scan("",
                            new CsvDataSource(employeeCsv, Optional.of(expectedSchema), true, 1024),
                            Collections.emptyList())));

            LogicalPlanner planner = new LogicalPlanner();

            DataFrame df;
            try {
                df = planner.createDataFrame((SqlSelectStmt) parsedQuery, tables);

                LogicalPlan plan = df.getLogicalPlan();
                System.out.println(LogicalPlan.format(plan));

                return plan;
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {

        }
        return nullPlan;
    }
}