package co.clflushopt.glint.query.compiler;

import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.util.Collections;

import org.junit.Assume;
import org.junit.Test;

import co.clflushopt.glint.datasource.ParquetDataSource;
import co.clflushopt.glint.query.logical.plan.Scan;

public class QueryCompilerTest {
    @Test
    public void canCompileInlinedParquetScan() throws Exception {
        Assume.assumeTrue("Skipping test due to missing data",
                System.getenv("DISABLE_COMPILER_TESTS") != null);

        Path path = Path.of("../datasets/yellow_tripdata_2019-01.parquet");
        String filename = path.toAbsolutePath().toString();
        QueryCompiler c = new QueryCompiler();
        String scanCompiledQuery = c.generateParquetScanSourceCode(filename);
        c.compileAndRun(scanCompiledQuery);
    }

    @Test
    public void canCompileScanPlan() throws Exception {
        Assume.assumeTrue("Skipping test due to missing data",
                System.getenv("DISABLE_COMPILER_TESTS") != null);
        try {
            Path path = Path.of("../datasets/yellow_tripdata_2019-01.parquet");
            String filename = path.toAbsolutePath().toString();
            // Create a sample schema
            // Create a Parquet data source
            ParquetDataSource dataSource = new ParquetDataSource(filename);

            // Create a Scan logical plan
            Scan scanPlan = new Scan(filename, dataSource, Collections.emptyList());

            // Instantiate the logical plan compiler
            QueryCompiler compiler = new QueryCompiler();

            // Compile and execute the logical plan
            Object result = compiler.compile(scanPlan);
            System.out.println("Total Records Read: " + result);

        } catch (Exception e) {
            fail(e.getMessage() + "\n" + e.getStackTrace());
        }
    }

}
