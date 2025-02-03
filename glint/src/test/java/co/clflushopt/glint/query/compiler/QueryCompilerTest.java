package co.clflushopt.glint.query.compiler;

import java.nio.file.Path;

import org.junit.Test;

public class QueryCompilerTest {
    @Test
    public void canCompileBasicScanOperator() throws Exception {
        Path path = Path.of("../datasets/yellow_tripdata_2019-01.parquet");
        String filename = path.toAbsolutePath().toString();
        QueryCompiler c = new QueryCompiler();
        String scanCompiledQuery = c.generateParquetScanSourceCode(filename);
        c.compileAndRun(scanCompiledQuery);
    }

}
