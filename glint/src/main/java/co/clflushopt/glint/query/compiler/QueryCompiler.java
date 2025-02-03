package co.clflushopt.glint.query.compiler;

import java.io.StringReader;
import java.lang.reflect.Method;

import org.codehaus.commons.compiler.ISimpleCompiler;
import org.codehaus.janino.CompilerFactory;

/**
 * Core Query Compiler using Janino for runtime code generation. This class
 * provides the fundamental mechanism for generating and compiling executable
 * query plans at runtime.
 */
public class QueryCompiler {
    private final CompilerFactory compilerFactory = new CompilerFactory();

    /**
     * Compiles and executes a generated source code snippet.
     *
     * @param sourceCode The Java source code to compile and run
     * @return The result of executing the compiled code
     * @throws Exception If compilation or execution fails
     */
    public Object compileAndRun(String sourceCode) throws Exception {
        // Create a new simple compiler
        ISimpleCompiler compiler = compilerFactory.newSimpleCompiler();

        // Set the source version to Java 21
        compiler.setSourceVersion(21);
        compiler.setTargetVersion(21);

        // Cook (compile) the source code
        compiler.cook(new StringReader(sourceCode));

        // Load the compiled class
        Class<?> compiledClass = compiler.getClassLoader()
                .loadClass("co.clflushopt.glint.generated.QueryExecutor");

        // Create an instance
        Object instance = compiledClass.getDeclaredConstructor().newInstance();

        // Find and invoke the execute method
        Method executeMethod = compiledClass.getMethod("execute");
        return executeMethod.invoke(instance);
    }

    /**
     * Generates a basic source code template for query execution.
     *
     * @param filename The Parquet file to read
     * @return Generated Java source code as a string
     */
    public String generateParquetScanSourceCode(String filename) {
        return String.format("""
                package co.clflushopt.glint.generated;

                import co.clflushopt.glint.datasource.ParquetDataSource;
                import co.clflushopt.glint.types.RecordBatch;
                import java.util.List;

                public class QueryExecutor {
                    public long execute() {
                        ParquetDataSource dataSource = new ParquetDataSource("%s");
                        long totalRecords = 0;

                        for (RecordBatch batch : dataSource.scan(List.of())) {
                            totalRecords += batch.getRowSize();
                            System.out.println("Batch size: " + batch.getRowSize());
                        }

                        return totalRecords;
                    }
                }
                """, filename);
    }
}