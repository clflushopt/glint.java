package co.clflushopt.glint.query.compiler;

import java.io.StringReader;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

import org.codehaus.commons.compiler.ISimpleCompiler;
import org.codehaus.janino.CompilerFactory;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Scan;

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
                            totalRecords += batch.getRowCount();
                            System.out.println("Batch size: " + batch.getRowCount());
                        }

                        return totalRecords;
                    }
                }
                """, filename);
    }

    /**
     * Compiles and executes a logical plan.
     *
     * @param logicalPlan The root of the logical plan to compile
     * @return The result of executing the compiled plan
     * @throws Exception If compilation or execution fails
     */
    public Object compile(LogicalPlan logicalPlan) throws Exception {
        // Generate source code for the entire logical plan
        String sourceCode = generateSourceCode(logicalPlan);

        // Create a new simple compiler
        ISimpleCompiler compiler = compilerFactory.newSimpleCompiler();

        // Set the source version to Java 21
        compiler.setSourceVersion(21);
        compiler.setTargetVersion(21);

        // Cook (compile) the source code
        compiler.cook(new StringReader(sourceCode));

        // Load the compiled class
        Class<?> compiledClass = compiler.getClassLoader()
                .loadClass("co.clflushopt.glint.generated.LogicalPlanExecutor");

        // Create an instance
        Object instance = compiledClass.getDeclaredConstructor().newInstance();

        // Find and invoke the execute method
        Method executeMethod = compiledClass.getMethod("execute");
        return executeMethod.invoke(instance);
    }

    /**
     * Generates source code for a given logical plan.
     *
     * @param logicalPlan The logical plan to generate code for
     * @return Generated Java source code as a string
     */
    private String generateSourceCode(LogicalPlan logicalPlan) {
        return String.format(
                """
                        package co.clflushopt.glint.generated;

                        import co.clflushopt.glint.query.logical.plan.LogicalPlan;
                        import co.clflushopt.glint.query.logical.plan.Scan;
                        import co.clflushopt.glint.datasource.DataSource;
                        import co.clflushopt.glint.types.RecordBatch;
                        import co.clflushopt.glint.types.Schema;
                        import java.util.List;

                        public class LogicalPlanExecutor {
                            public long execute() {
                                return executePlan(%s);
                            }

                            private long executePlan(LogicalPlan plan) {
                                // Handle different logical plan types
                                if (plan instanceof Scan) {
                                    Scan scan = (Scan) plan;
                                    return executeScan(scan);
                                }

                                // TODO: Add support for other logical plan types
                                throw new UnsupportedOperationException("Unsupported logical plan type: " + plan.getClass().getSimpleName());
                            }

                            private long executeScan(Scan scan) {
                                DataSource dataSource = scan.getDataSource();
                                List<String> projections = scan.getProjections();

                                long totalRecords = 0;
                                for (RecordBatch batch : dataSource.scan(projections)) {
                                    totalRecords += batch.getRowCount();
                                    System.out.println("Scan batch size: " + batch.getRowCount() +
                                                       " Path: " + scan.getPath());
                                }

                                return totalRecords;
                            }
                        }
                        """,
                generatePlanArgument(logicalPlan));
    }

    /**
     * Generates a string representation of the logical plan to be used as a method
     * argument.
     *
     * @param plan The logical plan to convert
     * @return A string that can be used to reconstruct the plan in the generated
     *         code
     */
    private String generatePlanArgument(LogicalPlan plan) {
        if (plan instanceof Scan scan) {
            return String.format("new Scan(\"%s\", new %s(\"%s\"), %s)", scan.getPath(),
                    scan.getDataSource().getClass().getName(), scan.getPath(),
                    formatProjections(scan.getProjections()));
        }

        // TODO: Add support for other logical plan types
        throw new UnsupportedOperationException(
                "Unsupported logical plan type: " + plan.getClass().getSimpleName());
    }

    /**
     * Formats the list of projections as a Java code string.
     *
     * @param projections List of projection column names
     * @return A Java code representation of the projections list
     */
    private String formatProjections(List<String> projections) {
        if (projections == null || projections.isEmpty()) {
            return "List.of()";
        }

        return "List.of("
                + projections.stream().map(p -> "\"" + p + "\"").collect(Collectors.joining(", "))
                + ")";
    }
}