package co.clflushopt.glint;

import java.io.FileNotFoundException;
import java.util.Iterator;

import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.core.ExecutionContext;
import co.clflushopt.glint.dataframe.DataFrame;
import co.clflushopt.glint.query.logical.expr.AggregateExpr;
import co.clflushopt.glint.query.logical.expr.CastExpr;
import co.clflushopt.glint.query.logical.expr.ColumnExpr;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.optimizer.QueryOptimizer;
import co.clflushopt.glint.types.RecordBatch;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Welcome to the Glint query compiler");
        try {
            nycTripsBenchmark(args);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void nycTripsBenchmark(String[] args) throws FileNotFoundException {
        // Create execution context
        ExecutionContext ctx = ExecutionContext.create().build();

        long startTime = System.currentTimeMillis();
        try {

            // Create DataFrame and apply transformations
            DataFrame df = ctx.readParquet("./datasets/yellow_tripdata_2019-01.parquet", null);

            System.out.println("Logical Plan:\t" + LogicalPlan.format(df.getLogicalPlan()));
            System.out.println("Schema:\t" + df.getSchema());

            // Optimize and execute the plan
            LogicalPlan optimizedPlan = QueryOptimizer.optimize(df.getLogicalPlan());
            System.out.println("Optimized Plan:\t" + LogicalPlan.format(optimizedPlan));

            // Execute and print results
            Iterator<RecordBatch> results = ctx.execute(optimizedPlan);

            while (results.hasNext()) {
                RecordBatch batch = results.next();
                System.out.println(batch.getSchema());
                System.out.println(batch.toCsv());

            }

        } finally {
            long endTime = System.currentTimeMillis();
            System.out.println("Query took " + (endTime - startTime) + " ms");
        }
    }

    // Helper methods for creating expressions
    private static LogicalExpr col(String name) {
        return new ColumnExpr(name);
    }

    private static LogicalExpr cast(LogicalExpr expr, ArrowType targetType) {
        return new CastExpr(expr, targetType);
    }

    private static AggregateExpr max(LogicalExpr expr) {
        return new AggregateExpr.Max(expr);
    }
}