package co.clflushopt.glint.query.logical.expr;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;
import org.junit.Test;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

public class BinaryExprTest {

    // Mock classes for testing
    private static class MockLogicalExpr implements LogicalExpr {
        private final ArrowType type;
        private final String value;

        public MockLogicalExpr(String value) {
            this.value = value;
            this.type = ArrowTypes.StringType;
        }

        public MockLogicalExpr(String value, ArrowType type) {
            this.value = value;
            this.type = type;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public Field toField(LogicalPlan plan) {
            return new Field("mock", ArrowTypes.Int32Type);
        }
    }

    private LogicalExpr col1;
    private LogicalExpr col2;
    private LogicalExpr intExpr;
    private LogicalExpr stringExpr;

    private LogicalPlan mockPlan;

    @Before
    public void setUp() {
        col1 = new MockLogicalExpr("col1");
        col2 = new MockLogicalExpr("col2");
        intExpr = new MockLogicalExpr("age", ArrowTypes.Int64Type);
        stringExpr = new MockLogicalExpr("name", ArrowTypes.StringType);

        mockPlan = new LogicalPlan() {

            @Override
            public Schema getSchema() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'getSchema'");
            }

            @Override
            public List<LogicalPlan> children() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'children'");
            }

        }; // Anonymous implementation for testing
    }

    @Test
    public void testBooleanExprEquality() {
        BooleanExpr expr = BooleanExpr.Eq(col1, col2);

        assertEquals("eq", expr.getName());
        assertEquals("=", expr.getOperator());
        assertEquals(col1, expr.getLhs());
        assertEquals(col2, expr.getRhs());
        assertEquals("col1 = col2", expr.toString());
    }

    @Test
    public void testBooleanExprToField() {
        BooleanExpr expr = BooleanExpr.Eq(col1, col2);
        Field field = expr.toField(mockPlan);

        assertEquals("eq", field.name());
        assertEquals(ArrowTypes.BooleanType, field.dataType());
    }

    @Test
    public void testAllComparisonOperators() {
        // Test creation and string representation of all comparison operators
        assertEquals("col1 = col2", BooleanExpr.Eq(col1, col2).toString());
        assertEquals("col1 != col2", BooleanExpr.Neq(col1, col2).toString());
        assertEquals("col1 > col2", BooleanExpr.Gt(col1, col2).toString());
        assertEquals("col1 >= col2", BooleanExpr.Gte(col1, col2).toString());
        assertEquals("col1 < col2", BooleanExpr.Lt(col1, col2).toString());
        assertEquals("col1 <= col2", BooleanExpr.Lte(col1, col2).toString());
    }

    @Test
    public void testLogicalOperators() {
        // Create some comparison expressions to use in logical operations
        BooleanExpr eq = BooleanExpr.Eq(col1, col2);
        BooleanExpr gt = BooleanExpr.Gt(col1, col2);

        // Test AND operator
        BooleanExpr and = BooleanExpr.And(eq, gt);
        assertEquals("col1 = col2 AND col1 > col2", and.toString());

        // Test OR operator
        BooleanExpr or = BooleanExpr.Or(eq, gt);
        assertEquals("col1 = col2 OR col1 > col2", or.toString());
    }

    @Test
    public void testComplexExpressionTree() {
        // Build a more complex expression tree: (col1 = col2) AND (col1 > col2 OR col1
        // < col2)
        BooleanExpr eq = BooleanExpr.Eq(col1, col2);
        BooleanExpr gt = BooleanExpr.Gt(col1, col2);
        BooleanExpr lt = BooleanExpr.Lt(col1, col2);
        BooleanExpr or = BooleanExpr.Or(gt, lt);
        BooleanExpr and = BooleanExpr.And(eq, or);

        String expected = "col1 = col2 AND col1 > col2 OR col1 < col2";
        assertEquals(expected, and.toString());

        // Verify the structure
        assertEquals("and", and.getName());
        assertEquals("AND", and.getOperator());
        assertEquals(eq, and.getLhs());
        assertEquals(or, and.getRhs());
    }

    // CastExpr Tests
    @Test
    public void testCastExprToString() {
        CastExpr cast = new CastExpr(intExpr, ArrowTypes.Int64Type);
        assertEquals("CAST(age AS INT64)", cast.toString());
    }

    @Test
    public void testCastExprField() {
        CastExpr cast = new CastExpr(intExpr, ArrowTypes.Int64Type);
        Field field = cast.toField(mockPlan);

        assertEquals("mock", field.name());
        assertEquals(ArrowTypes.Int64Type, field.dataType());
    }

    @Test
    public void testCastExprMultipleTypes() {
        // Test casting between different types
        CastExpr intToString = new CastExpr(intExpr, ArrowTypes.StringType);
        assertEquals("CAST(age AS STRING)", intToString.toString());
        assertEquals(ArrowTypes.StringType, intToString.toField(mockPlan).dataType());

        CastExpr stringToInt = new CastExpr(stringExpr, ArrowTypes.Int32Type);
        assertEquals("CAST(name AS INT32)", stringToInt.toString());
        assertEquals(ArrowTypes.Int32Type, stringToInt.toField(mockPlan).dataType());
    }

    @Test
    public void testNestedCastExpr() {
        // Test nested casts (e.g., CAST(CAST(x AS type1) AS type2))
        CastExpr innerCast = new CastExpr(intExpr, ArrowTypes.Int64Type);
        CastExpr outerCast = new CastExpr(innerCast, ArrowTypes.DoubleType);

        assertEquals("CAST(CAST(age AS INT64) AS DOUBLE)", outerCast.toString());
        assertEquals(ArrowTypes.DoubleType, outerCast.toField(mockPlan).dataType());
    }

    // AliasExpr Tests
    @Test
    public void testAliasExprToString() {
        AliasExpr alias = new AliasExpr(intExpr, "user_age");
        assertEquals("age AS user_age", alias.toString());
    }

    @Test
    public void testAliasExprField() {
        AliasExpr alias = new AliasExpr(intExpr, "user_age");
        Field field = alias.toField(mockPlan);

        // Field should maintain original name and type
        assertEquals("mock", field.name());
        assertEquals(ArrowTypes.Int32Type, field.dataType());
    }

    @Test
    public void testAliasExprWithDifferentTypes() {
        // Test aliasing expressions of different types
        AliasExpr intAlias = new AliasExpr(intExpr, "user_age");
        assertEquals(ArrowTypes.Int32Type, intAlias.toField(mockPlan).dataType());
    }

    @Test
    public void testNestedAliasExpr() {
        // Test nested aliases (although this might be rare in practice)
        AliasExpr innerAlias = new AliasExpr(intExpr, "years");
        AliasExpr outerAlias = new AliasExpr(innerAlias, "age_in_years");

        assertEquals("age AS years AS age_in_years", outerAlias.toString());
        assertEquals(ArrowTypes.Int32Type, outerAlias.toField(mockPlan).dataType());
    }

    @Test
    public void testAliasWithCastExpr() {
        // Test combining alias with cast
        CastExpr cast = new CastExpr(intExpr, ArrowTypes.Int64Type);
        AliasExpr alias = new AliasExpr(cast, "big_age");

        assertEquals("CAST(age AS INT64) AS big_age", alias.toString());
        assertEquals(ArrowTypes.Int64Type, cast.toField(mockPlan).dataType());
    }
}
