package co.clflushopt.glint.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

public class ParserTest {

    @Test
    public void canParseArithmeticExpressions() {
        var sql = "1 + 2 * 3";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        var expected = new SqlBinaryExpr(new SqlLong(1), "+",
                new SqlBinaryExpr(new SqlLong(2), "*", new SqlLong(3)));

        assertEquals(actual, expected);
    }

    @Test
    public void canParseArithmeticExpressionWithLeftPrec() {
        var sql = "1 * 2 + 3";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        var expected = new SqlBinaryExpr(new SqlBinaryExpr(new SqlLong(1), "*", new SqlLong(2)),
                "+", new SqlLong(3));

        assertEquals(actual, expected);
    }

    @Test
    public void canParseSimpleSelectStatement() {
        var sql = "SELECT id, first_name FROM employees";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;
        assertEquals(select.getTableName(), "employees");
        assertEquals(select.getProjection(),
                List.of(new SqlIdentifier("id"), new SqlIdentifier("first_name")));
    }

    @Test
    public void canParseSelectStatementWithBinaryExpression() {
        var sql = "SELECT salary * 0.1 FROM employees";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;
        assertEquals(select.getTableName(), "employees");
        assertEquals(select.getProjection(),
                List.of(new SqlBinaryExpr(new SqlIdentifier("salary"), "*", new SqlDouble(0.1))));

    }

    @Test
    public void canParseSelectStatementWithAliasedExpression() {
        var sql = "SELECT salary * 0.1 AS bonus FROM employees";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;
        assertEquals(select.getTableName(), "employees");
        assertEquals(select.getProjection(),
                List.of(new SqlAliasExpr(
                        new SqlBinaryExpr(new SqlIdentifier("salary"), "*", new SqlDouble(0.1)),
                        new SqlIdentifier("bonus"))));

    }

    @Test
    public void canParseSelectWithWhere() {
        var sql = "SELECT id, first_name, last_name FROM employee WHERE state = 'CO'";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;

        assertEquals(List.of(new SqlIdentifier("id"), new SqlIdentifier("first_name"),
                new SqlIdentifier("last_name")), select.getProjection());

        assertEquals(new SqlBinaryExpr(new SqlIdentifier("state"), "=", new SqlString("CO")),
                select.getSelection());

        assertEquals("employee", select.getTableName());
    }

    @Test
    public void canParseSelectWithOrder() {
        var sql = "SELECT state, salary FROM employee ORDER BY salary desc, state";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;

        assertEquals(List.of(new SqlIdentifier("state"), new SqlIdentifier("salary")),
                select.getProjection());

        assertEquals(List.of(new SqlSort(new SqlIdentifier("salary"), false),
                new SqlSort(new SqlIdentifier("state"), true)), select.getOrderBy());
    }

    @Test
    public void canParseSelectWithAggregates() {
        var sql = "SELECT state, MAX(salary) FROM employee GROUP BY state";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;

        assertEquals(
                List.of(new SqlIdentifier("state"),
                        new SqlFunction("MAX", List.of(new SqlIdentifier("salary")))),
                select.getProjection());

        assertEquals(List.of(new SqlIdentifier("state")), select.getGroupBy());

        assertEquals("employee", select.getTableName());
    }

    @Test
    public void canParseSelectWithAliasedAggregates() {
        var sql = "SELECT state, MAX(salary) AS top_wage FROM employee GROUP BY state";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;

        var max = new SqlFunction("MAX", List.of(new SqlIdentifier("salary")));
        var alias = new SqlAliasExpr(max, new SqlIdentifier("top_wage"));

        assertEquals(List.of(new SqlIdentifier("state"), alias), select.getProjection());

        assertEquals(List.of(new SqlIdentifier("state")), select.getGroupBy());

        assertEquals("employee", select.getTableName());
    }

    @Test
    public void canParseSelectWithAggregatesAndHaving() {
        var sql = "SELECT state, MAX(salary) AS top_wage FROM employee "
                + "GROUP BY state HAVING MAX(salary) > 10 AND MAX(salary) < 100";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;

        var max = new SqlFunction("MAX", List.of(new SqlIdentifier("salary")));
        var alias = new SqlAliasExpr(max, new SqlIdentifier("top_wage"));

        assertEquals(List.of(new SqlIdentifier("state"), alias), select.getProjection());

        assertEquals(List.of(new SqlIdentifier("state")), select.getGroupBy());

        assertEquals("employee", select.getTableName());
    }

    @Test
    public void canParseSelectWithAggregatesAndCast() {
        var sql = "SELECT state, MAX(CAST(salary AS double)) FROM employee GROUP BY state";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelectStmt);
        var select = (SqlSelectStmt) actual;

        assertEquals(
                List.of(new SqlIdentifier("state"), new SqlFunction("MAX", List.of(
                        new SqlCast(new SqlIdentifier("salary"), new SqlIdentifier("double"))))),
                select.getProjection());

        assertEquals(List.of(new SqlIdentifier("state")), select.getGroupBy());

        assertEquals("employee", select.getTableName());
    }
}
