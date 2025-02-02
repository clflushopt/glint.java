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
        var expected = new SqlBinaryExpression(new SqlLong(1), "+",
                new SqlBinaryExpression(new SqlLong(2), "*", new SqlLong(3)));

        assertEquals(actual, expected);
    }

    @Test
    public void canParseArithmeticExpressionWithLeftPrec() {
        var sql = "1 * 2 + 3";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        var expected = new SqlBinaryExpression(
                new SqlBinaryExpression(new SqlLong(1), "*", new SqlLong(2)), "+", new SqlLong(3));

        assertEquals(actual, expected);
    }

    @Test
    public void canParseSimpleSelectStatement() {
        var sql = "SELECT id, first_name FROM employees";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelect);
        var select = (SqlSelect) actual;
        assertEquals(select.getTableName(), "employees");
        assertEquals(select.getProjection(),
                List.of(new SqlIdentifier("id"), new SqlIdentifier("first_name")));
    }

    @Test
    public void canParseSelectStatementWithBinaryExpression() {
        var sql = "SELECT salary * 0.1 FROM employees";
        var tokens = new Tokenizer(sql).tokenize();
        var actual = new Parser(tokens).parse();
        assertTrue(actual instanceof SqlSelect);
        var select = (SqlSelect) actual;
        assertEquals(select.getTableName(), "employees");
        assertEquals(select.getProjection(), List
                .of(new SqlBinaryExpression(new SqlIdentifier("salary"), "*", new SqlDouble(0.1))));

    }
}
