package co.clflushopt.glint.sql;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TokenizerTest {

    @Test
    public void canTokenizeSimpleSelect() {
        List<Token> expected = Arrays.asList(new Token("SELECT", Keyword.SELECT, 6),
                new Token("id", Literal.IDENTIFIER, 9), new Token(",", Symbol.COMMA, 10),
                new Token("first_name", Literal.IDENTIFIER, 21), new Token(",", Symbol.COMMA, 22),
                new Token("last_name", Literal.IDENTIFIER, 32), new Token("FROM", Keyword.FROM, 37),
                new Token("employee", Literal.IDENTIFIER, 46));

        List<Token> actual = new Tokenizer("SELECT id, first_name, last_name FROM employee")
                .tokenize().getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void canTokenizeProjectionWithBinaryExpression() {
        List<Token> expected = Arrays.asList(new Token("SELECT", Keyword.SELECT, 6),
                new Token("salary", Literal.IDENTIFIER, 13), new Token("*", Symbol.STAR, 15),
                new Token("0.1", Literal.DOUBLE, 19), new Token("FROM", Keyword.FROM, 24),
                new Token("employee", Literal.IDENTIFIER, 33));

        List<Token> actual = new Tokenizer("SELECT salary * 0.1 FROM employee").tokenize()
                .getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void testProjectionWithAliasedBinaryExpression() {
        List<Token> expected = Arrays.asList(new Token("SELECT", Keyword.SELECT, 6),
                new Token("salary", Literal.IDENTIFIER, 13), new Token("*", Symbol.STAR, 15),
                new Token("0.1", Literal.DOUBLE, 19), new Token("AS", Keyword.AS, 22),
                new Token("bonus", Literal.IDENTIFIER, 28), new Token("FROM", Keyword.FROM, 33),
                new Token("employee", Literal.IDENTIFIER, 42));

        List<Token> actual = new Tokenizer("SELECT salary * 0.1 AS bonus FROM employee").tokenize()
                .getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void canTokenizeSelectWithWhere() {
        List<Token> expected = Arrays.asList(new Token("SELECT", Keyword.SELECT, 6),
                new Token("a", Literal.IDENTIFIER, 8), new Token(",", Symbol.COMMA, 9),
                new Token("b", Literal.IDENTIFIER, 11), new Token("FROM", Keyword.FROM, 16),
                new Token("employee", Literal.IDENTIFIER, 25),
                new Token("WHERE", Keyword.WHERE, 31), new Token("state", Literal.IDENTIFIER, 37),
                new Token("=", Symbol.EQ, 39), new Token("CO", Literal.STRING, 44));

        List<Token> actual = new Tokenizer("SELECT a, b FROM employee WHERE state = 'CO'")
                .tokenize().getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void canTokenizeSelectWithAggregates() {
        List<Token> expected = Arrays.asList(new Token("SELECT", Keyword.SELECT, 6),
                new Token("state", Literal.IDENTIFIER, 12), new Token(",", Symbol.COMMA, 13),
                new Token("MAX", Keyword.MAX, 17), new Token("(", Symbol.LEFT_PAREN, 18),
                new Token("salary", Literal.IDENTIFIER, 24), new Token(")", Symbol.RIGHT_PAREN, 25),
                new Token("FROM", Keyword.FROM, 30), new Token("employee", Literal.IDENTIFIER, 39),
                new Token("GROUP", Keyword.GROUP, 45), new Token("BY", Keyword.BY, 48),
                new Token("state", Literal.IDENTIFIER, 54));

        List<Token> actual = new Tokenizer("SELECT state, MAX(salary) FROM employee GROUP BY state")
                .tokenize().getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void canTokenizeSelectWithAggregatesAndHaving() {
        List<Token> expected = Arrays.asList(new Token("SELECT", Keyword.SELECT, 6),
                new Token("state", Literal.IDENTIFIER, 12), new Token(",", Symbol.COMMA, 13),
                new Token("MAX", Keyword.MAX, 17), new Token("(", Symbol.LEFT_PAREN, 18),
                new Token("salary", Literal.IDENTIFIER, 24), new Token(")", Symbol.RIGHT_PAREN, 25),
                new Token("FROM", Keyword.FROM, 30), new Token("employee", Literal.IDENTIFIER, 39),
                new Token("GROUP", Keyword.GROUP, 45), new Token("BY", Keyword.BY, 48),
                new Token("state", Literal.IDENTIFIER, 54), new Token("HAVING", Keyword.HAVING, 61),
                new Token("MAX", Keyword.MAX, 65), new Token("(", Symbol.LEFT_PAREN, 66),
                new Token("salary", Literal.IDENTIFIER, 72), new Token(")", Symbol.RIGHT_PAREN, 73),
                new Token(">", Symbol.GT, 75), new Token("10", Literal.LONG, 78));

        List<Token> actual = new Tokenizer(
                "SELECT state, MAX(salary) FROM employee GROUP BY state HAVING MAX(salary) > 10")
                        .tokenize().getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void canTokenizeCompoundOperators() {
        List<Token> expected = Arrays.asList(new Token("a", Literal.IDENTIFIER, 1),
                new Token(">=", Symbol.GT_EQ, 4), new Token("b", Literal.IDENTIFIER, 6),
                new Token("OR", Keyword.OR, 9), new Token("a", Literal.IDENTIFIER, 11),
                new Token("<=", Symbol.LT_EQ, 14), new Token("b", Literal.IDENTIFIER, 16),
                new Token("OR", Keyword.OR, 19), new Token("a", Literal.IDENTIFIER, 21),
                new Token("<>", Symbol.LT_GT, 24), new Token("b", Literal.IDENTIFIER, 26),
                new Token("OR", Keyword.OR, 29), new Token("a", Literal.IDENTIFIER, 31),
                new Token("!=", Symbol.BANG_EQ, 34), new Token("b", Literal.IDENTIFIER, 36));

        List<Token> actual = new Tokenizer("a >= b OR a <= b OR a <> b OR a != b").tokenize()
                .getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void canTokenizeLongValues() {
        List<Token> expected = Arrays.asList(new Token("123456789", Literal.LONG, 9),
                new Token("+", Symbol.PLUS, 11), new Token("987654321", Literal.LONG, 21));

        List<Token> actual = new Tokenizer("123456789 + 987654321").tokenize().getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void canTokenizeFloatDoubleValues() {
        List<Token> expected = Arrays.asList(new Token("123456789.00", Literal.DOUBLE, 12),
                new Token("+", Symbol.PLUS, 14), new Token("987654321.001", Literal.DOUBLE, 28));

        List<Token> actual = new Tokenizer("123456789.00 + 987654321.001").tokenize().getTokens();

        assertEquals(expected, actual);
    }

    @Test
    public void canTokenizeTableGroup() {
        List<Token> expected = Arrays.asList(new Token("select", Keyword.SELECT, 6),
                new Token("*", Symbol.STAR, 8), new Token("from", Keyword.FROM, 13),
                new Token("group", Literal.IDENTIFIER, 19));

        List<Token> actual = new Tokenizer("select * from group").tokenize().getTokens();

        assertEquals(expected, actual);
    }
}