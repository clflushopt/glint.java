package co.clflushopt.glint.sql;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TokenizerTest {

        @Test
        public void canTokenizeSimpleSelect() {
                List<Token> expected = Arrays.asList(new Token("SELECT", SqlKeyword.SELECT, 6),
                                new Token("id", SqlLiteral.IDENTIFIER, 9),
                                new Token(",", Symbol.COMMA, 10),
                                new Token("first_name", SqlLiteral.IDENTIFIER, 21),
                                new Token(",", Symbol.COMMA, 22),
                                new Token("last_name", SqlLiteral.IDENTIFIER, 32),
                                new Token("FROM", SqlKeyword.FROM, 37),
                                new Token("employee", SqlLiteral.IDENTIFIER, 46));

                List<Token> actual = new Tokenizer("SELECT id, first_name, last_name FROM employee")
                                .tokenize().getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void canTokenizeProjectionWithBinaryExpression() {
                List<Token> expected = Arrays.asList(new Token("SELECT", SqlKeyword.SELECT, 6),
                                new Token("salary", SqlLiteral.IDENTIFIER, 13),
                                new Token("*", Symbol.STAR, 15),
                                new Token("0.1", SqlLiteral.DOUBLE, 19),
                                new Token("FROM", SqlKeyword.FROM, 24),
                                new Token("employee", SqlLiteral.IDENTIFIER, 33));

                List<Token> actual = new Tokenizer("SELECT salary * 0.1 FROM employee").tokenize()
                                .getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void testProjectionWithAliasedBinaryExpression() {
                List<Token> expected = Arrays.asList(new Token("SELECT", SqlKeyword.SELECT, 6),
                                new Token("salary", SqlLiteral.IDENTIFIER, 13),
                                new Token("*", Symbol.STAR, 15),
                                new Token("0.1", SqlLiteral.DOUBLE, 19),
                                new Token("AS", SqlKeyword.AS, 22),
                                new Token("bonus", SqlLiteral.IDENTIFIER, 28),
                                new Token("FROM", SqlKeyword.FROM, 33),
                                new Token("employee", SqlLiteral.IDENTIFIER, 42));

                List<Token> actual = new Tokenizer("SELECT salary * 0.1 AS bonus FROM employee")
                                .tokenize().getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void canTokenizeSelectWithWhere() {
                List<Token> expected = Arrays.asList(new Token("SELECT", SqlKeyword.SELECT, 6),
                                new Token("a", SqlLiteral.IDENTIFIER, 8),
                                new Token(",", Symbol.COMMA, 9),
                                new Token("b", SqlLiteral.IDENTIFIER, 11),
                                new Token("FROM", SqlKeyword.FROM, 16),
                                new Token("employee", SqlLiteral.IDENTIFIER, 25),
                                new Token("WHERE", SqlKeyword.WHERE, 31),
                                new Token("state", SqlLiteral.IDENTIFIER, 37),
                                new Token("=", Symbol.EQ, 39),
                                new Token("CO", SqlLiteral.STRING, 44));

                List<Token> actual = new Tokenizer("SELECT a, b FROM employee WHERE state = 'CO'")
                                .tokenize().getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void canTokenizeSelectWithAggregates() {
                List<Token> expected = Arrays.asList(new Token("SELECT", SqlKeyword.SELECT, 6),
                                new Token("state", SqlLiteral.IDENTIFIER, 12),
                                new Token(",", Symbol.COMMA, 13),
                                new Token("MAX", SqlKeyword.MAX, 17),
                                new Token("(", Symbol.LEFT_PAREN, 18),
                                new Token("salary", SqlLiteral.IDENTIFIER, 24),
                                new Token(")", Symbol.RIGHT_PAREN, 25),
                                new Token("FROM", SqlKeyword.FROM, 30),
                                new Token("employee", SqlLiteral.IDENTIFIER, 39),
                                new Token("GROUP", SqlKeyword.GROUP, 45),
                                new Token("BY", SqlKeyword.BY, 48),
                                new Token("state", SqlLiteral.IDENTIFIER, 54));

                List<Token> actual = new Tokenizer(
                                "SELECT state, MAX(salary) FROM employee GROUP BY state").tokenize()
                                                .getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void canTokenizeSelectWithAggregatesAndHaving() {
                List<Token> expected = Arrays.asList(new Token("SELECT", SqlKeyword.SELECT, 6),
                                new Token("state", SqlLiteral.IDENTIFIER, 12),
                                new Token(",", Symbol.COMMA, 13),
                                new Token("MAX", SqlKeyword.MAX, 17),
                                new Token("(", Symbol.LEFT_PAREN, 18),
                                new Token("salary", SqlLiteral.IDENTIFIER, 24),
                                new Token(")", Symbol.RIGHT_PAREN, 25),
                                new Token("FROM", SqlKeyword.FROM, 30),
                                new Token("employee", SqlLiteral.IDENTIFIER, 39),
                                new Token("GROUP", SqlKeyword.GROUP, 45),
                                new Token("BY", SqlKeyword.BY, 48),
                                new Token("state", SqlLiteral.IDENTIFIER, 54),
                                new Token("HAVING", SqlKeyword.HAVING, 61),
                                new Token("MAX", SqlKeyword.MAX, 65),
                                new Token("(", Symbol.LEFT_PAREN, 66),
                                new Token("salary", SqlLiteral.IDENTIFIER, 72),
                                new Token(")", Symbol.RIGHT_PAREN, 73),
                                new Token(">", Symbol.GT, 75),
                                new Token("10", SqlLiteral.LONG, 78));

                List<Token> actual = new Tokenizer(
                                "SELECT state, MAX(salary) FROM employee GROUP BY state HAVING MAX(salary) > 10")
                                                .tokenize().getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void canTokenizeCompoundOperators() {
                List<Token> expected = Arrays.asList(new Token("a", SqlLiteral.IDENTIFIER, 1),
                                new Token(">=", Symbol.GT_EQ, 4),
                                new Token("b", SqlLiteral.IDENTIFIER, 6),
                                new Token("OR", SqlKeyword.OR, 9),
                                new Token("a", SqlLiteral.IDENTIFIER, 11),
                                new Token("<=", Symbol.LT_EQ, 14),
                                new Token("b", SqlLiteral.IDENTIFIER, 16),
                                new Token("OR", SqlKeyword.OR, 19),
                                new Token("a", SqlLiteral.IDENTIFIER, 21),
                                new Token("<>", Symbol.LT_GT, 24),
                                new Token("b", SqlLiteral.IDENTIFIER, 26),
                                new Token("OR", SqlKeyword.OR, 29),
                                new Token("a", SqlLiteral.IDENTIFIER, 31),
                                new Token("!=", Symbol.BANG_EQ, 34),
                                new Token("b", SqlLiteral.IDENTIFIER, 36));

                List<Token> actual = new Tokenizer("a >= b OR a <= b OR a <> b OR a != b")
                                .tokenize().getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void canTokenizeLongValues() {
                List<Token> expected = Arrays.asList(new Token("123456789", SqlLiteral.LONG, 9),
                                new Token("+", Symbol.PLUS, 11),
                                new Token("987654321", SqlLiteral.LONG, 21));

                List<Token> actual = new Tokenizer("123456789 + 987654321").tokenize().getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void canTokenizeFloatDoubleValues() {
                List<Token> expected = Arrays.asList(
                                new Token("123456789.00", SqlLiteral.DOUBLE, 12),
                                new Token("+", Symbol.PLUS, 14),
                                new Token("987654321.001", SqlLiteral.DOUBLE, 28));

                List<Token> actual = new Tokenizer("123456789.00 + 987654321.001").tokenize()
                                .getTokens();

                assertEquals(expected, actual);
        }

        @Test
        public void canTokenizeTableGroup() {
                List<Token> expected = Arrays.asList(new Token("select", SqlKeyword.SELECT, 6),
                                new Token("*", Symbol.STAR, 8),
                                new Token("from", SqlKeyword.FROM, 13),
                                new Token("group", SqlLiteral.IDENTIFIER, 19));

                List<Token> actual = new Tokenizer("select * from group").tokenize().getTokens();

                assertEquals(expected, actual);
        }
}