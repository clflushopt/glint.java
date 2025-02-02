package co.clflushopt.glint.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Parser {
    private static final Logger logger = Logger.getLogger(Parser.class.getSimpleName());

    private final TokenStream tokens;

    public Parser(TokenStream tokens) {
        this.tokens = tokens;
        logger.setLevel(Level.FINE);
    }

    public SqlExpression parse() {
        return parse(0);
    }

    public int nextPrecedence() {
        Token token = tokens.peek();
        if (token == null) {
            return 0;
        }

        int precedence;
        TokenType type = token.getType();

        if (type == SqlKeyword.AS || type == SqlKeyword.ASC || type == SqlKeyword.DESC) {
            precedence = 10;
        } else if (type == SqlKeyword.OR) {
            precedence = 20;
        } else if (type == SqlKeyword.AND) {
            precedence = 30;
        } else if (type == Symbol.LT || type == Symbol.LT_EQ || type == Symbol.EQ
                || type == Symbol.BANG_EQ || type == Symbol.GT_EQ || type == Symbol.GT) {
            precedence = 40;
        } else if (type == Symbol.PLUS || type == Symbol.SUB) {
            precedence = 50;
        } else if (type == Symbol.STAR || type == Symbol.SLASH) {
            precedence = 60;
        } else if (type == Symbol.LEFT_PAREN) {
            precedence = 70;
        } else {
            precedence = 0;
        }

        logger.fine("nextPrecedence(" + token + ") returning " + precedence);
        return precedence;
    }

    public SqlExpression parsePrefix() {
        logger.fine("parsePrefix() next token = " + tokens.peek());
        Token token = tokens.next();
        if (token == null) {
            return null;
        }

        SqlExpression expr;
        TokenType type = token.getType();

        if (type == SqlKeyword.SELECT) {
            expr = parseSelect();
        } else if (type == SqlKeyword.CAST) {
            expr = parseCast();
        } else if (type == SqlKeyword.MAX) {
            expr = new SqlIdentifier(token.getText());
        } else if (type == SqlKeyword.INT || type == SqlKeyword.DOUBLE) {
            expr = new SqlIdentifier(token.getText());
        } else if (type == SqlLiteral.IDENTIFIER) {
            expr = new SqlIdentifier(token.getText());
        } else if (type == SqlLiteral.STRING) {
            expr = new SqlString(token.getText());
        } else if (type == SqlLiteral.LONG) {
            expr = new SqlLong(Long.parseLong(token.getText()));
        } else if (type == SqlLiteral.DOUBLE) {
            expr = new SqlDouble(Double.parseDouble(token.getText()));
        } else {
            throw new IllegalStateException("Unexpected token " + token);
        }

        logger.fine("parsePrefix() returning " + expr);
        return expr;
    }

    public SqlExpression parseInfix(SqlExpression left, int precedence) {
        logger.fine("parseInfix() next token = " + tokens.peek());
        Token token = tokens.peek();
        if (token == null) {
            throw new IllegalStateException("Unexpected end of input");
        }

        SqlExpression expr;
        TokenType type = token.getType();

        if (type == Symbol.PLUS || type == Symbol.SUB || type == Symbol.STAR || type == Symbol.SLASH
                || type == Symbol.EQ || type == Symbol.GT || type == Symbol.LT) {
            tokens.next(); // consume the token
            expr = new SqlBinaryExpr(left, token.getText(), parse(precedence));
        } else if (type == SqlKeyword.AS) {
            tokens.next(); // consume the token
            expr = new SqlAliasExpr(left, parseIdentifier());
        } else if (type == SqlKeyword.AND || type == SqlKeyword.OR) {
            tokens.next(); // consume the token
            expr = new SqlBinaryExpr(left, token.getText(), parse(precedence));
        } else if (type == SqlKeyword.ASC || type == SqlKeyword.DESC) {
            tokens.next();
            expr = new SqlSort(left, type == SqlKeyword.ASC);
        } else if (type == Symbol.LEFT_PAREN) {
            if (left instanceof SqlIdentifier) {
                tokens.next(); // consume the token
                List<SqlExpression> args = parseExprList();
                Token next = tokens.next();
                if (next == null || next.getType() != Symbol.RIGHT_PAREN) {
                    throw new IllegalStateException("Expected right parenthesis");
                }
                expr = new SqlFunction(((SqlIdentifier) left).getId(), args);
            } else {
                throw new IllegalStateException("Unexpected LPAREN");
            }
        } else {
            throw new IllegalStateException("Unexpected infix token " + token);
        }

        logger.fine("parseInfix() returning " + expr);
        return expr;
    }

    private List<SqlExpression> parseOrder() {
        List<SqlExpression> sortList = new ArrayList<>();
        SqlExpression sort = parseExpr();

        while (sort != null) {
            if (sort instanceof SqlIdentifier) {
                sort = new SqlSort(sort, true);
            } else if (!(sort instanceof SqlSort)) {
                throw new IllegalStateException(
                        "Unexpected expression " + sort + " after order by.");
            }

            sortList.add((SqlSort) sort);

            if (tokens.peek() != null && tokens.peek().getType() == Symbol.COMMA) {
                tokens.next();
            } else {
                break;
            }
            sort = parseExpr();
        }
        return sortList;
    }

    private SqlCast parseCast() {
        if (!tokens.consumeTokenType(Symbol.LEFT_PAREN)) {
            throw new IllegalStateException("Expected left parenthesis");
        }

        SqlExpression expr = parseExpr();
        if (expr == null) {
            throw new RuntimeException("Expected expression in CAST");
        }

        if (!(expr instanceof SqlAliasExpr)) {
            throw new IllegalStateException("Expected AS in CAST");
        }

        SqlAliasExpr alias = (SqlAliasExpr) expr;

        if (!tokens.consumeTokenType(Symbol.RIGHT_PAREN)) {
            throw new IllegalStateException("Expected right parenthesis");
        }

        return new SqlCast(alias.getExpr(), alias.getAlias());
    }

    private SqlSelectStmt parseSelect() {
        List<SqlExpression> projection = parseExprList();

        if (!tokens.consumeKeyword("FROM")) {
            throw new IllegalStateException("Expected FROM keyword, found " + tokens.peek());
        }

        SqlExpression tableExpr = parseExpr();
        if (!(tableExpr instanceof SqlIdentifier)) {
            throw new IllegalStateException("Expected table name");
        }
        SqlIdentifier table = (SqlIdentifier) tableExpr;

        // parse optional WHERE clause
        SqlExpression filterExpr = null;
        if (tokens.consumeKeyword("WHERE")) {
            filterExpr = parseExpr();
        }

        // parse optional GROUP BY clause
        List<SqlExpression> groupBy = Collections.emptyList();
        if (tokens.consumeKeywords(Arrays.asList("GROUP", "BY"))) {
            groupBy = parseExprList();
        }

        // parse optional HAVING clause
        SqlExpression havingExpr = null;
        if (tokens.consumeKeyword("HAVING")) {
            havingExpr = parseExpr();
        }

        // parse optional ORDER BY clause
        List<SqlExpression> orderBy = Collections.emptyList();
        if (tokens.consumeKeywords(Arrays.asList("ORDER", "BY"))) {
            orderBy = parseOrder();
        }

        return new SqlSelectStmt(projection, filterExpr, groupBy, orderBy, havingExpr,
                table.getId());
    }

    private List<SqlExpression> parseExprList() {
        logger.fine("parseExprList()");
        List<SqlExpression> list = new ArrayList<>();
        SqlExpression expr = parseExpr();

        while (expr != null) {
            list.add(expr);

            if (tokens.peek() != null && tokens.peek().getType() == Symbol.COMMA) {
                tokens.next();
            } else {
                break;
            }
            expr = parseExpr();
        }

        logger.fine("parseExprList() returning " + list);
        return list;
    }

    private SqlExpression parseExpr() {
        return parse(0);
    }

    private SqlIdentifier parseIdentifier() {
        SqlExpression expr = parseExpr();
        if (expr == null) {
            throw new RuntimeException("Expected identifier, found EOF");
        }
        if (!(expr instanceof SqlIdentifier)) {
            throw new RuntimeException("Expected identifier, found " + expr);
        }
        return (SqlIdentifier) expr;
    }

    protected SqlExpression parse(int precedence) {
        SqlExpression left = parsePrefix();
        if (left == null) {
            return null;
        }

        while (precedence < nextPrecedence()) {
            left = parseInfix(left, nextPrecedence());
        }

        return left;
    }
}