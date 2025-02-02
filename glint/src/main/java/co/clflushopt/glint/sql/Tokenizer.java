package co.clflushopt.glint.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Tokenize a SQL expression and return a stream of tokens.
 *
 */
public class Tokenizer {
    private final String sql;
    private int offset = 0;

    public Tokenizer(String sql) {
        this.sql = sql;
    }

    public TokenStream tokenize() {
        List<Token> tokens = new ArrayList<>();
        Token token = nextToken();
        while (token != null) {
            tokens.add(token);
            token = nextToken();
        }
        return new TokenStream(tokens);
    }

    private Token nextToken() {
        offset = skipWhitespace(offset);
        if (offset >= sql.length()) {
            return null;
        }

        char currentChar = sql.charAt(offset);
        if (SqlLiteral.isIdentifier(currentChar)) {
            var token = scanIdentifier(offset);
            offset = token.getEndOffset();
            return token;
        } else if (SqlLiteral.isNumber(currentChar)) {
            var token = scanNumber(offset);
            offset = token.getEndOffset();
            return token;
        } else if (Symbol.isSymbolStart(currentChar)) {
            var token = scanSymbol(offset);
            offset = token.getEndOffset();
            return token;
        } else if (SqlLiteral.isCharsStart(currentChar)) {
            var token = scanChars(offset, currentChar);
            offset = token.getEndOffset();
            return token;
        } else {
            offset++;
        }
        return null;
    }

    private int skipWhitespace(int startOffset) {
        return indexOfFirst(startOffset, ch -> !Character.isWhitespace(ch));
    }

    private Token scanNumber(int startOffset) {
        int endOffset;
        if (sql.charAt(startOffset) == '-') {
            endOffset = indexOfFirst(startOffset + 1, ch -> !Character.isDigit(ch));
        } else {
            endOffset = indexOfFirst(startOffset, ch -> !Character.isDigit(ch));
        }

        if (endOffset == sql.length()) {
            return new Token(sql.substring(startOffset, endOffset), SqlLiteral.LONG, endOffset);
        }

        boolean isFloat = sql.charAt(endOffset) == '.';
        if (isFloat) {
            endOffset = indexOfFirst(endOffset + 1, ch -> !Character.isDigit(ch));
        }

        return new Token(sql.substring(startOffset, endOffset),
                isFloat ? SqlLiteral.DOUBLE : SqlLiteral.LONG, endOffset);
    }

    private Token scanIdentifier(int startOffset) {
        if (sql.charAt(startOffset) == '`') {
            int endOffset = getOffsetUntilTerminatedChar('`', startOffset);
            return new Token(sql.substring(offset, endOffset), SqlLiteral.IDENTIFIER, endOffset);
        }

        int endOffset = indexOfFirst(startOffset, ch -> !SqlLiteral.isIdentifierPart(ch));
        String text = sql.substring(startOffset, endOffset);

        if (isAmbiguousIdentifier(text)) {
            return new Token(text, processAmbiguousIdentifier(endOffset, text), endOffset);
        } else {
            TokenType tokenType = SqlKeyword.textOf(text) != null ? SqlKeyword.textOf(text)
                    : SqlLiteral.IDENTIFIER;
            return new Token(text, tokenType, endOffset);
        }
    }

    private boolean isAmbiguousIdentifier(String text) {
        return SqlKeyword.ORDER.name().equalsIgnoreCase(text)
                || SqlKeyword.GROUP.name().equalsIgnoreCase(text);
    }

    private TokenType processAmbiguousIdentifier(int startOffset, String text) {
        int skipWhitespaceOffset = skipWhitespace(startOffset);
        if (skipWhitespaceOffset != sql.length()
                && sql.substring(skipWhitespaceOffset, skipWhitespaceOffset + 2)
                        .equalsIgnoreCase(SqlKeyword.BY.name())) {
            return SqlKeyword.textOf(text);
        }
        return SqlLiteral.IDENTIFIER;
    }

    private int getOffsetUntilTerminatedChar(char terminatedChar, int startOffset) {
        int nextOffset = sql.indexOf(terminatedChar, startOffset + 1);
        if (nextOffset == -1) {
            throw new RuntimeException("Must contain " + terminatedChar + " in remain sql["
                    + startOffset + " .. end]");
        }
        return nextOffset;
    }

    private Token scanSymbol(int startOffset) {
        int endOffset = indexOfFirst(startOffset, ch -> !Symbol.isSymbol(ch));
        String text = sql.substring(startOffset, endOffset);
        Symbol symbol = null;

        while ((symbol = Symbol.textOf(text)) == null && endOffset > startOffset) {
            text = sql.substring(offset, --endOffset);
        }

        return new Token(text, symbol, endOffset);
    }

    private Token scanChars(int startOffset, char terminatedChar) {
        int endOffset = getOffsetUntilTerminatedChar(terminatedChar, startOffset);
        return new Token(sql.substring(startOffset + 1, endOffset), SqlLiteral.STRING,
                endOffset + 1);
    }

    private int indexOfFirst(int startIndex, Predicate<Character> predicate) {
        for (int i = startIndex; i < sql.length(); i++) {
            if (predicate.test(sql.charAt(i))) {
                return i;
            }
        }
        return sql.length();
    }
}