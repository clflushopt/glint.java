package co.clflushopt.glint.sql;

import java.util.List;
import java.util.logging.Logger;

/**
 * TokenStream is like an iterable but specialized for token processing.
 *
 */
public class TokenStream {
    private static final Logger logger = Logger.getLogger(TokenStream.class.getName());

    private final List<Token> tokens;
    private int index = 0;

    public TokenStream(List<Token> tokens) {
        this.tokens = tokens;
    }

    public List<Token> getTokens() {
        return tokens;
    }

    public Token peek() {
        if (index < tokens.size()) {
            return tokens.get(index);
        }
        return null;
    }

    public Token next() {
        if (index < tokens.size()) {
            return tokens.get(index++);
        }
        return null;
    }

    public boolean consumeKeywords(List<String> keywords) {
        int savedIndex = index;
        for (String keyword : keywords) {
            if (!consumeKeyword(keyword)) {
                index = savedIndex;
                return false;
            }
        }
        return true;
    }

    public boolean consumeKeyword(String keyword) {
        Token peek = peek();
        logger.fine("consumeKeyword('" + keyword + "') next token is " + peek);

        if (peek != null && peek.getType() instanceof SqlKeyword
                && peek.getText().equals(keyword)) {
            index++;
            logger.fine("consumeKeyword() returning true");
            return true;
        } else {
            logger.fine("consumeKeyword() returning false");
            return false;
        }
    }

    public boolean consumeTokenType(TokenType type) {
        Token peek = peek();
        if (peek != null && peek.getType().equals(type)) {
            index++;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            if (i > 0) {
                sb.append(" ");
            }
            if (i == index) {
                sb.append("*");
            }
            sb.append(tokens.get(i));
        }
        return sb.toString();
    }
}