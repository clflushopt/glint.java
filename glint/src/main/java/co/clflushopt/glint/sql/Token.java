package co.clflushopt.glint.sql;

import java.util.Objects;

/**
 * SQL tokens representation.
 *
 */
public class Token {
    private final String text;
    private final TokenType type;
    private final int endOffset;

    public Token(String text, TokenType type, int endOffset) {
        this.text = text;
        this.type = type;
        this.endOffset = endOffset;
    }

    public String getText() {
        return text;
    }

    public TokenType getType() {
        return type;
    }

    public int getEndOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        String typeType = "";
        if (type instanceof SqlKeyword)
            typeType = "Keyword";
        else if (type instanceof Symbol)
            typeType = "Symbol";
        else if (type instanceof SqlLiteral)
            typeType = "Literal";

        return String.format("Token(\"%s\", %s.%s, %d)", text, typeType, type, endOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Token token = (Token) o;
        return endOffset == token.endOffset && Objects.equals(text, token.text)
                && Objects.equals(type, token.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(text, type, endOffset);
    }
}