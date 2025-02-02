package co.clflushopt.glint.sql;

/**
 * Literals for (long, double, string datatypes).
 *
 */
public enum SqlLiteral implements TokenType {
    LONG, DOUBLE, STRING, IDENTIFIER;

    /**
     * Check if it's a digit or a decimal
     *
     * @param ch
     * @return
     */
    public static boolean isNumber(char ch) {
        return Character.isDigit(ch) || '.' == ch;
    }

    /**
     * Check if it's an identifier.
     *
     * @param ch
     * @return
     */
    public static boolean isIdentifier(char ch) {
        return Character.isLetter(ch);
    }

    /**
     * Check if we are within a keyword space.
     *
     * @param ch
     * @return
     */
    public static boolean isIdentifierPart(char ch) {
        return Character.isLetter(ch) || Character.isDigit(ch) || ch == '_';
    }

    /**
     * Check if we are within a literal space.
     *
     * @param ch
     * @return
     */
    public static boolean isCharsStart(char ch) {
        return '\'' == ch || '"' == ch;
    }
}