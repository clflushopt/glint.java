package co.clflushopt.glint.sql;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Symbols that are tokenized.
 *
 */
public enum Symbol implements TokenType {
    LEFT_PAREN("("), RIGHT_PAREN(")"), LEFT_BRACE("{"), RIGHT_BRACE("}"), LEFT_BRACKET("["),
    RIGHT_BRACKET("]"), SEMI(";"), COMMA(","), DOT("."), DOUBLE_DOT(".."), PLUS("+"), SUB("-"),
    STAR("*"), SLASH("/"), QUESTION("?"), EQ("="), GT(">"), LT("<"), BANG("!"), TILDE("~"),
    CARET("^"), PERCENT("%"), COLON(":"), DOUBLE_COLON("::"), COLON_EQ(":="), LT_EQ("<="),
    GT_EQ(">="), LT_EQ_GT("<=>"), LT_GT("<>"), BANG_EQ("!="), BANG_GT("!>"), BANG_LT("!<"),
    AMP("&"), BAR("|"), DOUBLE_AMP("&&"), DOUBLE_BAR("||"), DOUBLE_LT("<<"), DOUBLE_GT(">>"),
    AT("@"), POUND("#");

    private final String text;
    private static final Map<String, Symbol> SYMBOLS;
    private static final Set<Character> SYMBOL_STARTS;

    static {
        SYMBOLS = new HashMap<>();
        Set<Character> starts = new HashSet<>();

        for (Symbol symbol : values()) {
            SYMBOLS.put(symbol.text, symbol);
            // Add first character of each symbol to starts set
            starts.add(symbol.text.charAt(0));
        }

        SYMBOL_STARTS = Collections.unmodifiableSet(starts);
    }

    Symbol(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public static Symbol textOf(String text) {
        return SYMBOLS.get(text);
    }

    public static boolean isSymbol(char ch) {
        return SYMBOL_STARTS.contains(ch);
    }

    public static boolean isSymbolStart(char ch) {
        return isSymbol(ch);
    }
}