package co.clflushopt.glint.query.physical;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import co.clflushopt.glint.query.physical.expr.LiteralLongExpr;
import co.clflushopt.glint.query.physical.expr.LiteralStringExpr;

public class ExprTest {

    @Test
    public void testLiteralStringExpr() {
        LiteralStringExpr literalStringExpr = new LiteralStringExpr("test");
        assertEquals("'test'", literalStringExpr.toString());
    }

    @Test
    public void testLiteralStringExprEmpty() {
        LiteralStringExpr literalStringExpr = new LiteralStringExpr("");
        assertEquals("''", literalStringExpr.toString());
    }

    @Test
    public void testLiteralLongExpr() {
        LiteralLongExpr literalLongExpr = new LiteralLongExpr(1);
        assertEquals("1", literalLongExpr.toString());
    }

    @Test
    public void testLiteralLongExprZero() {
        LiteralLongExpr literalLongExpr = new LiteralLongExpr(0);
        assertEquals("0", literalLongExpr.toString());
    }

    @Test
    public void testLiteralLongExprNegative() {
        LiteralLongExpr literalLongExpr = new LiteralLongExpr(-1);
        assertEquals("-1", literalLongExpr.toString());
    }

    @Test
    public void testLiteralLongExprMax() {
        LiteralLongExpr literalLongExpr = new LiteralLongExpr(Long.MAX_VALUE);
        assertEquals(Long.toString(Long.MAX_VALUE), literalLongExpr.toString());
    }

    @Test
    public void testLiteralLongExprMin() {
        LiteralLongExpr literalLongExpr = new LiteralLongExpr(Long.MIN_VALUE);
        assertEquals(Long.toString(Long.MIN_VALUE), literalLongExpr.toString());
    }

}
