package co.clflushopt.glint;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.logging.Logger;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        assertTrue(true);
    }

    /**
     * Asserts that Janino works.
     */
    @Test
    public void shouldEvaluateJavaCodeAtRuntime() {
        ICompilerFactory compiler = null;
        try {
            compiler = CompilerFactoryFactory
                    .getDefaultCompilerFactory(AppTest.class.getClassLoader());
            var ee = compiler.newExpressionEvaluator();

            // Sets the arguments to our code as a `double`
            Object[] args = { Double.valueOf(125.5) };
            ee.setExpressionType(double.class);
            ee.setParameters(new String[] { "total" }, new Class[] { double.class });

            try {
                ee.cook("total <= 100.0 ? 0.0 : 7.95");
            } catch (Exception e) {
                fail("failed to compile test expression: " + e.toString());
            }

            // Evaluate expression with actual parameter values.
            Object res = null;
            final Logger log = Logger.getAnonymousLogger();
            log.info("I'm starting");

            try {
                res = ee.evaluate(args);
            } catch (Exception e) {
                fail("failed to evaluate test expression");
            }

            var expected = Double.valueOf(7.95);
            var actual = (Double) res;

            assertTrue(String.format("Expected %f got %f", expected, actual),
                    expected.equals(actual));
        } catch (Exception e) {
            fail("creating expression evaluator throwed an exception:" + e.toString());
        }

    }
}
