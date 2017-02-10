package rpw.common;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.Test;

public class EquivIOTest {
    private static final Logger LOG = Logger.getLogger(EquivIOTest.class);

    private static final String[][] iTest= { {"1-1","2-2","3-3"}, {"1-2","2-1","3-4","4-3"} };
    private static final String[] oToText= {  "1-1,2-2,3-3",       "1-2,2-1,3-4,4-3"        };
    private static final int[] oGetNumFld= {  3,                   4                        };

    private static final String[][] iMove = { { "uno", "dos", "tres" }, { "uno", "dos", "tres", "cuatro" } };
    private static final String[][] oMove = { { "uno", "dos", "tres" }, { "dos", "uno", "cuatro", "tres" } };
    
    
    @Test
    public final void testEquivIO() {
        testInit();
    }

    @Test
    public final void testInit() {
        for ( int i=0 ; i<iTest.length; i++) {
            try {
                EquivIO e = new EquivIO(iTest[i]);
                String t = e.toText();
                LOG.trace("testInit = "+t);
                assertTrue("testInit", t.equals(oToText[i]));
            }
            catch (BatchException e) {
                LOG.trace("testInit exception"+e);
            }
        }
    }

    @Test
    public final void testMove() {
        for ( int i=0; i<iTest.length; i++) {
            try {
                EquivIO e = new EquivIO(iTest[i]);
                String[] t = new String[iTest[i].length];
                int n = e.move(iMove[i], t);
                for ( int j=0; j<n; j++) {
                    LOG.trace("testMove t["+j+"] = "+t[j]);
                    assertTrue("testMove", t[j].equals(oMove[i][j]));
                }
            }
            catch (BatchException e) {
                LOG.trace("testMove exception"+e);
            }
        }
    }

    @Test
    public final void testEnd() {
        for ( int i=0 ; i<iTest.length; i++) {
            try {
                EquivIO e = new EquivIO(iTest[i]);
                String t = e.toText();
                LOG.trace("testEnd = "+t);
                e.end();
                t = e.toText();
                LOG.trace("testEnd = "+t);
                assertTrue("testEnd", t.isEmpty());
            }
            catch (BatchException e) {
                LOG.trace("testEnd exception"+e);
            }
        }
    }

    @Test
    public final void testGetNumFields() {
        for ( int i=0 ; i<iTest.length; i++) {
            try {
                EquivIO e = new EquivIO(iTest[i]);
                String t = e.toText();
                int n = e.getNumFields();
                LOG.trace("testGetNumFields = "+t+" -> "+n);
                assertTrue("testGetNumFields", n==oGetNumFld[i]);
            }
            catch (BatchException e) {
                LOG.trace("testGetNumFields exception"+e);
            }
        }
    }

}
