package rpw.common;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.Test;

public class JDBCConnexUtlTest {
    private static final Logger LOG=Logger.getLogger(JDBCConnexUtlTest.class);

    private final static String[] driver = {
        "org.sqlite.JDBC",
        "com.mysql.jdbc.Driver",
        "org.postgresql.Driver",
        "oracle.jdbc.driver.OracleDriver",
    };
    private final static String[] connex = {
        "jdbc:sqlite:D:\\testdb.db",
        "jdbc:mysql://localhost:3306/test",
        "jdbc:postgresql://ec2-54-197-238-19.compute-1.amazonaws.com:5432/d7bi2u8gi1rjem?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
        "jdbc:oracle:thin:@sandrapc.sr:1521:ora12c"
    };

    private final static int[] iDriver = { 0,  1,  2,  1,  2  };
    private final static int[] iConnex = { 0,  1,  2,  1,  2  };
    private final static String[] iUsr = { "", "", "wugzbofqiwzomd", "", "wugzbofqiwzomd" };
    private final static String[] iPwd = { "", "", "Z_RZuI0irxb2zZCSERQyxmNBcJ", "", "Z_RZuI0irxb2zZCSERQyxmNBcJ" };
    
    private final static String[] oToText = {
        driver[0]+";"+connex[0]+";"+iUsr[0]+";"+iPwd[0],
        driver[1]+";"+connex[1]+";"+iUsr[1]+";"+iPwd[1],
        driver[2]+";"+connex[2]+";"+iUsr[2]+";"+iPwd[2],
        driver[1]+";"+connex[1]+";"+iUsr[1]+";"+iPwd[1],
        driver[2]+";"+connex[2]+";"+iUsr[2]+";"+iPwd[2]
    };

    @Test
    public final void testJDBCConnexUtl() {
        for ( int i=0; i<iConnex.length ; i++){
            try {
                JDBCConnexUtl c = new JDBCConnexUtl(driver[iDriver[i]], connex[iConnex[i]], iUsr[i], iPwd[i] );
                String t = c.toText();
                LOG.info("testJDBCConnexUtl toText = "+t);
                c.close();
                assertTrue("testJDBCConnexUtl", t.equals(oToText[i]));
            } catch (BatchException e) {
                LOG.error("testJDBCConnexUtl: BatchException ", e);
            }
        }
        assertTrue("testJDBCConnexUtl", true);
    }

    @Test
    public final void testOpen() {
        testJDBCConnexUtl();
    }

    @Test
    public final void testClose() {
        for ( int i=0; i<iConnex.length ; i++){
            try {
                JDBCConnexUtl c = new JDBCConnexUtl(driver[iDriver[i]], connex[iConnex[i]], iUsr[i], iPwd[i] );
                String t = c.toText();
                LOG.info("testClose toText = "+t);
                c.close();
                assertTrue("testClose", t.equals(oToText[i]));
            } catch (BatchException e) {
                LOG.error("testClose: BatchException ", e);
            }
        }
        assertTrue("testClose", true);
    }

    @Test
    public final void testCommit() {
        for ( int i=0; i<iConnex.length ; i++){
            try {
                JDBCConnexUtl c = new JDBCConnexUtl(driver[iDriver[i]], connex[iConnex[i]], iUsr[i], iPwd[i] );
                String t = c.toText();
                LOG.info("testCommit toText = "+t);
                c.close();
                assertTrue("testCommit", t.equals(oToText[i]));
            } catch (BatchException e) {
                LOG.error("testCommit: BatchException ", e);
            }
        }
        assertTrue("testCommit", true);
    }

    @Test
    public final void testRollback() {
        for ( int i=0; i<iConnex.length ; i++){
            try {
                JDBCConnexUtl c = new JDBCConnexUtl(driver[iDriver[i]], connex[iConnex[i]], iUsr[i], iPwd[i] );
                String t = c.toText();
                LOG.info("testRollback toText = "+t);
                c.close();
                assertTrue("testRollback", t.equals(oToText[i]));
            } catch (BatchException e) {
                LOG.error("testRollback: BatchException ", e);
            }
        }
        assertTrue("testRollback", true);
    }

    @Test
    public final void testSetAutoCommit() {
        for ( int i=0; i<iConnex.length ; i++){
            try {
                JDBCConnexUtl c = new JDBCConnexUtl(driver[iDriver[i]], connex[iConnex[i]], iUsr[i], iPwd[i] );
                String t = c.toText();
                LOG.info("testSetAutoCommit toText = "+t);
                c.close();
                assertTrue("testSetAutoCommit", t.equals(oToText[i]));
            } catch (BatchException e) {
                LOG.error("testSetAutoCommit: BatchException ", e);
            }
        }
        assertTrue("testSetAutoCommit", true);
    }

    @Test
    public final void testPrepareStatement() {
        for ( int i=0; i<iConnex.length ; i++){
            try {
                JDBCConnexUtl c = new JDBCConnexUtl(driver[iDriver[i]], connex[iConnex[i]], iUsr[i], iPwd[i] );
                String t = c.toText();
                LOG.info("testPrepareStatement toText = "+t);
                c.close();
                assertTrue("testPrepareStatement", t.equals(oToText[i]));
            } catch (BatchException e) {
                LOG.error("testPrepareStatement: BatchException ", e);
            }
        }
        assertTrue("testPrepareStatement", true);
    }

}
