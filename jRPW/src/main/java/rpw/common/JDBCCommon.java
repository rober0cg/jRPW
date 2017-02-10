/**
 * JDBCCommon
 * Refactoriza el código común entre JDBCReader y JDBCWriter 
 */
package rpw.common;

import org.apache.log4j.Logger;

import rpw.common.BatchException;
import rpw.common.JDBCConnexUtl;

import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author roberto
 *
 */
public class JDBCCommon extends JDBCConnexUtl {
    private static final Logger LOG=Logger.getLogger(JDBCCommon.class);

    private static List<PreparedStatement> aStmts = new ArrayList<>();
    
    public JDBCCommon(String drv, String con, String usr, String pwd ) throws BatchException {
        super( drv, con, usr, pwd );
    }

    // JDBCCommon.open args[]=
    //     args[0..3] = JDBC_DRIVER, DB_CONNECTION, DB_USER, DB_PASSWORD
    @Override
    public void open ( String drv, String con, String usr, String pwd ) throws BatchException {
        super.open(drv, con, usr, pwd );
        return;
    }

    public PreparedStatement statement ( String sql ) throws BatchException {
        LOG.debug("JDBCCommon.JDBCStmt SQL="+sql);
        PreparedStatement stmt;
        stmt= super.prepareStatement(sql);
        aStmts.add(stmt);
        return stmt;
    }
    
    @Override
    public void close () throws BatchException {
        LOG.trace("JDBCCommon.JDBCClose");
        for ( PreparedStatement stmt : aStmts ) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    LOG.error("JDBCCommon.close stmt close: SQLException ", e);
                }
            }
        }
        super.close();
        return;
    }

}
