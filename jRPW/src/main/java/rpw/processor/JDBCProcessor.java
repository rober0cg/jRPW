package rpw.processor;

import org.apache.log4j.Logger;

import rpw.common.BatchException;
import rpw.common.JDBCCommon;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCProcessor extends JDBCCommon implements BatchProcessor {
    private static final Logger LOG=Logger.getLogger(JDBCProcessor.class);

    String sSQL=null; // Query
    PreparedStatement prStmt = null;
    ResultSet rs=null;

    String[] iFld = null;
    String[] oFld = null;
    int nProc = 0 ;

    public JDBCProcessor(String[] args, String[] ifs, String[] ofs) throws BatchException {
        super(args[0], args[1], args[2], args[3]);
        init(args, ifs, ofs);
    }

    // JDBCProcessor.initBP args[]=
    //     args[0..3] = JDBC_DRIVER, DB_CONNECTION, DB_USER, DB_PASSWORD
    //     args[4] = SQL_STATEMENT
    @Override
    public int init(String[] args, String[] ifs, String[] ofs) throws BatchException {
        LOG.debug("JDBCPRocessor.initBP");
        iFld = ifs;
        oFld = ofs;
        sSQL = args[4];
        prStmt = super.statement(sSQL);
        super.setAutoCommit(false);
        
        return 0;
    }

    @Override
    public int exec(String[] iVals, String[] oVals) throws BatchException {
        LOG.debug("JDBCPRocessor.doBP");

        try {
         // stmt.setString
            LOG.debug("JDBCProcessor.doBP SQL = "+sSQL);
            for ( int i=0; i<iFld.length ; i++ ) {
                LOG.debug("JDBCProcessor.doBP in "+iFld[i]+" = "+iVals[i]);
                prStmt.setString(i+1,iVals[i]);
            }

         // stmt.executeQuery()
            rs = prStmt.executeQuery();

         // rs.next() + rs.getString
            boolean rsFound = rs.next();
            for ( int i=0; i<oFld.length ; i++ ) {
                oVals[i] = rsFound ? rs.getString(oFld[i]) : "" ;
                LOG.debug("JDBCProcessor.doBR out "+oFld[i]+"="+oVals[i]);
            }
            nProc++;
            
        } catch ( SQLException e ){
            LOG.error("JDBCProcessor.doBP executeQuery SQLException = "+sSQL+" exception = "+e);
        }

        return 0;
    }

    @Override
    public int end() throws BatchException {
        LOG.debug("JDBCPRocessor.closeBP");
        super.close();
        return 0;
    }

    @Override
    public int getNumInFields() {
        return iFld.length;
    }

    @Override
    public int getNumOutFields() {
        return oFld.length;
    }

}
