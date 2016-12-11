/**
 * 
 */
package sr.jRPW.reader;

import org.apache.log4j.Logger;

import sr.jRPW.common.BatchException;
import sr.jRPW.common.JDBCCommon;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author roberto
 *
 */
public class JDBCReader extends JDBCCommon implements BatchReader {
    private static final Logger LOG=Logger.getLogger(JDBCReader.class);

    String sSQL=null; // Query
    PreparedStatement prStmt = null;
    ResultSet rs=null;

    String[] fs=null; // FieldsName
    int[] fl=null; // FieldsLength
    int nf=0; // NumFields
    int rn=0; // RecNum

    public JDBCReader(String[] args, String[] flds) throws BatchException {
        open(args, flds);
    }

    // JDBCReader.openBR args[]=
    //     args[0..3] = JDBC_DRIVER, DB_CONNECTION, DB_USER, DB_PASSWORD
    //     args[4] = SQL_STATEMENT
    @Override
    public int open(String[] args, String[] flds) throws BatchException {
        LOG.trace("JDBCReader.openBR");

        super.open(args[0], args[1], args[2], args[3]);

        // preparar la query
        sSQL=args[4];
        prStmt = super.statement(sSQL);
        LOG.debug("JDBCReader.openBR SQL="+sSQL);

        try {
            // ejecutarla y dejar preparado el ResultSet
            rs = prStmt.executeQuery();
        } catch (SQLException e) {
            LOG.error("JDBCReader.openBR getConn/prepareStmt/execQry SQLException", e);
            close();
            throw new BatchException("JDBCReader.openBR getConn/prepareStmt/execQry SQLException");
        }

        nf=flds.length;
        fs=flds;
        rn=0;

        return nf;
    }

    @Override
    public int read(String[] vals) throws BatchException {
        LOG.trace("JDBCReader.readBR");

        try {
            LOG.debug("JDBCReader.readBR SQL="+sSQL+" - FETCH NEXT");
            if (rs.next()) {
                for ( int i=0; i<nf ; i++ ) {
                    vals[i] = rs.getString(fs[i]);
                    LOG.debug("JDBCReader.readBR "+fs[i]+"="+vals[i]);
                }
                rn++;
            }
            else {
                return -1;
            }
        }
        catch (SQLException e) {
            LOG.error("JDBCReader.readBR rs.next/rs.getString SQLException", e);
        }

        return 0;
    }

    @Override
    public void close() throws BatchException {
        LOG.trace("JDBCReader.closeBR");

        super.close();
        nf=0;
        fs=null;
        rn=0;
        
        return;
    }

    @Override
    public int getNumInFields() {
        return nf;
    }

}
