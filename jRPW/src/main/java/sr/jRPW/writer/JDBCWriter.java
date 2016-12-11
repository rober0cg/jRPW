/**
 * 
 */
package sr.jRPW.writer;

import org.apache.log4j.Logger;

import sr.jRPW.common.BatchException;
import sr.jRPW.common.JDBCCommon;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author roberto
 *
 */
public class JDBCWriter extends JDBCCommon implements BatchWriter {
    private static final Logger LOG=Logger.getLogger(JDBCWriter.class);

    String sSQL=null; // Query
    PreparedStatement prStmt = null;

    String[] fs=null; // FieldsName
    int[] fl=null; // FieldsLength
    int nf=0; // NumFields
    int rn=0; // RecNum
    int bn=0; // BlqNum

    public JDBCWriter(String[] args, String[] flds) throws BatchException {
        open(args,flds);
    }

    // JDBCWriter.openBW args[]=
    //     args[0..3] = JDBC_DRIVER, DB_CONNECTION, DB_USER, DB_PASSWORD
    //     args[4] = SQL_STATEMENT
    @Override
    public int open(String[] args, String[] flds) throws BatchException {
        LOG.trace("JDBCWriter.openBW");

        super.open(args[0], args[1], args[2], args[3]);

        // preparar la query
        sSQL=args[4];
        prStmt = super.statement(sSQL);
        LOG.debug("JDBCWriter.openBW SQL="+sSQL);

        // el commmit lo controlamos en writeBW
        super.setAutoCommit(false);

        nf=flds.length;
        fs=flds;
        rn=0;
        bn=Integer.parseInt(args[5]);
        if (bn<1)
            bn=1;

        return nf;
    }

    @Override
    public int write(String[] vals) throws BatchException {
        LOG.trace("JDBCWriter.writeBW");
        try {
            LOG.debug("JDBCWriter.writeBW SQL="+sSQL);
            for ( int i=0; i<nf ; i++ ) {
                LOG.debug("JDBCWriter.writeBW "+fs[i]+"="+vals[i]);
                prStmt.setString(i+1,vals[i]);
            }
            prStmt.executeUpdate();
            if ((++rn%bn) == 0)
                super.commit();
        }
        catch (SQLException e) {
            LOG.error("JDBCWriter.writeBW ps.setString/ps.execUpdate/db.commit ERROR", e);
        }
        return 0;
    }

    @Override
    public void close() throws BatchException {
        LOG.trace("JDBCWriter.closeBW");
        super.commit();
        super.close();
        return;
    }

    @Override
    public int getNumOutFields() {
        return nf;
    }

}
