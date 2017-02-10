package rpw.common;

import org.apache.log4j.Logger;

import rpw.common.BatchException;

import java.util.*;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;

/**
 * Clase para implementar la reutilización de la conexión cuando tiene los mismos parámetros.
 *   sJdbcDriver
 *   sdbConnection
 *   sdbUser
 *   sdbPassword
 */
public class JDBCConnexUtl {
    private static final Logger LOG=Logger.getLogger(JDBCConnexUtl.class);

    String sJdbcDrv=null;
    String sdbUrl=null;
    String sdbUsr=null;
    String sdbPwd=null;
    private Connection dbConn=null;

    // ArrayList estáticos para gestión reutilización driver y conexion
    private static List<String> aJdbcDrivers = new ArrayList<>();
    private static List<ConnexArgs> aConnexArgs = new ArrayList<>();

    public JDBCConnexUtl(String drv, String url, String usr, String pwd) throws BatchException {
        open(drv, url, usr, pwd);
    }
    
    public void open ( String drv, String url, String usr, String pwd ) throws BatchException {
        iniDriver(drv);
        iniConnex(url, usr, pwd);
        return;
    }

    // cargar el driver de DB adecuado
    private int iniDriver ( String drv ) throws BatchException {
        LOG.trace("JDBCConnex.iniDriver DRIVER: "+drv);
        sJdbcDrv=drv;
        // buscar sJdbcDriver en aJdbcDrivers
        for ( String sDrv : aJdbcDrivers ) {
            if ( sJdbcDrv.equals(sDrv) ) { // encontrado, ya cargado.
                LOG.trace("JDBCConnex.iniDriver YA CARGADO:"+sJdbcDrv);
                return 0;
            }
        }
        // si llegamos aquí no está cargado: cargado y añadido a la lista
        LOG.trace("JDBCConnex.iniDriver CARGANDO:"+sJdbcDrv);
        try {
            Class.forName(sJdbcDrv);
        } catch (ClassNotFoundException e){
            LOG.error("JDBCConnex.iniDriver Class.forName", e);
            throw new BatchException("JDBCConnex.iniDriver Class.forName: ClassNotFoundException");
        }
        aJdbcDrivers.add(sJdbcDrv);
        return 0;
    }

    private int iniConnex ( String url, String usr, String pwd ) throws BatchException {
        LOG.trace("JDBCConnex.iniConnex");
        sdbUrl=url;
        sdbUsr=usr;
        sdbPwd=pwd;
        LOG.trace("JDBCConnex.iniConnex CONNEX: "+sdbUrl+","+sdbUsr);
        // buscar sdbConnection y sdbUser en aConnexArgs
        for ( ConnexArgs cArgs : aConnexArgs ) {
            if ( sdbUrl.equals(cArgs.getUrl()) && sdbUsr.equals(cArgs.getUsr())) { // encontrado.
                LOG.trace("JDBCConnex.iniConnex YA CONECTADO: "+sdbUrl+","+sdbUsr+","+cArgs.getNUse());
                dbConn = reuseConnex(cArgs);
                return 0;
            }
        }
        // conectar a la DB y guardar en la lista
        LOG.trace("JDBCConnex.iniConnex CONECTANDO: "+sdbUrl+","+sdbUsr);
        dbConn = createConnex(sdbUrl, sdbUsr, sdbPwd);
        return 0;
    }

    private Connection reuseConnex( ConnexArgs cArgs ){
        cArgs.incNUse();
        return cArgs.getConn();
    }
    private Connection createConnex( String url, String usr, String pwd ) throws BatchException {
        Connection c;
        try {
            c = DriverManager.getConnection(url, usr, pwd);
        } catch (SQLException e) {
            LOG.error("JDBCConnex.createConnex DrvMngr.getConnection", e);
            throw new BatchException("JDBCConnex.createConnex DrvMngr.getConnection: SQLException");
        }
        aConnexArgs.add(new ConnexArgs(url, usr, c));
        return c;
    }

    public String toText(){
        String str;
        str = sJdbcDrv+";"+sdbUrl+";"+sdbUsr+";"+sdbPwd;
        return str;
    }

    public void close() throws BatchException {
        this.endConnex();
    }

    private int endConnex() throws BatchException {
        LOG.trace("JDBCConnex.endConnex CONNEX: "+sdbUrl+","+sdbUsr);
        // buscar sdbConnection y sdbUser en aConnexArgs
        for ( ConnexArgs cArgs : aConnexArgs ) {
            if ( sdbUrl.equals(cArgs.getUrl()) && sdbUsr.equals(cArgs.getUsr())) { // encontrado.
                if ( cArgs.decNUse()>0 ) { // si tras decrementar no hay más, cerrar conexion
                    LOG.trace("JDBCConnex.endConnex DEJANDO DE USAR: "+sdbUrl+","+sdbUsr+","+cArgs.getNUse());
                }
                else {
                    LOG.trace("JDBCConnex.endConnex DESCONECTANDO: "+sdbUrl+","+sdbUsr+","+cArgs.getNUse());
                    removeConnex(cArgs);
                }
                break;
            }
        }
        return 0;
    }
    private void removeConnex ( ConnexArgs cArgs ) throws BatchException {
        LOG.trace("JDBCConnex.removeConnex");
        if (dbConn != null) {
            try {
                dbConn.close();
            } catch (SQLException e) {
                LOG.error("JDBCConnex.removeConnex dbConn.close", e);
                throw new BatchException("JDBCConnex.removeConnex dbConn.close: SQLException");
            }
            dbConn=null;
        }
        aConnexArgs.remove(cArgs);
        return;
    }

    public void commit() throws BatchException {
        LOG.trace("JDBCConnex.commit");
        try {
            dbConn.commit();
        } catch (SQLException e) {
            LOG.error("JDBCConnex.commit dbConn.commit", e);
            throw new BatchException("JDBCConnex.commit dbConn.commit: SQLException");
        }
    }

    public void rollback() throws BatchException {
        LOG.trace("JDBCConnex.rollback");
        try {
            dbConn.rollback();
        } catch (SQLException e) {
            LOG.error("JDBCConnex.rollback dbConn.rollback", e);
            throw new BatchException("JDBCConnex.rollback dbConn.rollback: SQLException");
        }
    }

    public void setAutoCommit(boolean c) throws BatchException {
        LOG.trace("JDBCConnex.setAutoCommit "+ (c ? "TRUE" : "FALSE"));
        try {
            dbConn.setAutoCommit(c);
        } catch (SQLException e) {
            LOG.error("JDBCConnex.setAutoCommit dbConn.setAutoCommit", e);
            throw new BatchException("JDBCConnex.setAutoCommit dbConn.setAutoCommit: SQLException");
        }
    }

    public PreparedStatement prepareStatement (String sql) throws BatchException {
        LOG.trace("JDBCConnex.prepareStatement "+ sql);
        try {
            return dbConn.prepareStatement(sql);
        } catch (SQLException e) {
            LOG.error("JDBCConnex.prepareStatement dbConn.prepareStatement", e);
            throw new BatchException("JDBCConnex.prepareStatement dbConn.prepareStatement: SQLException");
        }
    }
    
    class ConnexArgs {
        String sUrl=null;
        String sUsr=null;
        Connection dbConn=null;
        int nUse=0;
        ConnexArgs( String url, String usr, Connection con) {
            sUrl=url;
            sUsr=usr;
            dbConn=con;
            nUse = 1;
        }
        String getUrl(){
            return sUrl;
        }
        String getUsr(){
            return sUsr;
        }
        Connection getConn(){
            return dbConn;
        }
        int getNUse (){
            return nUse;
        }
        int incNUse() {
            return ++nUse;
        }
        int decNUse() {
            return --nUse;
        }
    }

}
