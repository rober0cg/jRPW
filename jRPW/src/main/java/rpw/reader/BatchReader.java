package rpw.reader;

import rpw.common.BatchException;

public interface BatchReader {

    // en funcion del origen args llevara el nombre de fichero, bbdd...
    public int open( String[] args, String[] flds ) throws BatchException;

    // objetivo devolver datos recuperados del fichero, tabla...
    public int read( String[] vals ) throws BatchException;

    // cerrar fichero, query...
    public void close() throws BatchException;
    
    // devolver el numero de campos de entrada
    public int getNumInFields();

}
