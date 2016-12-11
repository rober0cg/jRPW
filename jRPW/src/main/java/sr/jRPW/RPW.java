package sr.jRPW;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import sr.jRPW.common.*;
import sr.jRPW.processor.*;
import sr.jRPW.reader.*;
import sr.jRPW.writer.*;

public final class RPW {
    private static final Logger LOG = Logger.getLogger(RPW.class);

    // Tipos de Lectura, Proceso y Escritura y sus nombres
    private static String iType=null, oType=null, pType=null;
    private static String[] pName=null, iName=null, oName=null;

    // Campos de entrada y salida, y sus equivalencias
    private static String[] riFlds=null, piFlds=null, poFlds=null, woFlds=null;
    private static String[] ripEquiv=null, powEquiv=null;

    // Objetos para la Lectura, Proceso, Escritura y Equivalencia de datos entre pasos
    private static BatchReader ir = null;
    private static BatchProcessor pr = null;
    private static BatchWriter ow = null;
    private static EquivIO eqRP = null; // Reader -> Processor
    private static EquivIO eqPW = null; // Processor -> Writer

    private RPW () {
        LOG.fatal("RPW.RPW constructor no debería ser usado.");
    }

    // Cuerpo principal de Inicializacion, Bucle de Proceso y Salida
    public static void main(String[] args) {
        LOG.trace("RPW.main: INI");

        try { // Carga parametros del fichero de properties
            String sProps = ( args.length < 1) ? "src/main/resources/RPW.properties" : args[0] ;
            LOG.trace("RPW.main: cargando parámetros: "+sProps);
            cargaParametros(sProps);
        } catch (BatchException e) {
            LOG.fatal("RPW.main: ERROR cargaParametros. ABORT", e);
            return;
        }

        try { // Creacion de las objetos de lectura, proceso y escritura adecuados
            LOG.trace("RPW.main: creando objetos INPUT, PROCESSOR, OUTPUT");
            ir = assignReader( iType, iName, riFlds);
            pr = assignProcessor(pType, pName, piFlds, poFlds);
            ow = assignWriter( oType, oName, woFlds);
            eqRP = new EquivIO(ripEquiv);
            eqPW = new EquivIO(powEquiv);
        } catch (BatchException e) {
            LOG.fatal("RPW.main: ERROR creando READER, PROCESSOR o WRITER. ABORT", e );
            cerrarObjetos();
            return;
        }

        String[] riVals = new String[ ir.getNumInFields() ];
        String[] piVals = new String[ pr.getNumInFields() ];
        String[] poVals = new String[ pr.getNumOutFields() ];
        String[] woVals = new String[ ow.getNumOutFields() ];

        try { // Bucle del proceso readInput - doProcessor - writeOutput
            LOG.trace("RPW.main: INI LOOP");
            while ( ir.read(riVals) == 0 ){
                eqRP.move(riVals, piVals);
                pr.exec(piVals, poVals);
                eqPW.move(poVals, woVals);
                ow.write(woVals);
                LOG.trace("RPW.main: NEXT LOOP");
            }
            LOG.trace("RPW.main: FIN LOOP");
        } catch (BatchException e) {
            LOG.fatal("RPW.main: ERROR BUCLE RPW. ABORT", e );
        }

        riVals = null;
        piVals = null;
        poVals = null;
        woVals = null;

        cerrarObjetos();

        LOG.trace("RPW.main: Bye!");
        return;
    }

    // Carga de los parametros de entrada
    private static void cargaParametros(String sProps) throws BatchException {
        LOG.trace("RPW.cargaParametros");

        try {
            // cargamos el archivo de propiedades
            InputStream fProps = new FileInputStream(sProps);
            Properties pProps = new Properties();
            pProps.load(fProps);

            String sDefCommaSep="\\s*,\\s*";
            cargaParametrosReader(pProps,sDefCommaSep);
            cargaParametrosProcessor(pProps,sDefCommaSep);
            cargaParametrosWriter(pProps,sDefCommaSep);
            cargaParametrosEquivalencia(pProps,sDefCommaSep);

            pProps = null ;
            if (fProps != null) {
                fProps.close();
                fProps = null;
            }

            // validaciones
            int nError=0;
            nError += validaParametrosInput(riFlds.length, piFlds.length, ripEquiv.length);
            nError += validaParametrosOutput(poFlds.length, woFlds.length, powEquiv.length);
            if ( nError!=0 ) {
                LOG.fatal("RPW.cargaParametros: validarParametros");
                throw new BatchException("RPW.cargaParametros: ERROR validarParametros");
            }
        }
        catch (FileNotFoundException e){
            LOG.fatal("RPW.cargaParametros: FileNotFoundException", e);
            throw new BatchException("RPW.cargaParametros: ERROR FileNotFoundException");
        }
        catch (IOException e){
            LOG.fatal("RPW.cargaParametros: IOException", e);
            throw new BatchException("RPW.cargaParametros: ERROR IOException");
        }
        return;
    }

    private static void cargaParametrosReader ( Properties pProps, String sDefCommaSep ) {
        iType = pProps.getProperty("INPUT.TYPE").trim();
        if ( "JDBC".equals(iType)) {
            iName = new String[5];
            iName[0] = pProps.getProperty("INPUT.JDBC_DRIVER").trim();
            iName[1] = pProps.getProperty("INPUT.DB_CONNECTION").trim();
            iName[2] = pProps.getProperty("INPUT.DB_USER").trim();
            iName[3] = pProps.getProperty("INPUT.DB_PASSWORD").trim();
            iName[4] = pProps.getProperty("INPUT.NAME").trim();
        }
        else {
            iName = new String[1];
            iName[0] = pProps.getProperty("INPUT.NAME").trim();
        }
        riFlds = pProps.getProperty("INPUT.FIELDS").trim().split(sDefCommaSep);
        return;
    }
    private static void cargaParametrosWriter ( Properties pProps, String sDefCommaSep ) {
        oType = pProps.getProperty("OUTPUT.TYPE").trim();
        if ( "JDBC".equals(oType)) {
            oName = new String[6];
            oName[0] = pProps.getProperty("OUTPUT.JDBC_DRIVER").trim();
            oName[1] = pProps.getProperty("OUTPUT.DB_CONNECTION").trim();
            oName[2] = pProps.getProperty("OUTPUT.DB_USER").trim();
            oName[3] = pProps.getProperty("OUTPUT.DB_PASSWORD").trim();
            oName[4] = pProps.getProperty("OUTPUT.NAME").trim();
            oName[5] = pProps.getProperty("OUTPUT.COMMIT-BLOCK").trim();
        }
        else {
            oName = new String[2];
            oName[0] = pProps.getProperty("OUTPUT.NAME").trim();
            oName[1] = pProps.getProperty("OUTPUT.COMMIT-BLOCK").trim();
        }
        woFlds = pProps.getProperty("OUTPUT.FIELDS").trim().split(sDefCommaSep);
        return;
    }
    private static void cargaParametrosProcessor ( Properties pProps, String sDefCommaSep ) {
        pType = pProps.getProperty("PROCESSOR.TYPE").trim();
        if ( "JDBC".equals(pType)) {
            pName = new String[5];
            pName[0] = pProps.getProperty("PROCESSOR.JDBC_DRIVER").trim();
            pName[1] = pProps.getProperty("PROCESSOR.DB_CONNECTION").trim();
            pName[2] = pProps.getProperty("PROCESSOR.DB_USER").trim();
            pName[3] = pProps.getProperty("PROCESSOR.DB_PASSWORD").trim();
            pName[4] = pProps.getProperty("PROCESSOR.NAME").trim();            }
        else {
            pName = new String[1];
            pName = pProps.getProperty("PROCESSOR.NAME").trim().split(sDefCommaSep);
        }
        piFlds = pProps.getProperty("PROCESSOR.INPUT-FIELDS").trim().split(sDefCommaSep);
        poFlds = pProps.getProperty("PROCESSOR.OUTPUT-FIELDS").trim().split(sDefCommaSep);
        return;
    }
    private static void cargaParametrosEquivalencia ( Properties pProps, String sDefCommaSep ) {
        ripEquiv = pProps.getProperty("EQUIV.READER-PROCESSOR").trim().split(sDefCommaSep);
        powEquiv = pProps.getProperty("EQUIV.PROCESSOR-WRITER").trim().split(sDefCommaSep);
        return;
    }
    
    // Valida longitudes arrays de campos de entrada y salida
    private static int validaParametrosInput( int nIR, int nIP, int nERP ) {
        LOG.trace("RPW.validaParametrosInput");
        int nError=0;

        // Comprobaciones de coherencia en cifras
        // numInputReader     >= numProcessorInput ; numProcessorInput == numEquivReaderProcessorInput
        if ( nIR==0 || nIP==0 || nIR < nIP ) {
            LOG.fatal("RPW.validaParametrosInput: INPUT.getNumInFields() < PROCESSOR.getNumInFields()");
            nError++;
        }
        if (nERP==0 || nIP != nERP ) {
            LOG.fatal("RPW.validaParametrosInput: PROCESSOR.getNumInFields() != EQUIV-RP.getNumFields()");
            nError++;
        }
        if ( nError > 0 ) {
            LOG.fatal("RPW.validaParametrosInput: errores coherencia");
        }
        return nError;
    }
    private static int validaParametrosOutput( int nOP, int nOW, int nEPW ) {
        LOG.trace("RPW.validaParametrosOutput");
        int nError=0;

        // Comprobaciones de coherencia en cifras
        // numProcessorOutput >= numOutputWriter   ; numEquivProcessorWriterOutput == numOutputWriter
        if ( nOP==0 || nOW==0 || nOP < nOW ) {
            LOG.fatal("RPW.validaParametrosOutput: PROCESSOR.getNumOutFields() < OUTPUT.getNumOutFields()");
            nError++;
        }
        if ( nEPW==0 || nEPW != nOW ) {
            LOG.fatal("RPW.validaParametrosOutput: EQUIV-PW.getNumFields() != OUTPUT.getNumOutFields()");
            nError++;
        }
        if ( nError > 0 ) {
            LOG.fatal("RPW.validaParametrosOutput: errores coherencia");
        }
        return nError;
    }

    private static BatchReader assignReader( String iType, String[] iName, String[] riFlds) throws BatchException {
        if ( "CSV".equals(iType) )
            return new CSVReader(iName, riFlds);
        if ( "FLV".equals(iType) )
            return new FLVReader(iName, riFlds);
        if ( "JDBC".equals(iType) ) 
            return new JDBCReader(iName, riFlds);
        throw new BatchException("RPW.assignReader tipo desconocido: "+iType);
    }

    private static BatchWriter assignWriter( String oType, String[] oName, String[] woFlds) throws BatchException {
        if ( "CSV".equals(oType) )
            return new CSVWriter(oName, woFlds);
        if ( "FLV".equals(oType) )
            return new FLVWriter(oName, woFlds);
        if ( "JDBC".equals(oType) )
            return new JDBCWriter(oName, woFlds);
        throw new BatchException("RPW.assignWriter tipo desconocido: "+oType);
    }

    private static BatchProcessor assignProcessor( String pType, String[] pName, String[] piFlds, String[] poFlds) throws BatchException {
        if ( "TEST".equals(pType) )
            return new TESTProcessor(pName, piFlds, poFlds);
        if ( "JDBC".equals(pType) )
            return new JDBCProcessor(pName, piFlds, poFlds);
        if ( "NA".equals(pType) )
            return new NAProcessor(pName, piFlds, poFlds);
        throw new BatchException("RPW.assignProcessor tipo desconocido: "+pType);
    }

    
    private static void cerrarObjetos() {
        try {// Limpiamos lo que hubiesemos abierto en Reader, Writer y Processor
            LOG.trace("RPW.main: saliendo");
            if (ir!=null)
                ir.close();
            if (pr!=null)
                pr.end();
            if (ow!=null)
                ow.close();
            if (eqRP!=null )
                eqRP.end();
            if (eqPW!=null )
                eqPW.end();
        } catch (BatchException e) {
            LOG.fatal("RPW.main: ERROR SALIENDO. ABORT", e );
        }
        ir = null;
        pr = null;
        ow = null;
        eqRP = null;
        eqPW = null;
        return;
    }

}
