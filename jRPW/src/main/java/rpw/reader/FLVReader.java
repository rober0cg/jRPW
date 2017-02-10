package rpw.reader;

import org.apache.log4j.Logger;

import rpw.common.BatchException;
import rpw.common.FLVCommon;


/* 
 * Implementa primitivas open, read y close para lectura de fichero FLV
 * 
 * @author Roberto Carrillo Garcia (rober_cg@telefonica.net)
 * @version 0.1 - 2014/11/15
 * 
 */
public class FLVReader extends FILEReader implements BatchReader {
    private static final Logger LOG=Logger.getLogger(FLVReader.class);

    String[] fs=null; // FieldsName
    int[] fl=null; // FieldsLength
    int nf=0; // NumFields
    int rn=0; // RecNum

    // en flds vendran los campos y sus longitudes: CAMPO/LONG
    public FLVReader(String[] args, String[] flds) throws BatchException {
        open(args,flds);
    }

    @Override
    public int open(String[] args, String[] flds) throws BatchException {
        LOG.trace("FLVReader.openBR");

        nf = flds.length;
        fs=new String[nf];
        fl=new int[nf];
        FLVCommon.flvFields(flds, fs, fl);
        rn=0;

        super.open(args[0]);

        return nf;
    }

    @Override
    public int read(String[] vals) throws BatchException {
        LOG.trace("FLVReader.readBR");

        String ln=super.readLine();
        if (ln==null) {
            return -1;
        }

        int d=0; // desde y ...
        int h;   // ... hasta
        for ( int i=0; i<nf; i++) {
            if ( d<ln.length() ) {
                h = d + fl[i]; // hasta le sumo el ancho del campo
                vals[i] = ln.substring(d, h).trim();
                d = h; // el proximo desde es el hasta actual
            }
            else {
                vals[i] = "";
            }
        }
        rn++;
        return 0;
    }

    @Override
    public int getNumInFields(){
        return nf;
    }

}
