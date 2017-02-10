package rpw.writer;

import org.apache.log4j.Logger;

import rpw.common.BatchException;
import rpw.common.FLVCommon;


/* 
 * Implementa primitivas open, write y close para escritura de fichero FLV
 * 
 * @author Roberto Carrillo Garcia (rober_cg@telefonica.net)
 * @version 0.1 - 2014/11/15
 * 
 */
public class FLVWriter extends FILEWriter implements BatchWriter {
    private static final Logger LOG=Logger.getLogger(FLVWriter.class);
    
    String[] fs=null;  // Fields Name
    int[] fl=null;  // FieldLength
    int nf=0;  // NumFields
    int rn=0;  // RecNum
    int bn=0;  // BlqNum

    // en flds vendran los campos y sus longitudes: CAMPO/LONG
    public FLVWriter(String[] args, String[] flds) throws BatchException {
        open(args, flds);
    }

    @Override
    public int open(String[] args, String[] flds) throws BatchException {
        LOG.trace("FLVWriter.openBW");

        bn=Integer.parseInt(args[1]);
        if (bn<1)
            bn=1;

        nf=flds.length;
        fs=new String[nf];
        fl=new int[nf];
        FLVCommon.flvFields(flds, fs, fl);
        rn=0;

        super.open(args[0]);

        return nf;
    }

    @Override
    public int write(String[] flds) throws BatchException {
        LOG.trace("FLVWriter.writeBW");
        super.write(stringArrayToFLVLine(flds,nf));
        if ((++rn%bn) == 0)
            super.flush();
        return 0;
    }
    private String stringArrayToFLVLine (String[] vals, int valn) {
        String str= "";
        if ( vals.length > 0) {
            str += fixedStringFLV(vals[0],fl[0]);
            for ( int i=1; i< valn; i++ ) {
                str +=  fixedStringFLV( i<vals.length ? vals[i] : "" ,fl[i]);
            }
        }
        str += "\n";
        return str;
    }
    private String fixedStringFLV(String val, int lng){
        String flv;
        String fmt="%-"+lng+"."+lng+"s"; // %-12.12s
        if ( val!=null ) {
            flv=String.format(fmt, val);
        }
        else {
            flv=String.format(fmt, "");
        }
        return flv;
    }

    @Override
    public void close() throws BatchException {
        LOG.trace("FLVWriter.closeBW");
        super.flush();
        super.close();
        return;
    }

    @Override
    public int getNumOutFields(){
        return nf;
    }
    
}
