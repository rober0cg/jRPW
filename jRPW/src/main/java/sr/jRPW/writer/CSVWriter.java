package sr.jRPW.writer;

import org.apache.log4j.Logger;

import sr.jRPW.common.BatchException;


/* 
 * Implementa primitivas open, write y close para escritura de fichero CSV
 * 
 * @author Roberto Carrillo Garcia (rober_cg@telefonica.net)
 * @version 0.1 - 2014/11/15
 * 
 */
public class CSVWriter extends FILEWriter implements BatchWriter {
    private static final Logger LOG=Logger.getLogger(CSVWriter.class);

    String[] fs=null;  // Fields Name
    int nf=0;  // NumFields
    int rn=0;  // RecNum
    int bn=0;  // BlqNum
    
    public CSVWriter(String[] args, String[] flds) throws BatchException {
        open(args, flds);
    }

    @Override
    public int open(String[] args, String[] flds) throws BatchException {
        LOG.trace("CSVWriter.openBW");

        bn=Integer.parseInt(args[1]);
        if (bn<1)
            bn=1;

        nf=flds.length;
        fs=flds;
        rn=0;

        super.open(args[0]);

        return 0;
    }

    @Override
    public int write(String[] flds) throws BatchException {
        LOG.trace("CSVWriter.writeBW");
        super.write(stringArrayToCSVLine(flds, nf));
        if ((++rn%bn) == 0)
            super.flush();
        return 0;
    }
    private String stringArrayToCSVLine(String[] vals, int valn) {
        String str= "";
        if ( vals.length > 0) {
            str += quoteStringCSV(vals[0]);
            for ( int i=1; i<valn; i++ ) {
                str += "," + ( i<vals.length ? quoteStringCSV(vals[i]) : "" );
            }
        }
        str += "\n";
        return str;
    }
    private String quoteStringCSV(String val) {
        String csv="";
        if ( val!=null ) {
            if ( val.indexOf(',')!=-1 || val.indexOf(' ')!=-1 || val.indexOf('\\')!=-1 )
                csv="\"" + val + "\"";
            else
                csv=val;
        }
        return csv;
    }

    @Override
    public void close() throws BatchException {
        LOG.trace("CSVWriter.closeBW");
        super.flush();
        super.close();
        return;
    }

    @Override
    public int getNumOutFields(){
        return nf;
    }

}
