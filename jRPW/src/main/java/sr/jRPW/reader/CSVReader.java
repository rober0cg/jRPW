package sr.jRPW.reader;

import org.apache.log4j.Logger;

import sr.jRPW.common.BatchException;


public class CSVReader extends FILEReader implements BatchReader {
    private static final Logger LOG=Logger.getLogger(CSVReader.class);

    String[] fs=null; // FieldsName
    int nf=0; // NumFields
    int rn=0; // RecNum

    public CSVReader(String[] args, String[] flds) throws BatchException {
        open(args,flds);
    }

    @Override
    public int open(String[] args, String[] flds) throws BatchException {
        LOG.trace("CSVReader.openBR");

        nf=flds.length;
        fs=flds;
        rn=0;

        super.open(args[0]);

        return 0;
    }

    @Override
    public int read(String[] vals) throws BatchException {
        LOG.trace("CSVReader.readBR");
        
        String ln=super.readLine();
        if (ln==null) {
            return -1;
        }

        String csvregex=",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
        String[] tk=ln.split(csvregex);
        for ( int i=0; i<nf; i++) {
            vals[i]=i<tk.length ? tk[i] : "";
        }
        rn++;
        
        return 0;
    }

    @Override
    public void close() throws BatchException {
        LOG.trace("CSVReader.closeBR");

        super.close();
        fn=null;
        fs=null;
        rn=0;
        
        return;
    }

  @Override
    public int getNumInFields(){
        return nf;
    }

}
