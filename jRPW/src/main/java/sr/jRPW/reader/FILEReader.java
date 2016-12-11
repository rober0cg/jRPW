package sr.jRPW.reader;

import org.apache.log4j.Logger;

import sr.jRPW.common.BatchException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FILEReader {
    private static final Logger LOG=Logger.getLogger(FILEReader.class);

    String fn=null; // FileName
    FileReader fr=null;
    BufferedReader br=null;

    public FILEReader() {
    }

    public FILEReader(String fn) throws BatchException {
        this.open(fn);
    }

    public int open(String fn) throws BatchException {
        LOG.trace("FILEReader.open "+fn);
        try {
            fr=new FileReader(fn);
            br=new BufferedReader(fr);
        } catch (FileNotFoundException e) {
            LOG.error("FILEReader.open FileReader", e);
            throw new BatchException("FILEReader.open FileNotFoundException");
        }
        return 0;
    }

    public String readLine() throws BatchException {
        LOG.trace("FILEReader.readLine");
        String l;
        try {
            l = br.readLine();
        } catch (IOException e) {
            LOG.error("FILEReader.readLine: ", e);
            throw new BatchException("FILEReader.readLine IOException");
        }
        return l;
    }

    public void close() throws BatchException {
        LOG.trace("FILEReader.close");
        try {
            if ( br!=null) {
                br.close();
                br=null;
            }
            if ( fr!=null) {
                fr.close();
                fr=null;
            }
        } catch (IOException e) {
            LOG.error("FILEReader.close: ", e);
            throw new BatchException("FILEReader.close IOException");
        }
        return;
    }

    public String getFileName() {
        return fn;
    }
}
