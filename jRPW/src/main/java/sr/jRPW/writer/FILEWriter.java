package sr.jRPW.writer;

import sr.jRPW.common.BatchException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

public class FILEWriter {
    private static final Logger LOG=Logger.getLogger(FILEWriter.class);

    String fn=null; // FileName
    FileWriter fw=null;
    BufferedWriter bw=null;

    public FILEWriter() {
    }
    public FILEWriter(String fn) throws BatchException {
        open(fn);
    }

    public int open(String fn) throws BatchException {
        LOG.trace("FILEWriter.open "+fn);

        try {
            fw=new FileWriter(fn);
            bw=new BufferedWriter(fw);
        } catch (IOException e) {
            LOG.error("FILEWriter.open FileWriter", e);
            close();
            throw new BatchException("FILEWriter.open IOException");
        }
        return 0;
    }

    public void write(String ln) throws BatchException {
        LOG.trace("FILEWriter.write");
        try {
            bw.write(ln);
        } catch (IOException e) {
            LOG.error("FILEWriter.write: ", e);
            throw new BatchException("FILEWriter.write IOException");
        }
        return;
    }

    public void flush() throws BatchException {
        LOG.trace("FILEWriter.flush");
        try {
            bw.flush();
        } catch (IOException e) {
            LOG.error("FILEWriter.flush: ", e);
            throw new BatchException("FILEWriter.flush IOException");
        }
        return;
    }
    
    
    public void close() throws BatchException {
        LOG.trace("FILEWriter.close");
        try {
            if ( bw!=null) {
                bw.flush();
                bw.close();
                bw=null;
            }
            if ( fw!=null) {
                fw.close();
                fw=null;
            }
        } catch (IOException e) {
            LOG.error("FILEWriter.close: ", e);
            throw new BatchException("FILEWriter.close IOException");
        }
        return;
    }

    public String getFileName() {
        return fn;
    }
}
