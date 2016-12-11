package sr.jRPW.common;

import org.apache.log4j.Logger;

import sr.jRPW.common.BatchException;


public class FLVCommon {
    private static final Logger LOG=Logger.getLogger(FLVCommon.class);
    
    private FLVCommon(){
    }
    
    public static int flvFields (String[] flds, String[] fs, int[] fl ) throws BatchException {
        LOG.trace("FLVCommon.flvFields");
        try {
            for (int i=0; i<flds.length; i++) {
                String[] tk=flds[i].split("/");
                fs[i]=tk[0];
                fl[i]=Integer.valueOf(tk[1]);
            }
        } catch ( NumberFormatException e ) {
            LOG.fatal("FLVCommon.flvFields Integer.valueOf NumberFormatException", e);
            throw new BatchException("FLVCommon.flvFields FieldsLength NumberFormatException");
        }
        return flds.length;
    }

}
