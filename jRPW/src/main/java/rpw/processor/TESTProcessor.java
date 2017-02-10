package rpw.processor;

import org.apache.log4j.Logger;

import rpw.common.BatchException;

public class TESTProcessor implements BatchProcessor {
    private static final Logger LOG = Logger.getLogger(TESTProcessor.class);
    
    String procName = null ;
    String[] iFlds = null ;
    String[] oFlds = null ;
    int nIFlds ;
    int nOFlds ;
    int nProc = 0 ;

    public TESTProcessor (String[] args, String[] ifs, String[] ofs) throws BatchException {
        init(args, ifs, ofs);
    }

    @Override
    public int init(String[] args, String[] ifs, String[] ofs) throws BatchException {
        LOG.trace("TESTProcessor.initBP");

        procName = args[0] ;
        iFlds = ifs ;
        oFlds = ofs ;
        nIFlds = iFlds.length;
        nOFlds = oFlds.length;
        nProc = 0;

        return 0;
    }

    @Override
    public int exec(String[] ivs, String[] ovs) throws BatchException {
        LOG.trace("TESTProcessor.doBP");
        
        for ( int i=0 ; i<nOFlds ; i++) {
            ovs[i] = i<nIFlds ? ivs[i] : "dummy"+oFlds[i] ;
        }
        nProc++ ;

        return 0;
    }

    @Override
    public int end() throws BatchException {
        LOG.trace("TESTProcessor.endBP");

        procName = null ;
        iFlds = null ;
        oFlds = null ;

        LOG.info("TESTProcessor.endBP: PROCESADOS " + nProc + " registros.");

        return 0;
    }

    @Override
    public int getNumInFields(){
        return iFlds.length;
    }

    @Override
    public int getNumOutFields(){
        return oFlds.length;
    }

}
