package sr.jRPW.processor;


import org.apache.log4j.Logger;

import sr.jRPW.common.BatchException;

import com.telefonica.na.NACampo;
import com.telefonica.na.NAEntorno;
import com.telefonica.na.NAServicio;
import com.telefonica.na.NAWRException;


public class NAProcessor implements BatchProcessor {
    private static final Logger LOG = Logger.getLogger(NAProcessor.class);

    NAEntorno naEnv = null;
    NAServicio naSrv = null;

    String svNA = null ;
    String[] iFld = null ; int[] iIdx ;
    String[] oFld = null ; int[] oIdx ;
    int nProc = 0 ;

    public NAProcessor (String[] args, String[] ifs, String[] ofs) throws BatchException {
        init(args, ifs, ofs);
    }

    @Override
    public int init(String[] args, String[] ifs, String[] ofs) throws BatchException {
        LOG.trace("NAProcessor.initBP");

        svNA = args[0] ;
        iFld = ifs ;
        oFld = ofs ;
        nProc = 0;

        iIdx = new int[ iFld.length];
        oIdx = new int[ oFld.length];
        
        String idxsep = ":" ;

        for ( int i=0 ; i<iFld.length ; i++ ) {
            if ( iFld[i].indexOf(idxsep) < 0) {
                iIdx[i] = 0 ;
                LOG.debug("NAProcessor.initBP: iFld["+i+"] = "+iFld[i]);
            }
            else {
                String[] tk = iFld[i].split(idxsep);
                iFld[i] = tk[0];
                iIdx[i] = Integer.parseInt(tk[1]) ;
                LOG.debug("NAProcessor.initBP: iFld["+i+"] = "+iFld[i]+":"+iIdx[i]);
            }
        }

        for ( int i=0 ; i<oFld.length ; i++ ) {
            if ( oFld[i].indexOf(idxsep) < 0) {
                oIdx[i] = 0 ;
                LOG.debug("NAProcessor.initBP: oFld["+i+"] = "+oFld[i]);
            }
            else {
                String[] tk = oFld[i].split(idxsep);
                oFld[i] = tk[0];
                oIdx[i] = Integer.parseInt(tk[1]) ;
                LOG.debug("NAProcessor.initBP: oFld["+i+"] = "+oFld[i]+":"+oIdx[i]);
            }
        }
        
        try {
            naEnv = new NAEntorno("");
            naSrv = naEnv.creaNAServicio(svNA);
        } catch (NAWRException e) {
            LOG.error("NAProcessor.initBP NAEntorno/creaNAServicio", e);
            closeBP();
            throw new BatchException("NAProcessor.initBP NAEntorno/creaNAServicio NAWRException");
        }

        return 0;
    }

    @Override
    public int exec(String[] ivs, String[] ovs) throws BatchException {
        LOG.trace("NAProcessor.doBP");

        try {
            // Datos de entrada
            for ( int i=0 ; i<iFld.length ; i++ ) {
                if (iIdx[i]==0) {
                    naSrv.setCampo( iFld[i], new NACampo(ivs[i]) );
                }
                else {
                    naSrv.setCampo( iFld[i], new NACampo(ivs[i]), iIdx[i]) ;
                }
                LOG.debug("NAProcessor.doBP naSrv.setCampo: "+iFld[i]+ ( (iIdx[0]==0) ? "" : (":"+iIdx[i]) ) +"="+ivs[i]);
            }

            // Llamada al servicio
            naSrv.ejecutar();
            LOG.info("NAProcessor.doBP: naSrv.ejecutar OK - naSrv.estado ="+ naSrv.estado());

            // Datos de salida
            for ( int i=0 ; i<oFld.length ; i++ ) {
                if ( oIdx[i]==0) {
                    ovs[i] = naSrv.getCampo(oFld[i]).toString().trim();
                }
                else {
                    ovs[i] = naSrv.getCampo(oFld[i], oIdx[i]).toString().trim();
                }
                LOG.debug("NAProcessor.doBP naSrv.getCampo: "+oFld[i]+ ( (oIdx[i]==0) ? "" : (":"+oIdx[i]) ) +"="+ovs[i]);
            }
            nProc++;
        } catch (NAWRException e) {
            LOG.error("NAProcessor.doBP: NOK NA-ERROR-", e);
            ovs[0] = "NA-ERROR-" + e.getCodigo();
        }

        return 0;
    }

    @Override
    public int end() throws BatchException {
        LOG.trace("NAProcessor.endBP");
        closeBP();
        LOG.info("TEST.endBP: PROCESADOS " + nProc + " registros.");
        return 0;
    }

    private int closeBP() throws BatchException {
        LOG.trace("NAProcessor.closeBP");

        if ( naSrv != null ) {
            naSrv.unload();
            naSrv=null;
        }
        if ( naEnv != null ) {
            naEnv.unload();
            naEnv=null;
        }

        return 0;
    }
    
    @Override
    public int getNumInFields(){
        return iFld.length;
    }

    @Override
    public int getNumOutFields(){
        return oFld.length;
    }

}
