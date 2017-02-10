package rpw.common;

import org.apache.log4j.Logger;


/*
 * Clase para implementar el mapeo de campos entre dos arrays, uno de entrada y otro de salida
 * Se va a utilizar para el mapeo de los campos leídos por el Reader y la Entrada al Processor,
 * y en la salida de Processor y la entrada al Writer.
*/
public class EquivIO {
    private static final Logger LOG = Logger.getLogger(EquivIO.class);

    // equiv tendra tantos elementos como campos de salida
    // en cada posicion indicara el campo del array de entrada que equivale a esa posicion
    int[] aEquiv = null;
    int nEquiv = 0 ;

    public EquivIO ( String[] equiv) throws BatchException {
        init(equiv);
    }
    
/*
 *  entrada: array de String, en cada posición una pareja origen-destino
 *  siendo el primer elemento el 1
 *    EQUIV.READER-PROCESSOR=1-1,2-2
 *    EQUIV.PROCESSOR-WRITER=1-1,2-2,3-4,4-3,5-6,6-5,7-8,8-7,3-9,5-10
 */
    public int init ( String[] equiv) throws BatchException {
        LOG.trace("EquivIO.init");

        nEquiv = equiv.length;
        aEquiv = new int[nEquiv];

        String tokSep = "[,-]";
        try {
            for (int i=0; i<nEquiv; i++){
                String[] pareja = equiv[i].split(tokSep);
                int nIn = Integer.parseInt(pareja[0].trim())-1;
                int nOut = Integer.parseInt(pareja[1].trim())-1;
                aEquiv[nOut] = nIn;
            }
        } catch (NumberFormatException e) {
            LOG.error("EquivIO.init NumberFormatException: ", e);
            throw new BatchException("EquivIO.init NumberFormatException");
        }
        return nEquiv;
    }

/*
 * cargamos en el cada posición del array de salida el valor
 * del array de entrada de la posición indicada por aEquiv.
 */
    public int move(String[] ai, String[] ao) {
        LOG.trace("EquivIO.move");
        for ( int i=0; i<nEquiv; i++) {
            ao[i] = ai[aEquiv[i]];
        }
        return nEquiv;
    }

/*
 * limpiamos
 */
    public int end() {
        LOG.trace("EquivIO.end");
        aEquiv=null;
        nEquiv=0;
        return 0;
    }

    public int getNumFields(){
        return nEquiv;
    }

    public String toText() {
        String str = "";
        for (int i=0; i<nEquiv; i++){
            str += ( i==0 ? "" : "," ) + (i+1) + "-" + (aEquiv[i]+1) ;
        }
        return str;
    }
    
    
}
