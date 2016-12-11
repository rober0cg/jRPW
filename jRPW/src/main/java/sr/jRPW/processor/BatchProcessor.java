package sr.jRPW.processor;

import sr.jRPW.common.BatchException;

public interface BatchProcessor {
    // inicialización: argumentos en función del procesador, campos de entrada y campos de salida
    public int init (String[] args, String[] inFlds, String[] outFlds) throws BatchException ;
    
    // procesador con los datos de entrada y los de salida
    public int exec (String[] inVals, String[] outVals) throws BatchException ;

    // fin: cierre conexiones, ficheros, acumulados...
    public int end () throws BatchException ;
    
    // devolver el número de campos de entrada y de salida
    public int getNumInFields();
    public int getNumOutFields();
}
