/*
 * Gestion de Excepciones de la aplicacion para subirlas unificadas al programa principal
 */
package rpw.common;

/*
 * @author roberto
 * 
 */
public class BatchException extends Exception {
    static final long serialVersionUID = 73807901L;

    public BatchException( ) {
        super();
    }

    public BatchException( String msg ) {
        super(msg);
    }

    public BatchException( String msg, Throwable cause ) {
        super(msg, cause);
    }

    public BatchException( Throwable cause ) {
        super(cause);
    }
    
}
