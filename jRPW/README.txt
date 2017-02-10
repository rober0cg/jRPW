RPW
Reader
 Processor
  Writer
  
Intento de disponer de una base de procesos batch con entrada y salidas configurables con soportes
de ficheros de campos separados por comas y campos de longitud fija, con soporte jdbc (oracle, mysql,
postgress, sqlite), y al que sólo hay que implementar la lógica de procesado por registro.

Proyecto maven.
En curso:
- avanzar en mejorar los ratios de calidad de sw (sonar)
- implementar el máximo de pruebas unitarias y seguir su covertura (jacoco)

Pendiente de implementar:
- lectura/escritura de ficheros json, xml.
- procesadores de WebService, Tuxedo/JOLT, RPC...
- paralelismo¿?

Y esto para avanzar en las teorías de la programación orientada a objetos.

Los clases implementadas:
  BatchReader -> CSVReader, FLVReader, JDBCRedader
  BatchWriter -> CSVWriter, FLVWriter, JDBCWriter
  BatchProcessor -> TESTProcessor, NAProcessor, JDBCPRocessor
  RPW (programa principal)

