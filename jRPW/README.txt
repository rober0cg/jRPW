ASR
Analizador
 Sintactico
  Recursivo

Intento de disponer de un evaluador en tiempo de ejecución de expresiones matemáticas
Y a su vez de profundizar en las teorías de la progranmación orientada a objetos.

La base del ASR:
  expr = term [ + expr ] 
  term = fact [ * term ]
  fact = cons | var | func(expr) | expr

Los clases implementadas:
  Expresion
  Terminio
  Factor
  Factor-Constante
  Factor-Variable
  Factor-Funciion(Expresion)
  Factor-(Expresion)



