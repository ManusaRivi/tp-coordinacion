# Informe del Trabajo Práctico

En este informe se explican las decisiones tomadas para asegurar que el sistema logre escalar respecto a la cantidad de clientes y controles, así como la coordinación entre instancias de Sum y Aggregation.

## 1. Con más de un Cliente

Para lograr que el sistema funcione para más de un cliente, se parte del requerimiento de que cada cliente reciba el top de su propio archivo input de frutas.

Para eso, debe haber independencia entre las frutas dependiendo de qué cliente provienen, para discriminar cada top para cada cliente. Esto se logra mediante el uso de uuids: el gateway instancia un `MessageHandler` para cada conexion. Al inicializarse, el `MessageHandler` genera un `uuid` y lo persiste en su estado. Este `uuid` se agrega a cada mensaje en formato hexadecimal para optimizar los bytes viajando por la red.

Los Sum y Aggregators llevan sumas separadas para cada cliente usando diccionarios. De esta forma, cada cliente recibe el top de frutas solamente del archivo que envió, no se mezclan frutas de archivos distintos en el mismo top.

Al deserializar un resultado, el `MessageHandler` del gateway devuelve `None` si el mensaje contiene un `uuid` distinto del de la conexión correspondiente a ese cliente. Esto funciona, porque handle_client_response itera cada cliente hasta que el mensaje deserializado sea truthy.
