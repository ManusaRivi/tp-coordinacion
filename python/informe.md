# Informe del Trabajo Práctico

En este informe se explican las decisiones tomadas para asegurar que el sistema logre escalar respecto a la cantidad de clientes y controles, así como la coordinación entre instancias de Sum y Aggregation.

## 1. Con más de un Cliente

Para lograr que el sistema funcione para más de un cliente, se parte del requerimiento de que cada cliente reciba el top de su propio archivo input de frutas.

Para eso, debe haber independencia entre las frutas dependiendo de qué cliente provienen, para discriminar cada top para cada cliente. Esto se logra mediante el uso de uuids: el gateway instancia un `MessageHandler` para cada conexion. Al inicializarse, el `MessageHandler` genera un `uuid` y lo persiste en su estado. Este `uuid` se agrega a cada mensaje en formato hexadecimal para optimizar los bytes viajando por la red.

Los Sum y Aggregators llevan sumas separadas para cada cliente usando diccionarios. De esta forma, cada cliente recibe el top de frutas solamente del archivo que envió, no se mezclan frutas de archivos distintos en el mismo top.

Al deserializar un resultado, el `MessageHandler` del gateway devuelve `None` si el mensaje contiene un `uuid` distinto del de la conexión correspondiente a ese cliente. Esto funciona, porque handle_client_response itera cada cliente hasta que el mensaje deserializado sea truthy.

## 2. Coordinación entre instancias Sum

Una o varias instancias de Sum van a recibir frutas del mismo cliente, pero solo una va a recibir el `EOF` de ese cliente, indicando que no hay más frutas para enviar.

Si el `EOF` es recibido, entonces todas las frutas del cliente fueron recibidas por una o más instancias. Por lo tanto, la instancia de Sum que recibe el `EOF` se encargará de notificar a las demás instancias de Sum que deben flushear sus sumas a los aggregators.

Para esto, utilizamos un exchange llamado `control_exchange`. Cada instancia de Sum se suscribe al exchange y las routing keys con las que interactuará serán correspondientes a las demás instancias de Sum presentes en el sistema, aprovechando que conocemos cuántas hay.

En este exchange se envian dos tipos de mensajes, definidos en common/message_protocol/internal.py:
1. `FLUSH_REQUEST (coordinator_id, client_id)`
    Este mensaje es enviado por el coordinador cuando recibe el EOF del Cliente. Las instancias que lo reciban deben enviar a su data_output_exchange el resultado obtenido para el cliente en cuestión.
2. `FLUSH_SUCCESS (client_id)`
    Este mensaje indica que un no-coordinador ha enviado sus datos al data_output_exchange con éxito. El coordinador va llevar la cuenta de los workers que terminaron para cada cliente, y una vez que la cantidad sea igual a la cantidad de workers en total, envía el EOF al aggregator.

Supuesto: ahora mismo, si un worker no tiene datos almacenados para el cliente a la hora de recibir FLUSH_REQUEST, asumo que es porque no recibió ninguna fruta de ese cliente, por ende debe darse como finalizado para ese cliente de todas formas.

Con un exchange por Sum worker funciona sin errores, pero:
FLUSH_REQUEST y FLUSH_SUCCESS se mandan a todos los exchanges.
FLUSH_REQUEST solo deberia mandarse a los demas exchanges.
FLUSH_SUCCESS solo deberia mandarse al coordinador.
Para eso, el mensaje FLUSH_REQUEST deberia incluir el ID del coordinador adentro, para que los otros workers puedan identificar quien es y pushear solo a ese exchange.

Posible mejora: abstraer el messaging en un `MessageHandler` del Sum.
