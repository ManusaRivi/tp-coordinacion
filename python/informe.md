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

Para esto, utilizamos un exchange llamado `control_exchange`. Cada instancia de Sum se suscribe al exchange y las routing keys con las que interactuará serán correspondientes a las demás instancias de Sum presentes en el sistema, aprovechando que conocemos cuántas hay. Cada worker lanza un thread para consumir de forma concurrente de este exchange y de su respectiva `input_queue`.

JUSTIFICAR THREADING EN LUGAR DE MULTIPROCESSING EN ESTE USE CASE
El Thread esta consumiendo de un exchange. La mayor parte del tiempo esta bloqueado escuchando, en una operacion I/O.

Cuando le toca operar, manda lo que tiene que mandar y termina. No esta siendo usado para intercalar computo intensivo entre el thread que procesa datos de la `input_queue` y el que consume del `control_exchange`.

En este exchange se envian dos tipos de mensajes, definidos en common/message_protocol/internal.py:
1. `FLUSH_REQUEST (coordinator_id, client_id)`
    Este mensaje es enviado por el coordinador a los demás workers cuando recibe el `EOF` del Cliente. Las instancias que lo reciban deben enviar a su data_output_exchange el resultado obtenido para el cliente en cuestión, y luego responder enviando al coordinador un mensaje de `FLUSH_SUCCESS`.
2. `FLUSH_SUCCESS (client_id)`
    Este mensaje indica que un no-coordinador ha enviado sus datos al data_output_exchange con éxito. El coordinador va llevar la cuenta de cuántos workers terminaron para cada cliente, y una vez que la cantidad sea igual a la cantidad de workers en total, envía el EOF al aggregator (El coordinador se cuenta a sí mismo al recibir el `EOF`).

Supuesto: ahora mismo, si un worker no tiene datos almacenados para el cliente a la hora de recibir FLUSH_REQUEST, asumo que es porque no recibió ninguna fruta de ese cliente, por ende debe darse como finalizado para ese cliente de todas formas.

Posible mejora: abstraer el messaging en un `MessageHandler` del Sum.

## 3. Coordinación entre instancias Aggregator

Los Sum dejan de broadcastear a todos los aggregators: cada par cliente-fruta se mapea a un aggregator (lo mismo para el EOF).

De esa forma no se duplican los datos y no tenemos tops que no son reales.

El joiner junta un top por cada aggregator por cada cliente.

Los aggregators se coordinan entre si.
