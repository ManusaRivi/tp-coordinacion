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

El Thread esta consumiendo de un exchange. La mayor parte del tiempo esta bloqueado escuchando, en una operacion I/O. No hay intercalado de cómputo intensivo entre los threads, la mayor parte del tiempo el thread que consume del exchange de control está bloqueado. Cuando recibe un mensaje va a buscar tomar el lock y procesar el mensaje que recibió de forma atómica. Una vez que finaliza, se vuelve a escuchar.

Dada esta circunstancia, es aceptable el uso de un thread en lugar de un proceso aparte, ya que el GIL no va a perjudicarnos si cada thread está bloqueado consumiendo de un Middleware.

En este exchange se envian dos tipos de mensajes, definidos en common/message_protocol/internal.py:
1. `RECORD_COUNT_REQUEST (coordinator_id, client_id)`
    El coordinador envía este mensaje a los demás workers para contar el total de registros recolectados para un cliente.
2. `RECORD_COUNT_RESPONSE (client_id, record_count)`
    El worker le response al coordenador la cantidad de frutas que proceso,
3. `FLUSH_REQUEST (coordinator_id, client_id)`
    Este mensaje es enviado por el coordinador a los demás workers cuando recibe el `EOF` del Cliente. Las instancias que lo reciban deben enviar a su data_output_exchange el resultado obtenido para el cliente en cuestión, y luego responder enviando al coordinador un mensaje de `FLUSH_SUCCESS`.
4. `FLUSH_SUCCESS (client_id)`
    Este mensaje indica que un no-coordinador ha enviado sus datos al data_output_exchange con éxito. El coordinador va llevar la cuenta de cuántos workers terminaron para cada cliente, y una vez que la cantidad sea igual a la cantidad de workers en total, envía el EOF al aggregator (El coordinador se cuenta a sí mismo al recibir el `EOF`).

Nota: si un worker no tiene datos almacenados para el cliente a la hora de recibir `FLUSH_REQUEST`, asumo que es porque no recibió ninguna fruta de ese cliente, por ende debe darse como finalizado para ese cliente de todas formas. Como ya estamos verificando que el total de frutas recibidas en todos los workers concide con el total de frutas del cliente, esto no genera problemas.

Para asegurar que todos los datos sean correctamente enviados a los aggregators se implementa un sistema de dos pasadas:
La primera se ejecuta cuando un worker recibe un eof (y el total de registros) de un cliente. Va a pedirles a los demás workers que le digan cuántas frutas procesaron cada uno para ese cliente. Luego va a verificar que el total de todos los workers coincida con el total esperado.

Si el total procesado es menor que el esperado, deben haber datos aún esperando a ser procesados por alguno(s) de los workers. Si esto sucede, el coordinador espera un cierto tiempo parametrizado (`RETRY_SLEEP_SECONDS`) y luego reinicia la suma de registros. Va a reintentar un máximo de veces parametrizado (`MAX_RETRY_ATTEMPTS`), al superar esta cantidad de intentos los workers flushean sus datos a los aggregators. Serán resultados incompletos, pero son resultados. Otro criterio podría ser de no enviar nada al join (o un top vacío) y retornar un error al cliente.

## 3. Coordinación entre instancias Aggregator

Los workers sum mapean cada par cliente-fruta con un aggregator, usando la librería zlib. Es un mapeo determinístico, para asegurarnos que cada cliente-fruta vaya a parar al mismo worker aggregator. De esta forma, los tops de los aggregators son precisos. Para los eof, cada worker envía un eof a cada aggregator. A su vez, los workers aggregator llevan la cuenta de los eof recibidos de cada worker sum: si reciben un eof de cada worker sum (`SUM_AMOUNT`) pueden enviar sus datos al join.
