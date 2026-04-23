# TP-COORDINACION

---

La solución implementa una arquitectura distribuida por etapas sobre RabbitMQ para calcular un top global de frutas por cliente.

El procesamiento se divide en tres filtros principales:
- Sum: acumula cantidades parciales por fruta para cada cliente y distribuye resultados hacia Aggregation.
- Aggregation: recibe resultados de múltiples instancias Sum, consolida por cliente y emite un top local.
- Join: recibe tops locales de todas las instancias Aggregation y calcula el top final global por cliente.


### Capa de Middleware y Modelo de Comunicación
La capa de middleware abstrae RabbitMQ con dos tipos principales:
- Colas (Queue): para consumo punto a punto.
- Exchanges (Direct/Fanout): para ruteo por clave o broadcast.

Cada consumidor trabaja con callback de mensaje que recibe tres elementos:
- message: payload
- ack: confirmación de procesamiento exitoso
- nack: rechazo con posibilidad de requeue

Además, el middleware define operaciones de ciclo de vida:
- start_consuming
- stop_consuming
- send
- close

Esto desacopla lógica de negocio del detalle de transporte.

--- 

### Componente Sum
#### Responsabilidad principal:

- Recibir registros de fruta por cliente.
- Acumular totales por fruta.
- Sincronizar fin de datos (EOF) por cliente.
- Enviar los acumulados de frutas a los Aggregation, de acuerdo a una función de partición.
- Enviar señal de control EOF a cada Aggregation para cerrar la etapa.

#### Lógica clave:

- Mantiene un diccionario por cliente con acumulados por fruta.
Tiene un hilo separado para consumir mensajes de control.
- Cuando detecta EOF de control para un cliente, espera un breve período para procesar los mensajes residuales.
- Luego envía cada fruta acumulada al aggregation correspondiente según una función de partición.
- Finalmente envía un EOF a todas las instancias de Aggregation para ese cliente.

#### Concurrencia:
Usa Condition para coordinar llegada de datos y señal de control.
El hilo de control y el hilo consumidor principal comparten estado por cliente.

#### Apagado:
- Implementa manejo de señales para shutdown ordenado.
- Detiene consumo, espera finalización del hilo de control y cierra.

--- 

### Aggregation
El componente Aggregation actúa como una etapa intermedia de reducción. Su objetivo es recibir los resultados parciales provenientes de Sum, consolidarlos por cliente y producir un top local que luego será enviado a Join.

#### Responsabilidades principales

Consumir mensajes de datos y de control (EOF) desde su cola de entrada.
Acumular cantidades por fruta para cada cliente.
Detectar cuándo recibió todos los EOF esperados de ese cliente.
Calcular el top local de frutas y enviarlo a la cola de salida.
Limpiar el estado en memoria del cliente una vez finalizado.

#### Estructuras de estado

totals_by_client: diccionario que guarda, para cada cliente, otro diccionario fruta -> total acumulado.
eof_received_by_client: contador de EOF recibidos por cliente.
finished_clients: conjunto de clientes ya cerrados, para ignorar datos tardíos y evitar reprocesamiento.

#### Flujo de procesamiento
Llega un mensaje.
Si es de datos (client_id, fruit, amount), se suma amount al acumulado de esa fruta para ese cliente.
Si es EOF, se incrementa el contador de EOF del cliente.
Cuando EOFs == SUM_AMOUNT, se considera completa la entrada para ese cliente.
Se ordenan las frutas por cantidad descendente, se toma TOP_SIZE y se serializa el top local.
Se publica el resultado hacia Join.
Se eliminan del estado interno los datos del cliente (contadores y acumulados), y se lo marca como finalizado.

#### Sincronización lógica
Aggregation implementa una barrera por cliente: no emite resultado final hasta recibir todas las señales de fin esperadas desde Sum. Esto garantiza consistencia, evitando calcular top con datos incompletos.

### Tolerancia a mensajes tardíos
Si llegan datos de un cliente que ya fue cerrado (finished_clients), se registran como condición anómala y no se reprocesan. Esto protege al sistema frente a desorden o retrasos en la red/middleware.

#### Salida de Aggregation
El mensaje de salida contiene:

client_id
fruit_top_serialized (lista de pares (fruit, amount))
Ese payload representa el top local de esa instancia de Aggregation y será combinado luego por Join para obtener el top global.


---
### Componente Join
#### Responsabilidad principal:

Recibir tops locales desde todas las instancias Aggregation.
Hacer barrera por cliente.
Generar top global final por cliente.
Enviar resultado al siguiente consumidor (por ejemplo gateway/cola de salida).
#### Lógica clave:

Para cada cliente, suma cantidades de frutas repetidas entre tops locales.
Lleva contador de cuántos tops locales llegaron.
Cuando llega la cantidad esperada (AGGREGATION_AMOUNT), ordena y selecciona TOP_SIZE.
Publica el top final y limpia estado del cliente.
En términos de patrón distribuido, Join funciona como un reducer final con sincronización por barrera.

#### Formato de Mensajes Internos
Se usa serialización JSON con dos formatos principales:

#### Mensaje de datos:
Lista con tres campos: client_id, fruit, amount.
Mensaje de control EOF:
Estructura de control con client_id.

Esto permite distinguir mensajes de flujo normal frente a mensajes de cierre por cliente.

---

### Formato de Mensajes Internos
Se usa serialización JSON con dos formatos principales:

#### Mensaje de datos:
Lista con tres campos: client_id, fruit, amount.
#### Mensaje de control EOF:
Estructura de control con client_id.
Esto permite distinguir mensajes de flujo normal frente a mensajes de cierre por cliente.

--- 
### Propiedades del Diseño

#### Escalabilidad horizontal:
Sum y Aggregation pueden tener múltiples instancias.
Tolerancia a desacople:
Cada etapa se comunica por colas/exchanges y no por llamada directa.
Sincronización por barrera:
EOF y contadores garantizan que cada etapa cierre por cliente cuando realmente recibió todo.
Limpieza de estado:
Cada cliente se elimina de memoria tras emitir resultado.


---
La implementación resuelve un pipeline distribuido de agregación en tres etapas, con coordinación por mensajes de control EOF y barreras por cliente. Sum realiza acumulación parcial y particionado, Aggregation consolida por shard hasta completar todos los EOF esperados, y Join fusiona los tops locales en un resultado global final. La abstracción de middleware desacopla la lógica de negocio de RabbitMQ, habilitando una arquitectura modular, escalable y mantenible.