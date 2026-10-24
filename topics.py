"""topics - Topics de Kafka, particiones por pais, grupos de consumidores y utilidades de Kafka.

Lo usan los modulos que hablan por Kafka: WM_Central, WM_WS_E (Engine) y WM_FO.
El Monitor (WM_WS_M) NO habla Kafka: solo importa de aqui la lista de paises.

PARTICIONES POR PAIS
  Cada topic tiene UNA PARTICION POR PAIS (ver PAISES). Quien produce elige la particion del
  pais al que va dirigido el mensaje, y quien consume solo lee la particion de su pais:
    - Un Engine y un FO del pais "ES" solo ven los mensajes de "ES".
    - CENTRAL consume todas las particiones (gobierna todos los paises) y sabe de que pais
      viene cada mensaje por el numero de particion.
  IMPORTANTE: el orden de PAISES define el numero de particion; debe ser el mismo en todos
  los modulos (por eso es una constante del codigo y no una variable de entorno).

TOPICS (nombre -> quien produce -> quien consume)
  irrigation-request               FO / Engine  -> CENTRAL       peticion de riego
  progress-of-irrigation-request   CENTRAL              -> FO, Engine    pasos de la autorizacion
  reply-to-irrigation-request      CENTRAL              -> FO, Engine    autorizacion YES / NO
  station-command                  CENTRAL              -> Engine        START / BLOCK / ACTIVATE
  reply-to-station-command         Engine               -> CENTRAL       la WS acepta o rechaza el START
  station-status                   Engine               -> CENTRAL       estado (cambios + keep-alive)
  irrigation-flow-reading          Engine               -> CENTRAL, FO   caudal/volumen cada segundo
  irrigation-finished              Engine               -> CENTRAL       ticket de riego   (DURABLE)
  irrigation-summary               CENTRAL              -> FO            resumen final     (DURABLE)
  available-stations               CENTRAL              -> FO            WS disponibles

GRUPOS DE CONSUMIDORES (group.id)
  EFIMEROS (solo interesan los mensajes nuevos): el group.id lleva un sufijo aleatorio por
    ejecucion, asi que al arrancar no se reprocesan ordenes antiguas.
  DURABLES (no se puede perder ninguno aunque el consumidor este caido): group.id FIJO y
    offsets confirmados tras procesar cada mensaje (irrigation-finished e irrigation-summary).
"""
import json
import uuid

# ------------------------- Paises (una particion por pais) -------------------------
PAISES = ("ES", "FR", "PT", "IT", "DE")
PAIS_POR_DEFECTO = PAISES[0]


def es_pais_valido(pais):
    return pais in PAISES


def particion_del_pais(pais):
    """Numero de particion de un pais. Lanza ValueError si el pais no existe."""
    if pais not in PAISES:
        raise ValueError(f"pais desconocido: {pais!r} (validos: {', '.join(PAISES)})")
    return PAISES.index(pais)


def pais_de_la_particion(particion):
    """Pais de una particion (None si el numero no corresponde a ningun pais)."""
    if 0 <= particion < len(PAISES):
        return PAISES[particion]
    return None


# ------------------------- Topics de Kafka -------------------------
# Peticion de riego y sus respuestas
TOPIC_IRRIGATION_REQUEST = "irrigation-request"
TOPIC_PROGRESS_OF_IRRIGATION_REQUEST = "progress-of-irrigation-request"
TOPIC_REPLY_TO_IRRIGATION_REQUEST = "reply-to-irrigation-request"

# Ordenes de CENTRAL a las WS y su respuesta
TOPIC_STATION_COMMAND = "station-command"
TOPIC_REPLY_TO_STATION_COMMAND = "reply-to-station-command"

# Lo que publican las WS
TOPIC_STATION_STATUS = "station-status"
TOPIC_IRRIGATION_FLOW_READING = "irrigation-flow-reading"
TOPIC_IRRIGATION_FINISHED = "irrigation-finished"          # DURABLE

# Lo que publica CENTRAL para los operarios
TOPIC_IRRIGATION_SUMMARY = "irrigation-summary"            # DURABLE
TOPIC_AVAILABLE_STATIONS = "available-stations"

ALL_TOPICS = [TOPIC_IRRIGATION_REQUEST, TOPIC_PROGRESS_OF_IRRIGATION_REQUEST,
              TOPIC_REPLY_TO_IRRIGATION_REQUEST, TOPIC_STATION_COMMAND,
              TOPIC_REPLY_TO_STATION_COMMAND, TOPIC_STATION_STATUS,
              TOPIC_IRRIGATION_FLOW_READING, TOPIC_IRRIGATION_FINISHED,
              TOPIC_IRRIGATION_SUMMARY, TOPIC_AVAILABLE_STATIONS]

# ------------------------- Grupos de consumidores -------------------------
GROUP_CENTRAL_EVENTS = "central-events"                                 # efimero
GROUP_CENTRAL_FINISHED_IRRIGATIONS = "central-finished-irrigations"     # durable
GROUP_STATION_ENGINE = "station-engine"                                 # efimero; + "-<idWS>"
GROUP_OPERATOR_EVENTS = "operator-events"                               # efimero; + "-<operario>"
GROUP_OPERATOR_SUMMARIES = "operator-summaries"                         # durable; + "-<operario>"

# ------------------------- Ordenes del topic station-command -------------------------
CMD_START = "START"
CMD_BLOCK = "BLOCK"
CMD_ACTIVATE = "ACTIVATE"

# ------------------------- Respuestas del topic reply-to-irrigation-request -------------------------
AUTORIZADO = "YES"
DENEGADO = "NO"

# ------------------------- Estados de una WS (en los mensajes Kafka y en la BD) -------------------------
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_DE_SERVICIO = "FUERA_DE_SERVICIO"
DESCONECTADA = "DESCONECTADA"

# ------------------------- Estados de un ticket de riego -------------------------
RIEGO_COMPLETADO = "COMPLETADO"
RIEGO_INTERRUMPIDO = "INTERRUMPIDO"

# ------------------------- Parametros de Kafka -------------------------
REPLICAS_POR_TOPIC = 1
TIMEOUT_LISTAR_TOPICS_S = 5
SEGUNDOS_ESPERA_FLUSH = 5
SEGUNDOS_ESPERA_BUFFER_LLENO = 0.5


# ------------------------- Topics -------------------------
def ensure_topics(broker, topics=None):
    """Crea los topics que no existan (con una particion por pais) y amplia los que tengan
    menos particiones de las necesarias. Lanza excepcion si el broker no esta disponible."""
    from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

    topics = topics or ALL_TOPICS
    admin = AdminClient({"bootstrap.servers": broker})

    topics_existentes = admin.list_topics(timeout=TIMEOUT_LISTAR_TOPICS_S).topics  # falla si no hay broker

    topics_nuevos = [NewTopic(topic, num_partitions=len(PAISES), replication_factor=REPLICAS_POR_TOPIC)
                     for topic in topics if topic not in topics_existentes]
    if topics_nuevos:
        _esperar_resultados(admin.create_topics(topics_nuevos))

    # Topics creados antes con una sola particion: se amplian a una por pais
    particiones_a_ampliar = [NewPartitions(topic, len(PAISES))
                             for topic in topics
                             if topic in topics_existentes
                             and len(topics_existentes[topic].partitions) < len(PAISES)]
    if particiones_a_ampliar:
        _esperar_resultados(admin.create_partitions(particiones_a_ampliar))


def _esperar_resultados(resultados_por_topic):
    for topic, resultado in resultados_por_topic.items():
        try:
            resultado.result()
        except Exception as error:
            if "ALREADY_EXISTS" not in str(error):    # otro modulo pudo crearlo a la vez
                raise


# ------------------------- Productor -------------------------
class JsonProducer:
    """Envoltorio de confluent_kafka.Producer: send(topic, dict, pais) y flush()."""

    def __init__(self, broker):
        from confluent_kafka import Producer
        self.productor = Producer({"bootstrap.servers": broker,
                                   "acks": "all",
                                   "enable.idempotence": True})

    @staticmethod
    def _al_entregar(error, mensaje):
        if error is not None:
            print(f"[KAFKA] Error entregando mensaje a {mensaje.topic()}: {error}", flush=True)

    def send(self, topic, datos, pais):
        """Publica 'datos' (JSON) en la particion del pais. Devuelve False si no se pudo encolar."""
        try:
            particion = particion_del_pais(pais)
            contenido = json.dumps(datos).encode("utf-8")
            self._encolar(topic, contenido, particion)
            self.productor.poll(0)
            return True
        except Exception as error:
            print(f"[KAFKA] No se pudo publicar en {topic} (pais {pais}): {error}", flush=True)
            return False

    def _encolar(self, topic, contenido, particion):
        try:
            self.productor.produce(topic, contenido, partition=particion,
                                   on_delivery=self._al_entregar)
        except BufferError:
            # Cola interna llena: se deja entregar un poco y se reintenta una vez
            self.productor.poll(SEGUNDOS_ESPERA_BUFFER_LLENO)
            self.productor.produce(topic, contenido, partition=particion,
                                   on_delivery=self._al_entregar)

    def flush(self):
        try:
            self.productor.flush(SEGUNDOS_ESPERA_FLUSH)
        except Exception as error:
            print(f"[KAFKA] Error al vaciar la cola de envio: {error}", flush=True)


class SinKafka:
    """Productor 'nulo' para cuando todavia no hay conexion con el broker: no rompe el
    codigo que publica, solo deja constancia en el log."""

    def __init__(self, escribir_log):
        self.escribir_log = escribir_log

    def send(self, topic, datos, pais=None):
        self.escribir_log(f"[KAFKA] sin conexion con el broker: mensaje a {topic} descartado")
        return False

    def flush(self):
        pass


# ------------------------- Consumidor -------------------------
def make_consumer(broker, topics, group_id, ready=None, durable=False, pais=None):
    """Crea un consumer.

    pais=None : se suscribe (subscribe) a TODAS las particiones de los topics (CENTRAL).
    pais="ES" : lee SOLO la particion de ese pais en cada topic (Engine y FO).

    durable=False: group.id con sufijo aleatorio -> solo mensajes nuevos (sin reprocesar
                   ordenes antiguas).
    durable=True : group.id FIJO y offsets confirmados a mano con confirmar() -> si el modulo
                   cae, al volver recibe lo que se publico mientras estuvo caido (la primera
                   vez empieza por los mensajes nuevos).

    'ready' (un threading.Event) se activa cuando el consumer ya tiene sus particiones."""
    from confluent_kafka import OFFSET_END, Consumer, TopicPartition

    if durable:
        grupo = group_id
    else:
        grupo = f"{group_id}-{uuid.uuid4().hex[:8]}"

    consumer = Consumer({"bootstrap.servers": broker,
                         "group.id": grupo,
                         "auto.offset.reset": "latest",
                         "enable.auto.commit": False})

    if pais is None:
        def al_asignar_particiones(consumer_asignado, particiones):
            if ready is not None:
                ready.set()

        consumer.subscribe(topics, on_assign=al_asignar_particiones)
        return consumer

    # Particion fija del pais. Efimero: empieza por el final. Durable: sin offset explicito,
    # asi que retoma el ultimo offset confirmado del grupo (o el final si el grupo es nuevo).
    particion = particion_del_pais(pais)
    asignaciones = []
    for topic in topics:
        if durable:
            asignaciones.append(TopicPartition(topic, particion))
        else:
            asignaciones.append(TopicPartition(topic, particion, OFFSET_END))
    consumer.assign(asignaciones)
    if ready is not None:
        ready.set()
    return consumer


def confirmar(consumer, mensaje):
    """Confirma el offset de un mensaje YA procesado (consumers durables)."""
    try:
        consumer.commit(message=mensaje, asynchronous=False)
    except Exception as error:
        print(f"[KAFKA] No se pudo confirmar el offset: {error}", flush=True)


def kafka_loads(mensaje):
    """Mensaje de Kafka -> dict/list (o None si no es JSON valido)."""
    try:
        return json.loads(mensaje.value().decode("utf-8"))
    except Exception:
        return None
