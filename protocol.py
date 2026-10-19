"""protocol: topics de Kafka, estados, protocolo de sockets <STX><DATA><ETX><LRC>
y utilidades de Kafka"""
import json
import uuid

# ------------------------- Topics de Kafka -------------------------
# Central -> WS
WS_CONTROL = "WS_CONTROL"                    # START / BLOCK / ACTIVATE
# WS -> Central
WS_ACK = "WS_ACK"                            # respuesta de la WS a un START
WS_STATUS = "WS_STATUS"                      # estado de la WS (cambios + keep-alive)
WS_SUPPLY_COMPLETE = "WS_SUPPLY_COMPLETE"    # ticket de riego finalizado
# WS -> Central y FO
WS_CONSUMPTION = "WS_CONSUMPTION"            # caudal / volumen cada segundo
# FO (o WS manual) -> Central
FO_REQUEST = "FO_REQUEST"                    # peticion de riego {idOperator, idWS}
# Central -> FO
FO_NOTIFY = "FO_NOTIFY"                      # pasos del proceso de autorizacion
FO_AUTH = "FO_AUTH"                          # autorizacion YES/NO
FO_TICKET = "FO_TICKET"                      # resumen final del riego
WS_LIST = "WS_LIST"                          # WS disponibles

# ------------------------- Estados de una WS ------------------------- no tengo q estar cambiandolo en todos lados
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_DE_SERVICIO = "FUERA_DE_SERVICIO"
DESCONECTADA = "DESCONECTADA"

# ------------------------- Protocolo de sockets -------------------------
STX, ETX, ENQ, ACK, NACK, EOT = b"\x02", b"\x03", b"\x05", b"\x06", b"\x15", b"\x04"

#comienzo, final, inicia la comunicación, confirma, indica que el msj llegó mal, final de comunicación


#byte de comprobacion de msj
def lrc(data: bytes) -> int:
    """XOR byte a byte del mensaje."""
    r = 0
    for b in data:
        r ^= b
    return r

#construye la trama que envuelve al msj
def build_frame(msg: str) -> bytes:
    data = msg.encode("utf-8")
    return STX + data + ETX + bytes([lrc(data)])


def _recv1(sock) -> bytes:
    b = sock.recv(1)
    if not b:
        raise ConnectionError("conexion cerrada")
    return b

#inicio de conexión
def handshake(sock):
    """Cliente: <ENQ> -> espera <ACK>."""
    sock.sendall(ENQ)
    if _recv1(sock) != ACK:
        raise ConnectionError("handshake rechazado")

#aceptar conexión
def accept_handshake(sock):
    """Servidor: espera <ENQ> -> responde <ACK>"""
    if _recv1(sock) != ENQ:
        raise ConnectionError("se esperaba ENQ")
    sock.sendall(ACK)


def send_message(sock, msg: str, retries: int = 3):
    """Envia <STX><msg><ETX><LRC> y espera <ACK>. Reintenta si recibe <NACK>."""
    frame = build_frame(msg)
    for _ in range(retries):
        sock.sendall(frame)
        if _recv1(sock) == ACK:
            return
    raise ConnectionError("mensaje no confirmado (NACK)")


def receive_message(sock) -> str:
    """Lee una trama, valida el LRC y responde <ACK>/<NACK>.
    Lanza OSError (ConnectionError / timeout) si se cierra o expira la conexion."""
    while True:
        b = _recv1(sock)
        if b == ENQ:
            sock.sendall(ACK)
            continue
        if b == EOT:
            raise ConnectionError("EOT recibido")
        if b != STX:
            continue
        data = bytearray()
        while True:
            c = _recv1(sock)
            if c == ETX:
                break
            data += c
        recibido = _recv1(sock)[0]
        if recibido != lrc(bytes(data)):
            sock.sendall(NACK)
            continue
        sock.sendall(ACK)
        return data.decode("utf-8")


def request(sock, msg: str) -> str:
    """Cliente: envia una peticion y devuelve la respuesta."""
    send_message(sock, msg)
    return receive_message(sock)


# ------------------------- Utilidades Kafka -------------------------
ALL_TOPICS = [WS_CONTROL, WS_ACK, WS_STATUS, WS_SUPPLY_COMPLETE, WS_CONSUMPTION,
              FO_REQUEST, FO_NOTIFY, FO_AUTH, FO_TICKET, WS_LIST]


def ensure_topics(broker, topics=None):
    """Crea los topics si no existen. Lanza excepcion si el broker no esta disponible."""
    from confluent_kafka.admin import AdminClient, NewTopic
    topics = topics or ALL_TOPICS
    admin = AdminClient({"bootstrap.servers": broker})
    existing = set(admin.list_topics(timeout=5).topics)      # falla si no hay broker
    nuevos = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in existing]
    if nuevos:
        for t, fut in admin.create_topics(nuevos).items():
            try:
                fut.result()
            except Exception as e:
                if "ALREADY_EXISTS" not in str(e):
                    raise


class JsonProducer:
    """Envoltorio de confluent_kafka.Producer: send(topic, dict) y flush()."""

    def __init__(self, broker):
        from confluent_kafka import Producer
        self.p = Producer({"bootstrap.servers": broker})

    @staticmethod
    def _cb(err, msg):
        if err is not None:
            print(f"[KAFKA] Error entregando mensaje a {msg.topic()}: {err}", flush=True)

    def send(self, topic, data):
        raw = json.dumps(data).encode("utf-8")
        try:
            self.p.produce(topic, raw, on_delivery=self._cb)
        except BufferError:
            self.p.poll(0.5)
            self.p.produce(topic, raw, on_delivery=self._cb)
        self.p.poll(0)

    def flush(self):
        self.p.flush(5)


def make_consumer(broker, topics, group_prefix, ready=None):
    """Consumer que lee solo mensajes nuevos (group.id unico por ejecucion, sin reprocesar
    ordenes antiguas). 'ready' se activa cuando Kafka le asigna las particiones."""
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": broker,
                  "group.id": f"{group_prefix}_{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "latest",
                  "enable.auto.commit": False})

    def on_assign(consumer, partitions):
        if ready is not None:
            ready.set()

    c.subscribe(topics, on_assign=on_assign)
    return c


def kafka_loads(msg):
    """Mensaje de Kafka -> dict (o None si no es JSON valido)."""
    try:
        return json.loads(msg.value().decode("utf-8"))
    except Exception:
        return None