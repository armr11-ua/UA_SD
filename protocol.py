"""protocolos - Protocolo de SOCKETS <STX><DATA><ETX><LRC>.

Lo usan solo los modulos que hablan por sockets:
  Monitor <-> Central   (registro/autenticacion, latido, fugas)
  Monitor <-> Engine    (ID de la WS y comprobacion de salud cada segundo)

No depende de Kafka: los topics y el resto de utilidades de Kafka estan en topics.py.

Flujo de una conversacion (cliente -> servidor):
  <ENQ>                          el cliente abre la comunicacion
  <ACK>                          el servidor la acepta
  <STX>PETICION<ETX><LRC>        peticion
  <ACK>                          el servidor confirma que llego bien (o <NACK> si llego mal)
  <STX>RESPUESTA<ETX><LRC>       respuesta
  <ACK>                          el cliente confirma que llego bien (o <NACK> si llego mal)
  <EOT>                          cierre ordenado
"""

# ------------------------- Caracteres de control -------------------------
STX = b"\x02"    # comienzo del mensaje
ETX = b"\x03"    # final del mensaje
ENQ = b"\x05"    # inicia la comunicacion
ACK = b"\x06"    # confirma
NACK = b"\x15"   # el mensaje llego mal
EOT = b"\x04"    # final de la comunicacion

SEPARADOR_CAMPOS = "#"   # CODIGO#campo1#campo2...
SEP = SEPARADOR_CAMPOS   # alias corto, de uso frecuente

REINTENTOS_POR_DEFECTO = 3

# ------------------------- Mensajes Monitor -> Central -------------------------
REGISTER = "REGISTER"        # REGISTER#idWS#ubicacion#pais
HB = "HB"                    # HB#idWS            (latido)
LEAK = "LEAK"                # LEAK#idWS          (averia / fuga)
RECOVERED = "RECOVERED"      # RECOVERED#idWS     (averia resuelta)

# ------------------------- Respuestas de Central -------------------------
REGISTER_OK = "REGISTER_OK"
REGISTER_KO = "REGISTER_KO"  # REGISTER_KO#motivo (ID_INVALIDO, PAIS_INVALIDO, PAIS_DISTINTO,
                             #                     YA_CONECTADA, NO_REGISTRADA)
OK = "OK"
KO = "KO"                    # KO#motivo

# ------------------------- Monitor <-> Engine -------------------------
ID = "ID"                    # Monitor -> Engine: ID#idWS#pais (tras autenticarse en Central)
HEALTH = "HEALTH"            # Monitor -> Engine: HEALTH#CENTRAL_OK | HEALTH#CENTRAL_KO
CENTRAL_OK = "CENTRAL_OK"    #   (el Monitor le cuenta al Engine si Central esta alcanzable)
CENTRAL_KO = "CENTRAL_KO"    # Engine -> Monitor: OK | KO


def mensaje(*campos) -> str:
    """mensaje(REGISTER, 1, 'Parque') -> 'REGISTER#1#Parque'."""
    return SEPARADOR_CAMPOS.join(str(campo) for campo in campos)


# ------------------------- Trama -------------------------
def lrc(datos: bytes) -> int:
    """Byte de comprobacion: XOR byte a byte del mensaje."""
    resultado = 0
    for byte in datos:
        resultado ^= byte
    return resultado


def build_frame(texto: str) -> bytes:
    """Envuelve el mensaje: <STX><texto><ETX><LRC>."""
    datos = texto.encode("utf-8")
    return STX + datos + ETX + bytes([lrc(datos)])


def _recibir_un_byte(sock) -> bytes:
    byte = sock.recv(1)
    if not byte:
        raise ConnectionError("conexion cerrada")
    return byte


def _leer_hasta_etx(sock) -> bytes:
    """Lee los bytes del mensaje hasta encontrar <ETX> (que no se incluye)."""
    datos = bytearray()
    while True:
        byte = _recibir_un_byte(sock)
        if byte == ETX:
            return bytes(datos)
        datos += byte


# ------------------------- Conexion -------------------------
def handshake(sock):
    """Cliente: envia <ENQ> y espera <ACK>."""
    sock.sendall(ENQ)
    if _recibir_un_byte(sock) != ACK:
        raise ConnectionError("handshake rechazado")


def accept_handshake(sock):
    """Servidor: espera <ENQ> y responde <ACK>."""
    if _recibir_un_byte(sock) != ENQ:
        raise ConnectionError("se esperaba ENQ")
    sock.sendall(ACK)


def end_session(sock):
    """Cierre ordenado: envia <EOT> (si la conexion ya esta rota, no pasa nada)."""
    try:
        sock.sendall(EOT)
    except OSError:
        pass


# ------------------------- Mensajes -------------------------
def send_message(sock, texto: str, reintentos: int = REINTENTOS_POR_DEFECTO):
    """Envia <STX><texto><ETX><LRC> y espera <ACK>. Reintenta si recibe <NACK>."""
    trama = build_frame(texto)
    for _ in range(reintentos):
        sock.sendall(trama)
        if _recibir_un_byte(sock) == ACK:
            return
    raise ConnectionError("mensaje no confirmado (NACK)")


def receive_message(sock) -> str:
    """Lee una trama, valida el LRC y responde <ACK> (o <NACK> si el LRC no cuadra).

    Lanza OSError (ConnectionError / timeout) si se cierra o expira la conexion."""
    while True:
        byte = _recibir_un_byte(sock)

        if byte == ENQ:                      # el otro extremo abre una comunicacion
            sock.sendall(ACK)
            continue
        if byte == EOT:                      # el otro extremo cierra
            raise ConnectionError("EOT recibido")
        if byte != STX:                      # cualquier otro byte suelto se ignora
            continue

        datos = _leer_hasta_etx(sock)
        lrc_recibido = _recibir_un_byte(sock)[0]
        if lrc_recibido != lrc(datos):
            sock.sendall(NACK)               # llego mal: el emisor reintentara
            continue

        try:
            texto = datos.decode("utf-8")
        except UnicodeDecodeError:
            sock.sendall(NACK)               # no es texto valido: se trata como trama danada
            continue

        sock.sendall(ACK)
        return texto


def request(sock, texto: str) -> str:
    """Cliente: envia una peticion y devuelve la respuesta."""
    send_message(sock, texto)
    return receive_message(sock)
