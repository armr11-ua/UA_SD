"""WM_WS_E - Engine de la Watering Station (caudalimetro + electrovalvula simulados).

Uso: python WM_WS_E.py <broker_ip:puerto> <monitor_ip:puerto>

- Se conecta al Monitor por sockets (protocolos.py); el Monitor le asigna el ID (ya autenticado
  en CENTRAL) y le pregunta cada segundo por su salud (responde OK / KO). En esa pregunta el
  Monitor tambien le dice si WM_Central esta alcanzable.
- Recibe ordenes de CENTRAL por Kafka (topics.py): START / BLOCK / ACTIVATE. Publica estado,
  consumo (cada segundo) y ticket final. Su PAIS (que le comunica el Monitor junto al ID) decide
  la particion de Kafka que usa: solo lee y escribe en la particion de su pais.
- Resiliencia:
    * Si se pierde el Monitor, el riego en curso finaliza y la WS queda AVERIADA (no admite
      riegos, ni siquiera manuales) hasta que el Monitor vuelva.
    * Si WM_Central deja de estar alcanzable, el riego en curso termina y no se admiten
      nuevos ("Imposible conectar con WM_Central").
    * El riego en curso se guarda en disco (WS_STATE_DIR, por defecto el directorio actual).
      Si el Engine cae en pleno riego, al volver envia el ticket final con lo ya regado.
- Menu por consola: 1 = simular fuga (KO) / reparar, 2 = riego manual,
  3 = finalizar riego, 4 = estado, q = salir.
"""
import json
import os
import random
import socket
import sys
import threading
import time

from confluent_kafka import KafkaError, KafkaException

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from protocol import (
    CENTRAL_OK, HEALTH, ID, KO, OK, SEP,
    handshake, receive_message, send_message,
)
from topics import (
    AUTORIZADO, CMD_ACTIVATE, CMD_BLOCK, CMD_START,
    DISPONIBLE, FUERA_DE_SERVICIO, REGANDO, RIEGO_COMPLETADO, RIEGO_INTERRUMPIDO,
    GROUP_STATION_ENGINE,
    TOPIC_IRRIGATION_FINISHED, TOPIC_IRRIGATION_FLOW_READING, TOPIC_IRRIGATION_REQUEST,
    TOPIC_PROGRESS_OF_IRRIGATION_REQUEST, TOPIC_REPLY_TO_IRRIGATION_REQUEST,
    TOPIC_REPLY_TO_STATION_COMMAND, TOPIC_STATION_COMMAND, TOPIC_STATION_STATUS,
    PAISES, es_pais_valido,
    JsonProducer, ensure_topics, kafka_loads, make_consumer,
)

OPERARIO_LOCAL = "LOCAL"          # operario que representa al menu de la propia WS
LIMITE_RIEGO_POR_DEFECTO_S = 20   # si la orden START no trae limite
LIMITE_RIEGO_MAXIMO_S = 3600      # maximo aceptado en el riego manual

# ------------------------- Tiempos (segundos) -------------------------
TIMEOUT_CONEXION_MONITOR_S = 5
TIMEOUT_SIN_MENSAJES_DEL_MONITOR_S = 10
ESPERA_REINTENTO_MONITOR_S = 3
ESPERA_REINTENTO_KAFKA_S = 3
ESPERA_ASIGNACION_PARTICIONES_S = 30
PERIODO_ESTADO_S = 5              # keep-alive del estado hacia Central
PERIODO_CONSUMO_S = 1             # caudal/volumen hacia Central y FO
ESPERA_CONSULTA_KAFKA_S = 1.0
ESPERA_TRAS_ERROR_KAFKA_S = 2

# ------------------------- Caudal simulado (litros/minuto) -------------------------
CAUDAL_MINIMO_LPM = 8.0
CAUDAL_MAXIMO_LPM = 12.0

# ------------------------- Motivos de fin de riego (van en el ticket) -------------------------
MOTIVO_LIMITE_TIEMPO = "LIMITE_TIEMPO"
MOTIVO_FIN_MANUAL = "FIN_MANUAL"
MOTIVO_FUGA = "FUGA"
MOTIVO_BLOQUEO_CENTRAL = "BLOQUEO_CENTRAL"
MOTIVO_MONITOR_CAIDO = "MONITOR_CAIDO"
MOTIVO_ENGINE_CERRADO = "ENGINE_CERRADO"
MOTIVO_ENGINE_CAIDO = "ENGINE_CAIDO"


class SesionRiego:
    """Un riego en curso: quien lo pidio, hasta cuando, cuanto lleva y como pararlo."""

    def __init__(self, operario, limite_s):
        self.operario = operario
        self.limite_s = limite_s
        self.inicio = time.time()
        self.volumen_litros = 0.0
        self.detener = threading.Event()     # se activa para parar el hilo que riega

    @property
    def id(self):
        """Identificador de la sesion (instante de inicio). Viaja en consumos y ticket para que
        CENTRAL y los FO distingan un consumo rezagado de un riego ya cerrado."""
        return f"{self.inicio:.3f}"

    def segundos_transcurridos(self):
        return time.time() - self.inicio


class Motor:
    def __init__(self, broker, monitor_ip, monitor_puerto):
        self.broker = broker
        self.monitor_ip = monitor_ip
        self.monitor_puerto = monitor_puerto

        self.ws_id = None
        self.pais = None                          # lo comunica el Monitor junto al ID
        self.id_asignado = threading.Event()      # se activa cuando el Monitor nos da el ID
        self.lock = threading.RLock()

        self.estado = DISPONIBLE                  # DISPONIBLE | REGANDO | FUERA_DE_SERVICIO
        self.fuga_simulada = False
        self.conectado_al_monitor = False
        self.central_alcanzable = True            # segun el Monitor
        self.sesion = None                        # SesionRiego en curso (o None)

        self.productor = None
        self.consumidor = None
        self.particiones_asignadas = None
        self.directorio_estado = os.environ.get("WS_STATE_DIR", ".")

    def escribir_log(self, texto):
        print(f"[WS_E {self.ws_id or '?'}] {texto}", flush=True)

    # ------------------ Persistencia del riego en curso ------------------
    def ruta_fichero_sesion(self):
        return os.path.join(self.directorio_estado, f"ws_{self.ws_id}_riego.json")

    def guardar_sesion_en_disco(self, sesion):
        datos = {"idWS": self.ws_id,
                 "operario": sesion.operario,
                 "limite": sesion.limite_s,
                 "inicio": sesion.inicio,
                 "volumen": round(sesion.volumen_litros, 3),
                 "ultimo": time.time()}
        ruta = self.ruta_fichero_sesion()
        ruta_temporal = ruta + ".tmp"
        try:
            os.makedirs(self.directorio_estado, exist_ok=True)
            with open(ruta_temporal, "w", encoding="utf-8") as fichero:
                json.dump(datos, fichero)
            os.replace(ruta_temporal, ruta)          # sustitucion atomica: nunca queda a medias
        except OSError as error:
            self.escribir_log(f"No se pudo guardar el riego en disco: {error}")

    def borrar_sesion_de_disco(self):
        try:
            os.remove(self.ruta_fichero_sesion())
        except OSError:
            pass

    def recuperar_sesion_interrumpida(self):
        """Si el Engine cayo en pleno riego, envia ahora el ticket final con lo ya regado."""
        try:
            with open(self.ruta_fichero_sesion(), "r", encoding="utf-8") as fichero:
                datos = json.load(fichero)
        except (OSError, ValueError):
            return          # no hay nada que recuperar

        inicio = float(datos.get("inicio", 0))
        ultimo_guardado = float(datos.get("ultimo", 0))
        ticket = {"idWS": self.ws_id,
                  "idOperator": str(datos.get("operario", "?")),
                  "estado": RIEGO_INTERRUMPIDO,
                  "motivo": MOTIVO_ENGINE_CAIDO,
                  "sesion": f"{inicio:.3f}",
                  "volumen": round(float(datos.get("volumen", 0)), 2),
                  "duracion": round(max(0.0, ultimo_guardado - inicio), 1)}

        self.productor.send(TOPIC_IRRIGATION_FINISHED, {"ticket": ticket}, self.pais)
        self.productor.flush()
        self.borrar_sesion_de_disco()
        self.escribir_log(f"Recuperado riego interrumpido por la caida del Engine: "
                          f"{ticket['volumen']} L en {ticket['duracion']} s. Ticket enviado")

    # ------------------ Monitor (sockets) ------------------
    def respuesta_de_salud(self):
        if self.fuga_simulada:
            return KO
        return OK

    def actualizar_estado_de_central(self, central_alcanzable):
        """Registra (y avisa por pantalla) los cambios de alcanzabilidad de CENTRAL."""
        if central_alcanzable == self.central_alcanzable:
            return
        self.central_alcanzable = central_alcanzable

        if central_alcanzable:
            self.escribir_log("WM_Central de nuevo disponible")
        else:
            self.escribir_log("Imposible conectar con WM_Central: el riego en curso terminara "
                              "y no se admitiran nuevos hasta que vuelva")

    def recibir_id_del_monitor(self, sock):
        mensaje_id = receive_message(sock)
        campos = mensaje_id.split(SEP)
        if len(campos) < 3 or campos[0] != ID or not es_pais_valido(campos[2]):
            raise ConnectionError(f"el Monitor no envio un ID y pais validos (paises: {', '.join(PAISES)})")
        self.ws_id = campos[1]
        self.pais = campos[2]
        self.id_asignado.set()
        self.conectado_al_monitor = True
        self.escribir_log(f"Conectado al Monitor, ID asignado (pais {self.pais})")

    def responder_a_consultas_de_salud(self, sock):
        """Contesta a cada HEALTH del Monitor hasta que se pierda la conexion."""
        while True:
            consulta = receive_message(sock)
            if consulta.startswith(HEALTH):
                self.actualizar_estado_de_central(consulta.endswith(CENTRAL_OK))
                send_message(sock, self.respuesta_de_salud())

    def tratar_perdida_del_monitor(self, error):
        if not self.conectado_al_monitor:
            self.escribir_log(f"No se puede conectar con el Monitor ({error}). Reintentando...")
            return

        self.conectado_al_monitor = False
        self.escribir_log(f"Conexion con el Monitor perdida ({error}). WS AVERIADA: no admite riegos")
        self.terminar_riego(RIEGO_INTERRUMPIDO, MOTIVO_MONITOR_CAIDO)

    def mantener_conexion_con_monitor(self):
        """Hilo: conecta con el Monitor y le contesta; si se pierde, reintenta indefinidamente."""
        while True:
            sock = None
            try:
                sock = socket.create_connection((self.monitor_ip, self.monitor_puerto),
                                                timeout=TIMEOUT_CONEXION_MONITOR_S)
                sock.settimeout(None)               # el Monitor puede tardar en autenticarse
                handshake(sock)
                self.recibir_id_del_monitor(sock)
                sock.settimeout(TIMEOUT_SIN_MENSAJES_DEL_MONITOR_S)
                self.responder_a_consultas_de_salud(sock)
            except OSError as error:
                self.tratar_perdida_del_monitor(error)
            except Exception as error:
                # Fallo inesperado (mensaje mal formado...): se trata como una perdida del Monitor
                self.tratar_perdida_del_monitor(repr(error))
            finally:
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
            time.sleep(ESPERA_REINTENTO_MONITOR_S)

    # ------------------ Kafka ------------------
    def iniciar_kafka(self):
        """Crea productor y consumidor; reintenta hasta que el broker este disponible."""
        self.particiones_asignadas = threading.Event()
        while True:
            try:
                ensure_topics(self.broker)
                self.productor = JsonProducer(self.broker)
                # station-command: ordenes de CENTRAL. progress-of-irrigation-request y
                # reply-to-irrigation-request: solo para mostrar la respuesta a los riegos
                # manuales de esta WS (operario LOCAL). Solo la particion de nuestro pais.
                self.consumidor = make_consumer(
                    self.broker,
                    [TOPIC_STATION_COMMAND, TOPIC_PROGRESS_OF_IRRIGATION_REQUEST,
                     TOPIC_REPLY_TO_IRRIGATION_REQUEST],
                    f"{GROUP_STATION_ENGINE}-{self.ws_id}", self.particiones_asignadas,
                    pais=self.pais)
                return
            except Exception as error:
                self.escribir_log(f"Broker Kafka no disponible ({error}), reintentando en "
                                  f"{ESPERA_REINTENTO_KAFKA_S} s...")
                time.sleep(ESPERA_REINTENTO_KAFKA_S)

    def enviar_estado(self):
        with self.lock:
            if self.sesion:
                operario = self.sesion.operario
            else:
                operario = ""
            mensaje_estado = {"idWS": self.ws_id, "estado": self.estado,
                              "averia": self.fuga_simulada, "operario": operario}
        self.productor.send(TOPIC_STATION_STATUS, mensaje_estado, self.pais)

    def mantener_estado(self):
        """Hilo: publica el estado cada pocos segundos (keep-alive hacia CENTRAL)."""
        while True:
            try:
                self.enviar_estado()
            except Exception as error:
                self.escribir_log(f"No se pudo publicar el estado ({error!r})")
            time.sleep(PERIODO_ESTADO_S)

    def escuchar_kafka(self):
        """Hilo: procesa los mensajes de Kafka dirigidos a esta WS."""
        while True:
            try:
                mensaje_kafka = self.consumidor.poll(ESPERA_CONSULTA_KAFKA_S)
            except (KafkaException, RuntimeError) as error:
                self.escribir_log(f"Error leyendo de Kafka ({error}). Reintentando...")
                time.sleep(ESPERA_TRAS_ERROR_KAFKA_S)
                continue

            if mensaje_kafka is None:
                continue
            if mensaje_kafka.error():
                if mensaje_kafka.error().code() != KafkaError._PARTITION_EOF:
                    self.escribir_log(f"Kafka: {mensaje_kafka.error()}")
                continue

            try:
                self.procesar_mensaje_kafka(mensaje_kafka)
            except Exception as error:
                self.escribir_log(f"Error procesando mensaje de {mensaje_kafka.topic()}: {error!r}")

    def procesar_mensaje_kafka(self, mensaje_kafka):
        datos = kafka_loads(mensaje_kafka)
        if not isinstance(datos, dict) or str(datos.get("idWS")) != str(self.ws_id):
            return      # mensaje de otra WS o con formato invalido

        if mensaje_kafka.topic() == TOPIC_STATION_COMMAND:
            self.ejecutar_orden_de_central(datos)
        else:
            self.mostrar_respuesta_al_riego_manual(mensaje_kafka.topic(), datos)

    def ejecutar_orden_de_central(self, orden):
        comando = orden.get("cmd")

        if comando == CMD_START:
            operario = str(orden.get("idOperator", "?"))
            limite_s = float(orden.get("limite", LIMITE_RIEGO_POR_DEFECTO_S))
            self.iniciar_riego(operario, limite_s)
        elif comando == CMD_BLOCK:
            self.bloquear()
        elif comando == CMD_ACTIVATE:
            self.activar()

    def mostrar_respuesta_al_riego_manual(self, topic, datos):
        """Muestra lo que CENTRAL contesta a los riegos pedidos desde el menu de esta WS."""
        if str(datos.get("idOperator")) != OPERARIO_LOCAL:
            return

        if topic == TOPIC_PROGRESS_OF_IRRIGATION_REQUEST:
            self.escribir_log(f"CENTRAL: {datos.get('msg')}")
        elif topic == TOPIC_REPLY_TO_IRRIGATION_REQUEST and datos.get("authorize") != AUTORIZADO:
            self.escribir_log(f"CENTRAL DENIEGA el riego manual: {datos.get('motivo')}")

    # ------------------ Logica de riego ------------------
    def confirmar_orden(self, operario, aceptada, motivo=""):
        self.productor.send(TOPIC_REPLY_TO_STATION_COMMAND,
                            {"idWS": self.ws_id, "idOperator": operario, "ok": aceptada, "motivo": motivo},
                            self.pais)
        self.productor.flush()

    def motivo_no_disponible(self):
        """Motivo por el que la WS no puede regar ahora ('' si puede). Llamar con el lock."""
        if not self.conectado_al_monitor:
            return "Estacion AVERIADA (sin Monitor)"
        if self.fuga_simulada:
            return "Fuga detectada en la estacion"
        if not self.central_alcanzable:
            return "Imposible conectar con WM_Central"
        if self.estado == FUERA_DE_SERVICIO:
            return "Estacion fuera de servicio"
        if self.estado == REGANDO:
            return "Estacion ya esta regando"
        return ""

    def iniciar_riego(self, operario, limite_s):
        with self.lock:
            motivo_denegacion = self.motivo_no_disponible()
            if not motivo_denegacion:
                sesion = SesionRiego(operario, limite_s)
                self.sesion = sesion
                self.estado = REGANDO

        if motivo_denegacion:
            self.escribir_log(f"Riego DENEGADO para {operario}: {motivo_denegacion}")
            self.confirmar_orden(operario, False, motivo_denegacion)
            return

        self.guardar_sesion_en_disco(sesion)
        self.escribir_log(f"Autorizado por CENTRAL. Abriendo electrovalvula para {operario} "
                          f"(limite {limite_s:.0f} s)")
        self.confirmar_orden(operario, True)
        self.enviar_estado()
        threading.Thread(target=self.regar, args=(sesion,), daemon=True).start()

    def regar(self, sesion):
        """Hilo del riego: cada segundo mide el caudal y lo publica, hasta parar o llegar al limite."""
        while not sesion.detener.wait(PERIODO_CONSUMO_S):
            with self.lock:
                if self.sesion is not sesion:
                    return
                caudal_lpm = round(random.uniform(CAUDAL_MINIMO_LPM, CAUDAL_MAXIMO_LPM), 1)
                sesion.volumen_litros += caudal_lpm / 60.0
                duracion_s = sesion.segundos_transcurridos()
                consumo = {"idWS": self.ws_id,
                           "idOperator": sesion.operario,
                           "caudal": caudal_lpm,
                           "sesion": sesion.id,
                           "volumen": round(sesion.volumen_litros, 2),
                           "duracion": round(duracion_s, 1)}

            self.guardar_sesion_en_disco(sesion)
            self.productor.send(TOPIC_IRRIGATION_FLOW_READING, consumo, self.pais)
            self.escribir_log(f"Regando: {caudal_lpm} L/min | {consumo['volumen']} L | "
                              f"{consumo['duracion']} s")

            if duracion_s >= sesion.limite_s:
                self.terminar_riego(RIEGO_COMPLETADO, MOTIVO_LIMITE_TIEMPO, sesion=sesion)
                return

    def terminar_riego(self, estado_ticket, motivo, estado_siguiente=None, sesion=None):
        """Cierra el riego en curso y publica su ticket. Devuelve False si no habia riego
        (o si 'sesion' indica otro distinto al que esta en curso)."""
        with self.lock:
            riego = self.sesion
            if riego is None:
                return False
            if sesion is not None and riego is not sesion:
                return False

            self.sesion = None
            riego.detener.set()
            self.estado = estado_siguiente or DISPONIBLE
            ticket = {"idWS": self.ws_id,
                      "idOperator": riego.operario,
                      "estado": estado_ticket,
                      "sesion": riego.id,
                      "motivo": motivo,
                      "volumen": round(riego.volumen_litros, 2),
                      "duracion": round(riego.segundos_transcurridos(), 1)}

        self.productor.send(TOPIC_IRRIGATION_FINISHED, {"ticket": ticket}, self.pais)
        self.productor.flush()
        # Solo tras publicar el ticket: si el Engine cae antes, se reenvia al volver
        self.borrar_sesion_de_disco()
        self.escribir_log(f"Riego finalizado ({estado_ticket}, {motivo}): "
                          f"{ticket['volumen']} L en {ticket['duracion']} s")
        self.enviar_estado()
        return True

    def bloquear(self):
        """Orden BLOCK de CENTRAL: corta el riego en curso y deja la WS fuera de servicio."""
        self.terminar_riego(RIEGO_INTERRUMPIDO, MOTIVO_BLOQUEO_CENTRAL,
                            estado_siguiente=FUERA_DE_SERVICIO)
        with self.lock:
            self.estado = FUERA_DE_SERVICIO
        self.escribir_log("FUERA DE SERVICIO por orden de CENTRAL")
        self.enviar_estado()

    def activar(self):
        """Orden ACTIVATE de CENTRAL: si estaba fuera de servicio, vuelve a estar disponible."""
        with self.lock:
            if self.estado == FUERA_DE_SERVICIO:
                self.estado = DISPONIBLE
        self.escribir_log("Estacion ACTIVADA por CENTRAL")
        self.enviar_estado()

    # ------------------ Acciones del menu ------------------
    def alternar_fuga_simulada(self):
        """Opcion 1: simula una fuga (KO al Monitor) o la repara."""
        with self.lock:
            self.fuga_simulada = not self.fuga_simulada
            hay_fuga = self.fuga_simulada

        if hay_fuga:
            self.escribir_log("*** FUGA simulada: reportando KO al Monitor ***")
            self.terminar_riego(RIEGO_INTERRUMPIDO, MOTIVO_FUGA)
        else:
            self.escribir_log("Fuga reparada: reportando OK al Monitor")
        self.enviar_estado()

    def pedir_duracion_por_consola(self):
        """Pregunta la duracion del riego manual. Devuelve None si se deja vacia (limite por
        defecto de CENTRAL). Lanza ValueError si no es un numero entre 1 y 3600."""
        texto = input("Duracion en segundos (Enter = la de por defecto de CENTRAL): ").strip()
        if not texto:
            return None

        duracion_s = float(texto)
        if not 0 < duracion_s <= LIMITE_RIEGO_MAXIMO_S:
            raise ValueError("duracion fuera de rango")
        return duracion_s

    def pedir_riego_manual(self):
        """Opcion 2: riego pedido desde el menu de la propia WS; pasa igualmente por CENTRAL."""
        with self.lock:
            motivo = self.motivo_no_disponible()
        if motivo:
            self.escribir_log(f"Riego manual no disponible: {motivo}")
            return

        peticion = {"idOperator": OPERARIO_LOCAL, "idWS": self.ws_id}
        try:
            duracion_s = self.pedir_duracion_por_consola()
        except ValueError:
            self.escribir_log("Duracion no valida (1-3600 s). Riego manual cancelado")
            return
        if duracion_s is not None:
            peticion["limite"] = duracion_s

        self.productor.send(TOPIC_IRRIGATION_REQUEST, peticion, self.pais)
        self.productor.flush()
        self.escribir_log("Riego manual solicitado a CENTRAL")

    def finalizar_riego_manual(self):
        """Opcion 3: termina el riego en curso."""
        if not self.terminar_riego(RIEGO_COMPLETADO, MOTIVO_FIN_MANUAL):
            self.escribir_log("No hay riego en curso")

    def estado_para_mostrar(self):
        if not self.conectado_al_monitor:
            return "AVERIADA (sin Monitor)"
        if self.fuga_simulada:
            return "FUGA"
        return self.estado

    def mostrar_estado(self):
        """Opcion 4: escribe el estado actual de la WS."""
        with self.lock:
            texto = self.estado_para_mostrar()
            if not self.central_alcanzable:
                texto += " | SIN CONEXION con WM_Central"
            if self.sesion:
                texto += (f" | operario {self.sesion.operario} | "
                          f"{self.sesion.volumen_litros:.2f} L")
        self.escribir_log(f"Estado: {texto}")

    def esperar_sin_consola(self):
        """Sin consola interactiva (docker sin tty): la WS sigue funcionando como servicio."""
        self.escribir_log("Sin consola interactiva: la WS sigue en marcha como servicio")
        while True:
            time.sleep(3600)

    def menu(self):
        acciones = {"1": self.alternar_fuga_simulada,
                    "2": self.pedir_riego_manual,
                    "3": self.finalizar_riego_manual,
                    "4": self.mostrar_estado}

        print("\n1) Simular fuga / reparar   2) Riego manual   3) Finalizar riego   4) Estado   q) Salir")
        while True:
            try:
                opcion = input().strip().lower()
            except EOFError:
                self.esperar_sin_consola()
            except KeyboardInterrupt:
                break

            if opcion == "q":
                break
            accion = acciones.get(opcion)
            if accion:
                accion()

    # ------------------ Arranque ------------------
    def run(self):
        threading.Thread(target=self.mantener_conexion_con_monitor, daemon=True).start()
        self.escribir_log("Esperando asignacion de ID por el Monitor...")
        self.id_asignado.wait()

        self.iniciar_kafka()
        self.recuperar_sesion_interrumpida()      # ticket de un riego que quedo a medias por una caida
        threading.Thread(target=self.escuchar_kafka, daemon=True).start()
        self.particiones_asignadas.wait(ESPERA_ASIGNACION_PARTICIONES_S)
        threading.Thread(target=self.mantener_estado, daemon=True).start()

        self.menu()

        try:
            self.terminar_riego(RIEGO_INTERRUMPIDO, MOTIVO_ENGINE_CERRADO)
        except Exception:
            pass


def main():
    if len(sys.argv) < 3:
        print("Uso: python WM_WS_E.py <broker_ip:puerto> <monitor_ip:puerto>")
        sys.exit(1)

    broker = sys.argv[1]
    try:
        monitor_ip, monitor_puerto = sys.argv[2].rsplit(":", 1)
        monitor_puerto = int(monitor_puerto)
    except ValueError:
        print(f"Direccion del Monitor no valida: {sys.argv[2]!r} (formato ip:puerto)")
        sys.exit(1)

    Motor(broker, monitor_ip, monitor_puerto).run()


if __name__ == "__main__":
    main()
