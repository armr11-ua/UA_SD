"""WM_Central - Sistema central de WaterManagement.

Uso: python WM_Central.py <puerto_sockets> <broker_ip:puerto> [fichero_bd]

- Servidor de sockets para los Monitores (registro/autenticacion, heartbeat, fugas).
- Kafka (confluent_kafka): peticiones de operarios, estado/consumo/tickets de las WS, ordenes a las WS.
  Cada topic tiene una particion por pais: CENTRAL las lee todas y contesta siempre en la
  particion del pais de la WS / del operario (ver topics.py).
- BD SQLite (BD/WM_BD.py): WS, operarios e historico de riegos.
- Panel de monitorizacion web en el puerto de la variable de entorno PORT (por defecto 8080).
"""
import json
import os
import re
import socket
import sqlite3
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

from confluent_kafka import KafkaError, KafkaException

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from BD.WM_BD import BaseDatos
from protocol import (
    HB, KO, LEAK, OK, RECOVERED, REGISTER, REGISTER_KO, REGISTER_OK, SEP,
    mensaje, receive_message, send_message,
)
from topics import (
    AUTORIZADO, CMD_ACTIVATE, CMD_BLOCK, CMD_START, DENEGADO,
    DESCONECTADA, DISPONIBLE, FUERA_DE_SERVICIO, FUGA, REGANDO,
    GROUP_CENTRAL_EVENTS, GROUP_CENTRAL_FINISHED_IRRIGATIONS,
    TOPIC_AVAILABLE_STATIONS, TOPIC_IRRIGATION_FINISHED, TOPIC_IRRIGATION_FLOW_READING,
    TOPIC_IRRIGATION_REQUEST, TOPIC_IRRIGATION_SUMMARY, TOPIC_PROGRESS_OF_IRRIGATION_REQUEST,
    TOPIC_REPLY_TO_IRRIGATION_REQUEST, TOPIC_REPLY_TO_STATION_COMMAND, TOPIC_STATION_COMMAND,
    TOPIC_STATION_STATUS,
    PAISES, es_pais_valido, pais_de_la_particion,
    JsonProducer, SinKafka, confirmar, ensure_topics, kafka_loads, make_consumer,
)

# ------------------------- Parametros -------------------------
LIMITE_RIEGO_POR_DEFECTO_S = 20     # limite de tiempo de un riego si la peticion no trae uno
LIMITE_RIEGO_MAXIMO_S = 3600
TIMEOUT_LATIDO_S = 10               # sin mensajes del Monitor durante este tiempo => WS desconectada
TIMEOUT_AUTORIZACION_S = 5          # espera maxima de la respuesta de la WS a un START
PERIODO_LISTA_WS_S = 2              # cada cuanto se publican las WS disponibles
ESPERA_REINTENTO_KAFKA_S = 3
ESPERA_CONSULTA_KAFKA_S = 1.0
ESPERA_TRAS_ERROR_KAFKA_S = 2
ESPERA_TRAS_ERROR_ACCEPT_S = 1
MAX_LINEAS_LOG = 300
LINEAS_LOG_EN_PANEL = 120
PENDIENTES_EN_SOCKET_SERVIDOR = 20

PATRON_ID_VALIDO = re.compile(r"^[A-Za-z0-9_-]{1,10}$")

# ALTA_AUTOMATICA=False (variable AUTO_ALTA=0): solo se aceptan WS ya dadas de alta en la BD;
# el resto se rechaza con NO_REGISTRADA.
ALTA_AUTOMATICA = os.environ.get("AUTO_ALTA", "1") != "0"

OPERARIO_CENTRAL = "CENTRAL"        # operario que representa a las ordenes manuales del panel

# Ordenes del panel que se envian tal cual a la WS: orden -> (comando Kafka, texto del log)
ORDENES_A_LA_WS = {
    "block": (CMD_BLOCK, "BLOQUEAR"),
    "activate": (CMD_ACTIVATE, "ACTIVAR"),
}


# =====================================================================
# Estacion de riego (lo que CENTRAL sabe de cada WS)
# =====================================================================
@dataclass
class EstacionRiego:
    id: str
    ubicacion: str
    # Pais (decide la particion de Kafka). None = WS de la BD que aun no ha conectado: el pais se
    # fija cuando su Monitor se registra y se mantiene mientras CENTRAL siga en marcha
    pais: Optional[str] = None
    conexion: Optional[socket.socket] = None          # socket del Monitor (None si no esta conectado)
    conectada: bool = False
    monitor_reporta_averia: bool = False              # el Monitor ha enviado LEAK
    engine_reporta_averia: bool = False               # el Engine dice (por Kafka) que tiene una averia
    estado_engine: str = DISPONIBLE                   # DISPONIBLE | REGANDO | FUERA_DE_SERVICIO
    caudal: float = 0.0
    volumen: float = 0.0
    operario: str = ""
    autorizacion_pendiente: bool = False              # START enviado, esperando la respuesta de la WS
    temporizador_autorizacion: Optional[threading.Timer] = None
    ultimo_estado_guardado_en_bd: str = DESCONECTADA
    ultima_sesion_cerrada: Optional[str] = None       # id de la ultima sesion de riego con ticket recibido

    def estado_real(self):
        """Estado que se muestra, combinando lo que dicen el Monitor y el Engine."""
        if not self.conectada:
            return DESCONECTADA
        if self.monitor_reporta_averia or self.engine_reporta_averia:
            return FUGA
        return self.estado_engine

    def marcar_conectada(self, conexion):
        """El Monitor acaba de autenticarse: se parte de una WS limpia y disponible."""
        self.conexion = conexion
        self.conectada = True
        self.monitor_reporta_averia = False
        self.engine_reporta_averia = False
        self.estado_engine = DISPONIBLE
        self.caudal = 0.0
        self.volumen = 0.0
        self.operario = ""
        self.autorizacion_pendiente = False

    def marcar_desconectada(self):
        """El Monitor se ha ido: se conservan el estado del Engine y el operario."""
        self.conexion = None
        self.conectada = False
        self.monitor_reporta_averia = False
        self.autorizacion_pendiente = False
        self.caudal = 0.0

    def operario_si_esta_regando(self):
        """Operario al que se esta regando ahora mismo ('' si no se riega)."""
        if self.estado_engine == REGANDO:
            return self.operario
        return ""


# =====================================================================
# Central
# =====================================================================
class Central:
    def __init__(self, puerto_sockets, broker, ruta_bd, puerto_web):
        self.puerto_sockets = puerto_sockets
        self.broker = broker
        self.puerto_web = puerto_web

        self.lock = threading.RLock()                        # protege estaciones y logs
        self.logs = deque(maxlen=MAX_LINEAS_LOG)
        self.estaciones = {}                                 # idWS -> EstacionRiego

        self.kafka_conectado = False
        self.productor = SinKafka(self.escribir_log)         # se sustituye al conectar con Kafka

        # Que hacer con cada mensaje de Kafka segun su topic: manejador(datos, pais)
        self.manejadores_kafka = {
            TOPIC_IRRIGATION_REQUEST: self.recibir_peticion_de_riego,
            TOPIC_STATION_STATUS: self.recibir_estado,
            TOPIC_IRRIGATION_FLOW_READING: self.recibir_consumo,
            TOPIC_REPLY_TO_STATION_COMMAND: self.recibir_confirmacion,
            TOPIC_IRRIGATION_FINISHED: self.recibir_ticket,
        }

        self.base_datos = BaseDatos(ruta_bd, self.escribir_log)    # lanza sqlite3.Error si falla
        self.cargar_estaciones_de_la_bd()

    def escribir_log(self, texto):
        linea = f"[{time.strftime('%H:%M:%S')}] {texto}"
        with self.lock:
            self.logs.append(linea)
        print(linea, flush=True)

    # ------------------ Estaciones ------------------
    def cargar_estaciones_de_la_bd(self):
        """Al (re)iniciar, todas las WS conocidas aparecen como DESCONECTADA."""
        filas = self.base_datos.preparar_y_listar_estaciones()
        for ws_id, ubicacion in filas:
            self.estaciones[ws_id] = EstacionRiego(ws_id, ubicacion)
        self.escribir_log(f"BD cargada: {len(filas)} WS registradas "
                          f"(todas DESCONECTADA hasta que conecten)")

    def estacion_del_pais(self, ws_id, pais):
        """La estacion si existe y es de ese pais; si no, None. Llamar con el lock."""
        estacion = self.estaciones.get(ws_id)
        if estacion is None or estacion.pais != pais:
            return None
        return estacion

    def guardar_estado_en_bd_si_cambio(self, estacion):
        estado = estacion.estado_real()
        if estado == estacion.ultimo_estado_guardado_en_bd:
            return
        if self.base_datos.guardar_estado_estacion(estacion.id, estado):
            estacion.ultimo_estado_guardado_en_bd = estado

    # ------------------ Servidor de sockets (Monitores) ------------------
    def abrir_servidor_de_sockets(self):
        """Abre el puerto de los Monitores. Lanza OSError si no se puede (puerto ocupado...)."""
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor.bind(("0.0.0.0", self.puerto_sockets))
            servidor.listen(PENDIENTES_EN_SOCKET_SERVIDOR)
        except OSError:
            servidor.close()
            raise
        return servidor

    def aceptar_monitores(self, servidor):
        """Hilo: acepta conexiones de Monitores y atiende a cada uno en su propio hilo."""
        self.escribir_log(f"Servidor de sockets escuchando en el puerto {self.puerto_sockets}")
        while True:
            try:
                conexion, direccion = servidor.accept()
            except OSError as error:
                self.escribir_log(f"Error al aceptar una conexion de Monitor ({error})")
                time.sleep(ESPERA_TRAS_ERROR_ACCEPT_S)
                continue

            try:
                threading.Thread(target=self.atender_monitor, args=(conexion, direccion),
                                 daemon=True).start()
            except RuntimeError as error:
                self.escribir_log(f"No se pudo atender al Monitor {direccion} ({error})")
                conexion.close()

    def atender_monitor(self, conexion, direccion):
        """Conversacion con un Monitor: cada peticion recibida se contesta con una respuesta."""
        ws_id = None        # solo se conoce cuando el Monitor se registra con exito
        try:
            conexion.settimeout(TIMEOUT_LATIDO_S)
            while True:
                peticion = receive_message(conexion).split(SEP)
                respuesta, ws_id = self.responder_a_monitor(peticion, conexion, ws_id)
                send_message(conexion, respuesta)
        except OSError:
            pass        # el Monitor se cerro o dejo de enviar latidos
        except Exception as error:
            self.escribir_log(f"Error inesperado con el Monitor {direccion}: {error!r}")
        finally:
            try:
                conexion.close()
            except OSError:
                pass
            if ws_id:
                self.marcar_desconectada(ws_id, conexion)

    def responder_a_monitor(self, peticion, conexion, ws_id):
        """Devuelve (respuesta, ws_id): ws_id es la WS autenticada en esta conexion."""
        comando = peticion[0]

        if comando == REGISTER and len(peticion) >= 4:
            ws_id_solicitado, ubicacion, pais = peticion[1], peticion[2], peticion[3]
            aceptada, motivo_rechazo = self.registrar_ws(ws_id_solicitado, ubicacion, pais, conexion)
            if aceptada:
                return REGISTER_OK, ws_id_solicitado
            return mensaje(REGISTER_KO, motivo_rechazo), ws_id

        if ws_id is None:
            return mensaje(KO, "NO_AUTENTICADO"), ws_id

        if comando == HB:
            return OK, ws_id
        if comando == LEAK:
            self.cambiar_averia_monitor(ws_id, True)
            return OK, ws_id
        if comando == RECOVERED:
            self.cambiar_averia_monitor(ws_id, False)
            return OK, ws_id

        return mensaje(KO, "DESCONOCIDO"), ws_id

    def registrar_ws(self, ws_id, ubicacion, pais, conexion):
        """Autentica a un Monitor. Devuelve (aceptada, motivo_del_rechazo)."""
        if not PATRON_ID_VALIDO.match(ws_id):
            self.escribir_log(f"Registro rechazado: ID invalido '{ws_id}'")
            return False, "ID_INVALIDO"
        if not es_pais_valido(pais):
            self.escribir_log(f"Registro rechazado: WS {ws_id} con pais invalido '{pais}'")
            return False, "PAIS_INVALIDO"

        with self.lock:
            estacion = self.estaciones.get(ws_id)

            if estacion and estacion.pais not in (None, pais):
                self.escribir_log(f"Registro rechazado: WS {ws_id} esta dada de alta en "
                                  f"{estacion.pais}, no en {pais}")
                return False, "PAIS_DISTINTO"

            if estacion and estacion.conectada and estacion.conexion is not conexion:
                self.escribir_log(f"Registro rechazado: WS {ws_id} ya conectada")
                return False, "YA_CONECTADA"

            if not estacion and not ALTA_AUTOMATICA:
                self.escribir_log(f"Registro rechazado: WS {ws_id} no esta dada de alta")
                return False, "NO_REGISTRADA"

            if not estacion:
                estacion = self.dar_de_alta_estacion(ws_id, ubicacion, pais)
            estacion.pais = pais

            estacion.marcar_conectada(conexion)
            self.guardar_estado_en_bd_si_cambio(estacion)

        self.escribir_log(f"WS {ws_id} autenticada y conectada ({estacion.ubicacion}, {estacion.pais})")
        return True, ""

    def dar_de_alta_estacion(self, ws_id, ubicacion, pais):
        if not ubicacion:
            ubicacion = f"Zona-{ws_id}"
        estacion = EstacionRiego(ws_id, ubicacion, pais)
        self.estaciones[ws_id] = estacion
        self.base_datos.dar_de_alta_estacion(ws_id, ubicacion)
        self.escribir_log(f"Nueva WS {ws_id} dada de alta en '{ubicacion}' ({pais})")
        return estacion

    def marcar_desconectada(self, ws_id, conexion):
        """El Monitor de esta WS se ha ido. Si se estaba regando, se avisa al operario."""
        with self.lock:
            estacion = self.estaciones.get(ws_id)
            if not estacion or estacion.conexion is not conexion:
                return      # ya se registro otra conexion para esta WS
            operario_afectado = estacion.operario_si_esta_regando()
            pais = estacion.pais
            estacion.marcar_desconectada()
            self.guardar_estado_en_bd_si_cambio(estacion)

        self.escribir_log(f"WS {ws_id} DESCONECTADA (monitor sin conexion)")
        if operario_afectado:
            self.avisar_operario(operario_afectado, ws_id, pais,
                                 "La WS ha quedado DESCONECTADA: riego interrumpido. "
                                 "Recibiras el resumen cuando la estacion se recupere")

    def cambiar_averia_monitor(self, ws_id, hay_averia):
        """El Monitor comunica una averia (LEAK) o que se ha resuelto (RECOVERED)."""
        with self.lock:
            estacion = self.estaciones.get(ws_id)
            if not estacion:
                return
            operario_afectado = ""
            if hay_averia:
                operario_afectado = estacion.operario_si_esta_regando()
            pais = estacion.pais
            estacion.monitor_reporta_averia = hay_averia
            self.guardar_estado_en_bd_si_cambio(estacion)

        if hay_averia:
            self.escribir_log(f"WS {ws_id}: FUGA/averia reportada por el Monitor")
        else:
            self.escribir_log(f"WS {ws_id}: averia resuelta, vuelve a estar disponible")

        if operario_afectado:
            self.avisar_operario(operario_afectado, ws_id, pais,
                                 "AVERIA en la WS: riego interrumpido. "
                                 "Recibiras el resumen cuando la estacion se recupere")

    # ------------------ Kafka ------------------
    def iniciar_kafka(self):
        """Conecta con Kafka (reintenta hasta lograrlo) y arranca los hilos que lo usan."""
        while True:
            try:
                ensure_topics(self.broker)
                productor = JsonProducer(self.broker)

                # Mensajes efimeros de TODOS los paises: solo interesan los nuevos
                consumidor_general = make_consumer(
                    self.broker,
                    [TOPIC_IRRIGATION_REQUEST, TOPIC_STATION_STATUS,
                     TOPIC_IRRIGATION_FLOW_READING, TOPIC_REPLY_TO_STATION_COMMAND],
                    GROUP_CENTRAL_EVENTS, threading.Event())

                # Tickets: grupo FIJO y offsets confirmados => si CENTRAL cae no se pierde
                # ningun ticket de riego publicado mientras estuvo caida
                consumidor_tickets = make_consumer(
                    self.broker, [TOPIC_IRRIGATION_FINISHED], GROUP_CENTRAL_FINISHED_IRRIGATIONS,
                    threading.Event(), durable=True)

                self.productor = productor
                self.kafka_conectado = True
                self.escribir_log(f"Conectado a Kafka ({self.broker})")

                threading.Thread(target=self.escuchar_kafka, args=(consumidor_general, False),
                                 daemon=True).start()
                threading.Thread(target=self.escuchar_kafka, args=(consumidor_tickets, True),
                                 daemon=True).start()
                threading.Thread(target=self.publicar_lista_de_ws, daemon=True).start()
                return
            except Exception as error:
                self.escribir_log(f"Broker Kafka no disponible ({error}), reintentando en "
                                  f"{ESPERA_REINTENTO_KAFKA_S} s...")
                time.sleep(ESPERA_REINTENTO_KAFKA_S)

    def escuchar_kafka(self, consumidor, es_durable):
        """Hilo: lee mensajes del consumidor y los reparte al manejador de su topic."""
        while True:
            try:
                mensaje_kafka = consumidor.poll(ESPERA_CONSULTA_KAFKA_S)
            except (KafkaException, RuntimeError) as error:
                self.kafka_conectado = False
                self.escribir_log(f"Error leyendo de Kafka ({error}). Reintentando...")
                time.sleep(ESPERA_TRAS_ERROR_KAFKA_S)
                continue

            if mensaje_kafka is None:
                continue

            if mensaje_kafka.error():
                if mensaje_kafka.error().code() == KafkaError._ALL_BROKERS_DOWN:
                    self.kafka_conectado = False
                elif mensaje_kafka.error().code() != KafkaError._PARTITION_EOF:
                    self.escribir_log(f"Kafka: {mensaje_kafka.error()}")
                continue

            self.kafka_conectado = True
            self.procesar_mensaje_kafka(mensaje_kafka)

            if es_durable:
                # Solo tras procesarlo (guardado en BD y reenviado al FO)
                confirmar(consumidor, mensaje_kafka)

    def procesar_mensaje_kafka(self, mensaje_kafka):
        topic = mensaje_kafka.topic()
        pais = pais_de_la_particion(mensaje_kafka.partition())    # la particion dice de que pais viene
        datos = kafka_loads(mensaje_kafka)
        if pais is None or not isinstance(datos, dict):
            return

        manejador = self.manejadores_kafka.get(topic)
        if manejador is None:
            return

        try:
            manejador(datos, pais)
        except Exception as error:
            self.escribir_log(f"Error procesando mensaje de {topic}: {error!r}")

    def publicar_lista_de_ws(self):
        """Hilo: publica periodicamente las WS disponibles de cada pais (los FO de ese pais las muestran)."""
        while True:
            try:
                with self.lock:
                    disponibles_por_pais = {pais: [] for pais in PAISES}
                    for estacion in self.estaciones.values():
                        if estacion.estado_real() == DISPONIBLE and estacion.pais:
                            disponibles_por_pais[estacion.pais].append(
                                {"idWS": estacion.id, "ubicacion": estacion.ubicacion})

                for pais, disponibles in disponibles_por_pais.items():
                    self.productor.send(TOPIC_AVAILABLE_STATIONS, disponibles, pais)
            except Exception as error:
                self.escribir_log(f"No se pudo publicar la lista de WS ({error!r})")
            time.sleep(PERIODO_LISTA_WS_S)

    # ------------------ Peticiones de riego ------------------
    @staticmethod
    def limite_valido(valor):
        """Limite de la peticion en segundos; si falta o no es valido, el de por defecto."""
        try:
            limite_s = float(valor)
        except (TypeError, ValueError):
            return LIMITE_RIEGO_POR_DEFECTO_S

        if 0 < limite_s <= LIMITE_RIEGO_MAXIMO_S:
            return limite_s
        return LIMITE_RIEGO_POR_DEFECTO_S

    def recibir_peticion_de_riego(self, datos, pais):
        operario = str(datos.get("idOperator", "?"))
        ws_id = str(datos.get("idWS", ""))
        limite_s = self.limite_valido(datos.get("limite"))
        self.gestionar_peticion_riego(operario, ws_id, pais, limite_s)

    def avisar_operario(self, operario, ws_id, pais, texto):
        self.productor.send(TOPIC_PROGRESS_OF_IRRIGATION_REQUEST,
                            {"idOperator": operario, "idWS": ws_id, "msg": texto}, pais)
        self.escribir_log(f"[-> operario {operario}] {texto}")

    def denegar_riego(self, operario, ws_id, pais, motivo):
        self.productor.send(TOPIC_REPLY_TO_IRRIGATION_REQUEST,
                            {"idOperator": operario, "idWS": ws_id,
                             "authorize": DENEGADO, "motivo": motivo}, pais)
        self.productor.flush()
        self.escribir_log(f"Riego DENEGADO a {operario} en WS {ws_id}: {motivo}")

    def motivo_para_denegar(self, estacion, pais):
        """Motivo por el que no se puede regar en esa estacion ('' si se puede)."""
        if estacion is None:
            return "La WS no existe"
        if estacion.pais not in (None, pais):
            return f"La WS pertenece a otro pais ({estacion.pais})"
        if estacion.autorizacion_pendiente:
            return "La WS tiene otra solicitud en curso"
        if estacion.estado_real() != DISPONIBLE:
            return f"WS no disponible (estado: {estacion.estado_real()})"
        return ""

    def reservar_estacion_para_riego(self, estacion, operario):
        """Marca la WS como 'esperando respuesta' y arma un temporizador por si no contesta."""
        estacion.autorizacion_pendiente = True
        estacion.operario = operario
        estacion.temporizador_autorizacion = threading.Timer(
            TIMEOUT_AUTORIZACION_S, self.terminar_solicitud,
            args=(estacion.id, operario, estacion.pais))
        estacion.temporizador_autorizacion.daemon = True
        estacion.temporizador_autorizacion.start()

    def gestionar_peticion_riego(self, operario, ws_id, pais, limite_s=LIMITE_RIEGO_POR_DEFECTO_S):
        """Pasos: registrar la peticion -> comprobar que la WS esta disponible -> pedirle
        autorizacion (START). Cada paso se notifica al operario, en la particion de su pais."""
        self.escribir_log(f"Peticion de riego de {operario} ({pais}) para WS {ws_id}")
        self.base_datos.registrar_operario(operario)
        self.avisar_operario(operario, ws_id, pais,
                             "Peticion recibida por CENTRAL. Comprobando disponibilidad...")

        with self.lock:
            estacion = self.estaciones.get(ws_id)
            motivo_denegacion = self.motivo_para_denegar(estacion, pais)
            if not motivo_denegacion:
                self.reservar_estacion_para_riego(estacion, operario)

        if motivo_denegacion:
            self.denegar_riego(operario, ws_id, pais, motivo_denegacion)
            return

        self.avisar_operario(operario, ws_id, pais,
                             "WS disponible. Solicitando autorizacion a la estacion...")
        self.productor.send(TOPIC_STATION_COMMAND,
                            {"cmd": CMD_START, "idWS": ws_id,
                             "idOperator": operario, "limite": limite_s}, pais)
        self.productor.flush()

    def terminar_solicitud(self, ws_id, operario, pais):
        """Temporizador: la WS no contesto al START a tiempo => se deniega el riego."""
        with self.lock:
            estacion = self.estaciones.get(ws_id)
            if not estacion or not estacion.autorizacion_pendiente:
                return      # la WS ya contesto
            estacion.autorizacion_pendiente = False
            estacion.operario = ""
        self.denegar_riego(operario, ws_id, pais, "La WS no responde")

    def recibir_confirmacion(self, datos, pais):
        """Respuesta de la WS a un START: autoriza o deniega el riego al operario."""
        ws_id = str(datos.get("idWS"))
        operario = str(datos.get("idOperator"))
        aceptada = datos.get("ok")

        with self.lock:
            estacion = self.estacion_del_pais(ws_id, pais)
            if not estacion or not estacion.autorizacion_pendiente:
                return      # respuesta tardia (ya se denego por timeout) o de otro pais
            estacion.autorizacion_pendiente = False
            if estacion.temporizador_autorizacion:
                estacion.temporizador_autorizacion.cancel()
            if not aceptada:
                estacion.operario = ""

        if aceptada:
            self.productor.send(TOPIC_REPLY_TO_IRRIGATION_REQUEST,
                                {"idOperator": operario, "idWS": ws_id,
                                 "authorize": AUTORIZADO, "motivo": ""}, pais)
            self.productor.flush()
            self.avisar_operario(operario, ws_id, pais, "Riego AUTORIZADO. Abriendo electrovalvula")
        else:
            motivo = datos.get("motivo", "Denegado por la estacion")
            self.denegar_riego(operario, ws_id, pais, motivo)

    # ------------------ Estado y consumo que publica la WS ------------------
    def recibir_estado(self, datos, pais):
        """station-status: estado del Engine (cambios + keep-alive cada pocos segundos)."""
        ws_id = str(datos.get("idWS"))
        with self.lock:
            estacion = self.estacion_del_pais(ws_id, pais)
            if not estacion or not estacion.conectada:
                return

            estado_anterior = estacion.estado_real()

            if datos.get("estado") in (DISPONIBLE, REGANDO, FUERA_DE_SERVICIO):
                estacion.estado_engine = datos["estado"]
            estacion.engine_reporta_averia = bool(datos.get("averia"))

            if estacion.estado_engine == REGANDO:
                estacion.operario = str(datos.get("operario", estacion.operario))
            else:
                estacion.caudal = 0.0
                if not estacion.autorizacion_pendiente:
                    estacion.operario = ""

            self.guardar_estado_en_bd_si_cambio(estacion)
            estado_nuevo = estacion.estado_real()

        if estado_nuevo != estado_anterior:
            self.escribir_log(f"WS {ws_id}: {estado_anterior} -> {estado_nuevo}")

    def recibir_consumo(self, datos, pais):
        """irrigation-flow-reading: caudal y volumen acumulado (cada segundo mientras riega)."""
        ws_id = str(datos.get("idWS"))
        with self.lock:
            estacion = self.estacion_del_pais(ws_id, pais)
            if not estacion:
                return

            sesion = datos.get("sesion")
            if sesion is not None and sesion == estacion.ultima_sesion_cerrada:
                return      # consumo rezagado de un riego que ya tiene ticket

            estacion.estado_engine = REGANDO
            estacion.caudal = float(datos.get("caudal", 0))
            estacion.volumen = float(datos.get("volumen", 0))
            estacion.operario = str(datos.get("idOperator", ""))
            self.guardar_estado_en_bd_si_cambio(estacion)

    def recibir_ticket(self, datos, pais):
        self.guardar_ticket(datos.get("ticket", {}), pais)

    def guardar_ticket(self, ticket, pais):
        """irrigation-finished: el riego ha terminado. Se guarda en la BD y se reenvia al FO."""
        ws_id = str(ticket.get("idWS"))
        operario = str(ticket.get("idOperator"))

        with self.lock:
            estacion = self.estacion_del_pais(ws_id, pais)
            if estacion:
                estacion.caudal = 0.0
                estacion.volumen = 0.0
                estacion.operario = ""
                estacion.ultima_sesion_cerrada = ticket.get("sesion")

        self.base_datos.registrar_operario(operario)
        self.base_datos.guardar_riego(operario, ws_id,
                                      ticket.get("estado", "COMPLETADO"),
                                      ticket.get("motivo", ""),
                                      ticket.get("volumen", 0),
                                      ticket.get("duracion", 0))

        self.productor.send(TOPIC_IRRIGATION_SUMMARY, {"ticket": ticket}, pais)
        self.productor.flush()
        self.escribir_log(f"Riego {ticket.get('estado')} en WS {ws_id} (operario {operario}): "
                          f"{ticket.get('volumen')} L, {ticket.get('duracion')} s, "
                          f"motivo {ticket.get('motivo')}")

    # ------------------ Ordenes manuales del panel ------------------
    def ejecutar_orden(self, comando, objetivo):
        """Orden del panel ('start' | 'block' | 'activate') para una WS o para todas ('ALL')."""
        with self.lock:
            if objetivo == "ALL":
                candidatas = list(self.estaciones)
            else:
                candidatas = [objetivo]
            destinos = [(ws_id, self.estaciones[ws_id].pais)
                        for ws_id in candidatas
                        if ws_id in self.estaciones and self.estaciones[ws_id].pais]

        for ws_id, pais in destinos:
            if comando == "start":
                self.gestionar_peticion_riego(OPERARIO_CENTRAL, ws_id, pais)
            elif comando in ORDENES_A_LA_WS:
                comando_kafka, texto_log = ORDENES_A_LA_WS[comando]
                self.productor.send(TOPIC_STATION_COMMAND,
                                    {"cmd": comando_kafka, "idWS": ws_id}, pais)
                self.escribir_log(f"Orden {texto_log} enviada a WS {ws_id} ({pais})")
        self.productor.flush()

    # ------------------ Panel web ------------------
    def obtener_estado(self):
        """Lo que muestra el panel: estaciones ordenadas por ID, ultimos logs y estado de Kafka."""
        with self.lock:
            estaciones = [{"id": estacion.id,
                           "ubicacion": estacion.ubicacion,
                           "pais": estacion.pais or "-",
                           "estado": estacion.estado_real(),
                           "caudal": round(estacion.caudal, 1),
                           "volumen": round(estacion.volumen, 2),
                           "operario": estacion.operario}
                          for estacion in sorted(self.estaciones.values(),
                                                 key=lambda estacion: estacion.id)]
            logs = list(self.logs)[-LINEAS_LOG_EN_PANEL:]
        return {"ws": estaciones, "logs": logs, "kafka": self.kafka_conectado}

    # ------------------ Arranque ------------------
    def run(self):
        # 1. Sockets primero: los Monitores deben poder conectar SIEMPRE, aunque Kafka no este
        try:
            servidor_sockets = self.abrir_servidor_de_sockets()
        except OSError as error:
            self.escribir_log(f"No se pudo abrir el puerto {self.puerto_sockets} para los Monitores: {error}")
            sys.exit(1)
        threading.Thread(target=self.aceptar_monitores, args=(servidor_sockets,), daemon=True).start()

        # 2. Kafka en segundo plano (reintenta hasta que el broker este disponible)
        threading.Thread(target=self.iniciar_kafka, daemon=True).start()

        # 3. Panel web (bloquea hasta Ctrl+C)
        try:
            servidor_web = ThreadingHTTPServer(("0.0.0.0", self.puerto_web), crear_manejador_http(self))
        except OSError as error:
            self.escribir_log(f"No se pudo abrir el puerto {self.puerto_web} del panel web: {error}")
            sys.exit(1)
        self.escribir_log(f"Panel de monitorizacion en http://0.0.0.0:{self.puerto_web}")
        try:
            servidor_web.serve_forever()
        except KeyboardInterrupt:
            self.escribir_log("CENTRAL detenida por el usuario")


# =====================================================================
# Servidor HTTP del panel
# =====================================================================
HTML = r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>WM_Central</title>
  <style>
    :root {
      --fondo: #0f1a1f;
      --panel: #16252c;
      --texto: #e6f0f2;
      --texto-suave: #8aa0a8;
      --linea: #27404a;
    }
    * { box-sizing: border-box; }

    body {
      margin: 0;
      padding: 16px;
      background: var(--fondo);
      color: var(--texto);
      font-family: system-ui, sans-serif;
    }
    h1 { font-size: 18px; margin: 0 0 12px; }
    h1 span { color: var(--texto-suave); font-weight: 400; }

    /* Barra de botones */
    .barra {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 12px;
      align-items: center;
    }
    button {
      background: var(--panel);
      color: var(--texto);
      border: 1px solid var(--linea);
      padding: 8px 12px;
      border-radius: 6px;
      cursor: pointer;
    }
    button:hover { border-color: #4a7a8a; }
    label { color: var(--texto-suave); font-size: 13px; }

    /* Resumen: cuantas WS hay en cada estado */
    .resumen {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    .etiqueta {
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid var(--linea);
      background: var(--panel);
    }

    /* Tabla de estaciones */
    .contenedor-tabla { overflow-x: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      font-size: 14px;
    }
    th, td {
      padding: 8px 10px;
      text-align: left;
      border-bottom: 1px solid var(--linea);
    }
    th { color: var(--texto-suave); font-weight: 500; }
    tr { cursor: pointer; }
    tr.seleccionada td { outline: 2px solid #6fb3c8; outline-offset: -2px; }

    /* Un color por estado */
    .DISPONIBLE td        { background: #1f5c2e; }
    .REGANDO td           { animation: parpadeo 1s infinite; }
    .FUGA td              { background: #8b1e1e; }
    .FUERA_DE_SERVICIO td { background: #9a5b0f; }
    .DESCONECTADA td      { background: #4a4f52; color: #cfd6d9; }

    @keyframes parpadeo {
      0%, 100% { background: #1f5c2e; }
      50%      { background: #3fbf5f; color: #06210f; }
    }

    /* Registro de mensajes */
    #registro {
      margin-top: 12px;
      background: #0a1114;
      border: 1px solid var(--linea);
      border-radius: 6px;
      padding: 10px;
      height: 240px;
      overflow-y: auto;
      font: 12px/1.5 ui-monospace, Menlo, Consolas, monospace;
      color: #bfe8c8;
      white-space: pre-wrap;
    }
  </style>
</head>
<body>
  <h1>WM_Central <span>· Irrigation Monitoring Panel</span></h1>

  <div class="barra">
    <button onclick="enviarOrden('start', false)">Iniciar riego</button>
    <button onclick="enviarOrden('block', false)">Bloquear WS</button>
    <button onclick="enviarOrden('activate', false)">Activar WS</button>
    <span style="width:12px"></span>
    <button onclick="enviarOrden('start', true)">Iniciar en todas</button>
    <button onclick="enviarOrden('block', true)">Bloquear todas</button>
    <button onclick="enviarOrden('activate', true)">Activar todas</button>
    <label id="info-seleccion">Selecciona una WS en la tabla</label>
  </div>

  <div class="resumen" id="resumen"></div>

  <div class="contenedor-tabla">
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Ubicación</th>
          <th>País</th>
          <th>Estado</th>
          <th>Caudal (L/min)</th>
          <th>Volumen (L)</th>
          <th>Operario</th>
        </tr>
      </thead>
      <tbody id="tabla"></tbody>
    </table>
  </div>

  <div id="registro"></div>

  <script>
    let estacionSeleccionada = null;

    const NOMBRE_DE_ESTADO = {
      DISPONIBLE: 'DISPONIBLE',
      REGANDO: 'REGANDO',
      FUGA: 'FUGA',
      FUERA_DE_SERVICIO: 'FUERA DE SERVICIO',
      DESCONECTADA: 'DESCONECTADA',
    };

    const CARACTERES_HTML = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' };

    /** Evita que un texto recibido se interprete como HTML. */
    function escapar(texto) {
      return String(texto).replace(/[&<>"]/g, caracter => CARACTERES_HTML[caracter]);
    }

    function pintarResumen(estado) {
      const cuantasPorEstado = {};
      for (const estacion of estado.ws) {
        cuantasPorEstado[estacion.estado] = (cuantasPorEstado[estacion.estado] || 0) + 1;
      }

      let html = '';
      for (const nombreInterno of Object.keys(NOMBRE_DE_ESTADO)) {
        const cuantas = cuantasPorEstado[nombreInterno] || 0;
        html += `<span class="etiqueta">${NOMBRE_DE_ESTADO[nombreInterno]}: ${cuantas}</span>`;
      }

      let textoKafka = 'SIN CONEXIÓN';
      if (estado.kafka) {
        textoKafka = 'conectado';
      }
      html += `<span class="etiqueta">Kafka: ${textoKafka}</span>`;

      document.getElementById('resumen').innerHTML = html;
    }

    function pintarFila(estacion) {
      const estaRegando = estacion.estado === 'REGANDO';

      let clases = estacion.estado;
      if (estacion.id === estacionSeleccionada) {
        clases += ' seleccionada';
      }

      let caudal = '—';
      let volumen = '—';
      let operario = '—';
      if (estaRegando) {
        caudal = estacion.caudal;
        volumen = estacion.volumen;
        operario = escapar(estacion.operario);
      }

      return `
        <tr class="${clases}" onclick="seleccionar('${escapar(estacion.id)}')">
          <td>${escapar(estacion.id)}</td>
          <td>${escapar(estacion.ubicacion)}</td>
          <td>${escapar(estacion.pais)}</td>
          <td><b>${NOMBRE_DE_ESTADO[estacion.estado]}</b></td>
          <td>${caudal}</td>
          <td>${volumen}</td>
          <td>${operario}</td>
        </tr>`;
    }

    function pintarTabla(estado) {
      document.getElementById('tabla').innerHTML = estado.ws.map(pintarFila).join('');
    }

    function pintarRegistro(estado) {
      const registro = document.getElementById('registro');
      const estabaAlFinal = registro.scrollTop + registro.clientHeight >= registro.scrollHeight - 20;

      registro.textContent = estado.logs.join('\n');
      if (estabaAlFinal) {
        registro.scrollTop = registro.scrollHeight;
      }
    }

    function pintar(estado) {
      pintarResumen(estado);
      pintarTabla(estado);
      pintarRegistro(estado);
    }

    function seleccionar(id) {
      estacionSeleccionada = id;
      document.getElementById('info-seleccion').textContent = 'Seleccionada: WS ' + id;
      actualizar();
    }

    async function actualizar() {
      try {
        const respuesta = await fetch('/api/state');
        pintar(await respuesta.json());
      } catch (error) {
        // Sin conexión con Central: se mantiene lo que ya se estaba mostrando
      }
    }

    async function enviarOrden(orden, aTodas) {
      let objetivo = estacionSeleccionada;
      if (aTodas) {
        objetivo = 'ALL';
      }
      if (!objetivo) {
        alert('Selecciona una WS');
        return;
      }

      await fetch('/api/cmd', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cmd: orden, target: objetivo }),
      });
      actualizar();
    }

    setInterval(actualizar, 1000);
    actualizar();
  </script>
</body>
</html>
"""


def crear_manejador_http(central):
    pagina_html = HTML.encode("utf-8")

    class ManejadorHttp(BaseHTTPRequestHandler):
        def log_message(self, *argumentos):
            pass        # sin trazas de cada peticion HTTP

        def responder(self, codigo, cuerpo, tipo_contenido):
            self.send_response(codigo)
            self.send_header("Content-Type", tipo_contenido)
            self.send_header("Content-Length", str(len(cuerpo)))
            self.end_headers()
            self.wfile.write(cuerpo)

        def ejecutar_sin_romper(self, accion):
            """Un fallo atendiendo una peticion no debe tumbar el servidor del panel."""
            try:
                accion()
            except (BrokenPipeError, ConnectionResetError):
                pass        # el navegador cerro la conexion
            except Exception as error:
                central.escribir_log(f"Error en el panel web ({error!r})")
                try:
                    self.responder(500, b"internal error", "text/plain")
                except OSError:
                    pass

        def do_GET(self):
            self.ejecutar_sin_romper(self.atender_get)

        def do_POST(self):
            self.ejecutar_sin_romper(self.atender_post)

        def atender_get(self):
            if self.path == "/api/state":
                cuerpo = json.dumps(central.obtener_estado()).encode()
                self.responder(200, cuerpo, "application/json")
            elif self.path in ("/", "/index.html"):
                self.responder(200, pagina_html, "text/html; charset=utf-8")
            else:
                self.responder(404, b"not found", "text/plain")

        def atender_post(self):
            if self.path != "/api/cmd":
                return self.responder(404, b"not found", "text/plain")

            try:
                longitud = int(self.headers.get("Content-Length", 0))
                datos = json.loads(self.rfile.read(longitud))
                comando = datos["cmd"]
                objetivo = str(datos["target"])
                if comando not in ("start", "block", "activate"):
                    raise ValueError("comando desconocido")
            except Exception:
                return self.responder(400, b"bad request", "text/plain")

            central.ejecutar_orden(comando, objetivo)
            self.responder(200, b'{"ok":true}', "application/json")

    return ManejadorHttp


def leer_puerto(texto, nombre):
    """Convierte un argumento en numero de puerto (1-65535) o termina con un mensaje claro."""
    try:
        puerto = int(texto)
    except ValueError:
        puerto = 0
    if not 1 <= puerto <= 65535:
        print(f"Puerto no valido para {nombre}: {texto!r}")
        sys.exit(1)
    return puerto


def main():
    if len(sys.argv) < 3:
        print("Uso: python WM_Central.py <puerto_sockets> <broker_ip:puerto> [fichero_bd]")
        sys.exit(1)

    puerto_sockets = leer_puerto(sys.argv[1], "puerto_sockets")
    broker = sys.argv[2]
    if len(sys.argv) > 3:
        ruta_bd = sys.argv[3]
    else:
        ruta_bd = "Base_Datos.sqlite"
    puerto_web = leer_puerto(os.environ.get("PORT", "8080"), "PORT")

    try:
        central = Central(puerto_sockets, broker, ruta_bd, puerto_web)
    except sqlite3.Error as error:
        print(f"No se pudo abrir la base de datos '{ruta_bd}': {error}")
        sys.exit(1)
    central.run()


if __name__ == "__main__":
    main()