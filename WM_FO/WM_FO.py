"""WM_FO - Field Operator (aplicacion del operario de campo).

Uso: python WM_FO.py <broker_ip:puerto> <id_operador> [fichero_servicios]

- Pide riegos a CENTRAL por Kafka y muestra cada paso del proceso: peticion, comprobaciones,
  autorizacion o denegacion, caudal y volumen en tiempo real y ticket final.
- Tiene un panel web (puerto de la variable PORT, 8081 por defecto) para que el jardinero
  lo use desde el navegador del PC o del movil. Funciona igual dentro de Docker.
- Puede leer un fichero con los servicios a solicitar (desde el panel o como tercer argumento):
  pide el primero y, cuando termina (con exito o fracaso), espera 4 s y pide el siguiente.
  Formatos: JSON [{"idWS": "1", "tiempo": 30}] o texto con una linea "ID [segundos]".
  El tiempo (s) es opcional: sin el, CENTRAL usa el limite por defecto.
"""
import json
import os
import re
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import KafkaError, KafkaException

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# El FO solo habla Kafka: no necesita protocolos.py (sockets)
from topics import (
    AUTORIZADO, REGANDO,
    GROUP_OPERATOR_EVENTS, GROUP_OPERATOR_SUMMARIES,
    TOPIC_AVAILABLE_STATIONS, TOPIC_IRRIGATION_FLOW_READING, TOPIC_IRRIGATION_REQUEST,
    TOPIC_IRRIGATION_SUMMARY, TOPIC_PROGRESS_OF_IRRIGATION_REQUEST,
    TOPIC_REPLY_TO_IRRIGATION_REQUEST,
    PAIS_POR_DEFECTO, PAISES, es_pais_valido,
    JsonProducer, confirmar, ensure_topics, kafka_loads, make_consumer,
)

# ------------------------- Tiempos (segundos) -------------------------
ESPERA_ENTRE_SERVICIOS_S = 4        # entre el final de un servicio y la peticion del siguiente
TIMEOUT_SERVICIO_S = 120            # maximo esperando a que acabe un servicio del fichero
TIMEOUT_SOLICITUD_S = 30            # maximo esperando la respuesta de CENTRAL a una peticion
LISTA_CADUCADA_S = 10               # sin recibir la lista de WS => CENTRAL no responde
SIN_DATOS_S = 5                     # sin consumo de una WS regando => se avisa al operario
ESPERA_CENTRAL_ANTES_DE_SERVICIO_S = 30
ESPERA_REINTENTO_KAFKA_S = 3
ESPERA_TRAS_ERROR_KAFKA_S = 2
ESPERA_ASIGNACION_PARTICIONES_S = 30
ESPERA_CONSULTA_KAFKA_S = 1.0
PERIODO_VIGILANCIA_CENTRAL_S = 1
PERIODO_COMPROBACION_CENTRAL_S = 0.5

# ------------------------- Limites -------------------------
TIEMPO_MAXIMO_RIEGO_S = 3600
MAX_LINEAS_LOG = 300
LINEAS_LOG_EN_PANEL = 120
MAX_TICKETS_EN_PANEL = 50

PATRON_ID_VALIDO = re.compile(r"^[A-Za-z0-9_-]{1,10}$")

# ------------------------- Estados de un riego (lo que ve el operario) -------------------------
RIEGO_SOLICITADO = "SOLICITADO"
RIEGO_AUTORIZADO = "AUTORIZADO"
RIEGO_REGANDO = REGANDO
RIEGO_SIN_DATOS = "SIN_DATOS"
RIEGO_DENEGADO = "DENEGADO"

# ------------------------- Estados de un servicio del fichero -------------------------
SERVICIO_PENDIENTE = "PENDIENTE"
SERVICIO_EN_CURSO = "EN CURSO"
SERVICIO_ERROR = "ERROR"
SERVICIO_SIN_RESPUESTA = "SIN RESPUESTA"
SERVICIO_CANCELADO = "CANCELADO"

# Como se muestra al operario el motivo con el que termina un riego
MOTIVOS_LEGIBLES = {
    "LIMITE_TIEMPO": "limite de tiempo alcanzado",
    "FIN_MANUAL": "finalizado desde la estacion",
    "FUGA": "fuga detectada en la estacion",
    "BLOQUEO_CENTRAL": "estacion bloqueada por CENTRAL",
    "ENGINE_CERRADO": "estacion apagada",
}


class RiegoEnCurso:
    """Un riego pedido por este operario (desde que se solicita hasta que llega su ticket)."""

    def __init__(self, estado):
        self.estado = estado
        self.caudal = 0.0
        self.volumen = 0.0
        self.duracion = 0.0
        self.ultima_actualizacion = time.time()

    def estado_visible(self):
        """Un riego que ya no recibe consumo (WS averiada o caida) se muestra como SIN_DATOS."""
        if self.estado == RIEGO_REGANDO and time.time() - self.ultima_actualizacion > SIN_DATOS_S:
            return RIEGO_SIN_DATOS
        return self.estado


class Servicio:
    """Un servicio de la lista leida de fichero: regar en una WS (con tiempo opcional)."""

    def __init__(self, ws_id, tiempo_s):
        self.ws_id = ws_id
        self.tiempo_s = tiempo_s
        self.estado = SERVICIO_PENDIENTE


class FieldOperator:
    def __init__(self, broker, operario, pais, puerto_web):
        self.broker = broker
        self.operario = str(operario)
        self.pais = pais                         # el FO solo lee y escribe en la particion de su pais
        self.puerto_web = puerto_web

        self.lock = threading.RLock()
        self.logs = deque(maxlen=MAX_LINEAS_LOG)

        # Kafka
        self.productor = None
        self.consumidor_general = None
        self.consumidor_tickets = None
        self.particiones_general_asignadas = threading.Event()
        self.particiones_tickets_asignadas = threading.Event()
        self.kafka_conectado = False
        self.kafka_conectado_desde = 0.0

        # Lo que se sabe de CENTRAL y de las WS
        self.ws_disponibles = []                 # [{"idWS", "ubicacion"}]
        self.ultima_lista_recibida = 0.0
        self.central_estaba_viva = None          # para avisar solo cuando cambia

        # Riegos de este operario
        self.riegos = {}                         # idWS -> RiegoEnCurso
        self.tickets = []                        # historial (el mas reciente primero)
        self.sesiones_cerradas = set()           # sesiones de riego que ya tienen ticket

        # Servicios leidos de un fichero
        self.servicios = []
        self.lista_activa = False
        self.cancelar_lista = False
        self.servicio_terminado = threading.Event()
        self.ws_del_servicio_actual = None
        self.resultado_del_servicio_actual = None

        # Que hacer con cada mensaje de Kafka segun su topic (TOPIC_AVAILABLE_STATIONS se trata aparte: es una lista)
        self.manejadores_kafka = {
            TOPIC_PROGRESS_OF_IRRIGATION_REQUEST: self.recibir_aviso,
            TOPIC_REPLY_TO_IRRIGATION_REQUEST: self.recibir_autorizacion,
            TOPIC_IRRIGATION_FLOW_READING: self.recibir_consumo,
            TOPIC_IRRIGATION_SUMMARY: self.recibir_ticket,
        }

    def escribir_log(self, texto):
        linea = f"[{time.strftime('%H:%M:%S')}] {texto}"
        with self.lock:
            self.logs.append(linea)
        print(f"[FO {self.operario}] {linea}", flush=True)

    # ------------------ Kafka ------------------
    def iniciar_kafka(self):
        """Crea productor y consumidores; reintenta hasta que el broker este disponible."""
        while True:
            try:
                ensure_topics(self.broker)
                self.productor = JsonProducer(self.broker)

                # Efimeros: avisos, autorizaciones, caudal en tiempo real y lista de WS
                self.consumidor_general = make_consumer(
                    self.broker,
                    [TOPIC_PROGRESS_OF_IRRIGATION_REQUEST, TOPIC_REPLY_TO_IRRIGATION_REQUEST,
                     TOPIC_IRRIGATION_FLOW_READING, TOPIC_AVAILABLE_STATIONS],
                    f"{GROUP_OPERATOR_EVENTS}-{self.operario}", self.particiones_general_asignadas,
                    pais=self.pais)

                # Tickets: grupo FIJO por operario y offsets confirmados => si el FO se cierra
                # durante un riego, al volver ve el resultado de su servicio
                self.consumidor_tickets = make_consumer(
                    self.broker, [TOPIC_IRRIGATION_SUMMARY],
                    f"{GROUP_OPERATOR_SUMMARIES}-{self.operario}",
                    self.particiones_tickets_asignadas, durable=True, pais=self.pais)
                return
            except Exception as error:
                self.escribir_log(f"Broker Kafka no disponible ({error}), reintentando en "
                                  f"{ESPERA_REINTENTO_KAFKA_S} s...")
                time.sleep(ESPERA_REINTENTO_KAFKA_S)

    def arrancar_kafka(self, fichero_servicios=None):
        self.iniciar_kafka()
        threading.Thread(target=self.escuchar_kafka, args=(self.consumidor_general, False),
                         daemon=True).start()
        threading.Thread(target=self.escuchar_kafka, args=(self.consumidor_tickets, True),
                         daemon=True).start()
        self.particiones_general_asignadas.wait(ESPERA_ASIGNACION_PARTICIONES_S)
        self.particiones_tickets_asignadas.wait(ESPERA_ASIGNACION_PARTICIONES_S)

        self.kafka_conectado = True
        self.kafka_conectado_desde = time.time()
        self.escribir_log(f"Conectado a Kafka ({self.broker})")

        if fichero_servicios:
            self.lanzar_servicios_del_fichero(fichero_servicios)

    def lanzar_servicios_del_fichero(self, ruta):
        try:
            with open(ruta, "r", encoding="utf-8") as fichero:
                texto = fichero.read()
        except OSError as error:
            self.escribir_log(f"No se pudo leer el fichero '{ruta}': {error}")
            return

        _, mensaje_resultado = self.lanzar_servicios(self.parsear_servicios(texto))
        self.escribir_log(mensaje_resultado)

    def escuchar_kafka(self, consumidor, es_durable):
        """Hilo: lee mensajes del consumidor y los reparte al manejador de su topic."""
        while True:
            try:
                mensaje_kafka = consumidor.poll(ESPERA_CONSULTA_KAFKA_S)
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

            topic = mensaje_kafka.topic()
            try:
                self.procesar_mensaje(topic, kafka_loads(mensaje_kafka))
            except Exception as error:
                self.escribir_log(f"Error procesando mensaje de {topic}: {error}")

            if es_durable:
                confirmar(consumidor, mensaje_kafka)

    def procesar_mensaje(self, topic, datos):
        if topic == TOPIC_AVAILABLE_STATIONS:
            self.recibir_lista(datos)
            return

        if not isinstance(datos, dict):
            return
        manejador = self.manejadores_kafka.get(topic)
        if manejador:
            manejador(datos)

    # ------------------ Mensajes recibidos ------------------
    def recibir_lista(self, datos):
        """TOPIC_AVAILABLE_STATIONS: estaciones disponibles (CENTRAL la publica cada pocos segundos)."""
        if not isinstance(datos, list):
            return
        with self.lock:
            self.ws_disponibles = [{"idWS": str(ws.get("idWS")),
                                    "ubicacion": str(ws.get("ubicacion", ""))}
                                   for ws in datos if isinstance(ws, dict)]
            self.ultima_lista_recibida = time.time()

    def es_para_este_operario(self, datos):
        return str(datos.get("idOperator")) == self.operario

    def recibir_aviso(self, datos):
        """TOPIC_PROGRESS_OF_IRRIGATION_REQUEST: paso del proceso de autorizacion (solo los de este operario)."""
        if not self.es_para_este_operario(datos):
            return
        self.escribir_log(f"CENTRAL: {datos.get('msg')} (WS {datos.get('idWS')})")

    def recibir_autorizacion(self, datos):
        """TOPIC_REPLY_TO_IRRIGATION_REQUEST: CENTRAL autoriza o deniega el riego pedido."""
        if not self.es_para_este_operario(datos):
            return

        ws_id = str(datos.get("idWS"))
        if datos.get("authorize") == AUTORIZADO:
            with self.lock:
                if ws_id in self.riegos:
                    self.riegos[ws_id].estado = RIEGO_AUTORIZADO
            self.escribir_log(f"Riego AUTORIZADO en WS {ws_id}. Esperando datos de la estacion...")
            return

        motivo = datos.get("motivo", "")
        with self.lock:
            self.riegos.pop(ws_id, None)
            self.tickets.insert(0, self.crear_fila_de_historial(ws_id, RIEGO_DENEGADO, motivo, 0, 0))
        self.escribir_log(f"Riego DENEGADO en WS {ws_id}: {motivo}")
        self.finalizar_servicio(ws_id, RIEGO_DENEGADO)

    def recibir_consumo(self, datos):
        """TOPIC_IRRIGATION_FLOW_READING: caudal y volumen en tiempo real del riego de este operario."""
        if not self.es_para_este_operario(datos):
            return

        sesion = datos.get("sesion")
        if sesion is not None and sesion in self.sesiones_cerradas:
            return      # consumo rezagado de un riego que ya tiene ticket

        ws_id = str(datos.get("idWS"))
        riego = RiegoEnCurso(RIEGO_REGANDO)
        riego.caudal = float(datos.get("caudal", 0))
        riego.volumen = float(datos.get("volumen", 0))
        riego.duracion = float(datos.get("duracion", 0))
        with self.lock:
            self.riegos[ws_id] = riego

        print(f"[FO {self.operario}] WS {ws_id} regando | {riego.caudal:.1f} L/min | "
              f"{riego.volumen:.2f} L | {riego.duracion:.0f} s", flush=True)

    def recibir_ticket(self, datos):
        """TOPIC_IRRIGATION_SUMMARY: resumen final del riego."""
        ticket = datos.get("ticket", {})
        if not self.es_para_este_operario(ticket):
            return

        ws_id = str(ticket.get("idWS"))
        estado = str(ticket.get("estado", ""))
        motivo = MOTIVOS_LEGIBLES.get(ticket.get("motivo"), ticket.get("motivo", ""))
        volumen = float(ticket.get("volumen", 0))
        duracion = float(ticket.get("duracion", 0))

        with self.lock:
            if ticket.get("sesion") is not None:
                self.sesiones_cerradas.add(ticket.get("sesion"))
            self.riegos.pop(ws_id, None)
            self.tickets.insert(0, self.crear_fila_de_historial(ws_id, estado, motivo, volumen, duracion))

        self.escribir_log(f"TICKET WS {ws_id}: riego {estado} ({motivo}) - "
                          f"{volumen:.2f} L en {duracion:.1f} s")
        self.finalizar_servicio(ws_id, estado)

    @staticmethod
    def crear_fila_de_historial(ws_id, estado, motivo, volumen, duracion):
        return {"hora": time.strftime("%H:%M:%S"),
                "idWS": ws_id,
                "estado": estado,
                "motivo": motivo,
                "volumen": round(volumen, 2),
                "duracion": round(duracion, 1)}

    def finalizar_servicio(self, ws_id, resultado):
        """Un servicio del fichero acaba al llegar la denegacion o el ticket de su WS."""
        if self.ws_del_servicio_actual == ws_id:
            self.resultado_del_servicio_actual = resultado
            self.servicio_terminado.set()

    # ------------------ Estado de CENTRAL ------------------
    def central_esta_viva(self):
        """CENTRAL publica la lista de WS cada 2 s: si deja de llegar, no responde."""
        return time.time() - self.ultima_lista_recibida < LISTA_CADUCADA_S

    def esperar_a_central(self, segundos):
        """Espera hasta 'segundos' a que CENTRAL responda. Devuelve si responde."""
        limite = time.time() + segundos
        while time.time() < limite and not self.cancelar_lista:
            if self.central_esta_viva():
                return True
            time.sleep(PERIODO_COMPROBACION_CENTRAL_S)
        return self.central_esta_viva()

    def vigilar_central(self):
        """Hilo: avisa cuando CENTRAL deja de responder y cuando vuelve."""
        while True:
            viva = self.central_esta_viva()
            if viva != self.central_estaba_viva:
                self.avisar_cambio_de_central(viva)
            time.sleep(PERIODO_VIGILANCIA_CENTRAL_S)

    def avisar_cambio_de_central(self, viva):
        if viva:
            self.escribir_log("WM_Central en linea")
            self.central_estaba_viva = True
            return

        # Justo tras conectar a Kafka todavia no ha dado tiempo a recibir ninguna lista:
        # solo se avisa si CENTRAL ya estaba viva o si lleva un rato sin aparecer
        segundos_conectado = time.time() - self.kafka_conectado_desde
        if self.central_estaba_viva or (self.kafka_conectado and segundos_conectado > LISTA_CADUCADA_S):
            self.escribir_log("Imposible conectar con WM_Central (sin respuesta)")
            self.central_estaba_viva = False

    # ------------------ Peticiones ------------------
    @staticmethod
    def parsear_tiempo(valor):
        """None o '' -> None (limite por defecto de CENTRAL). Lanza ValueError si no es valido."""
        if valor is None or str(valor).strip() == "":
            return None

        tiempo_s = float(valor)
        if not 0 < tiempo_s <= TIEMPO_MAXIMO_RIEGO_S:
            raise ValueError("tiempo fuera de rango")
        return tiempo_s

    def solicitar(self, ws_id, tiempo=None):
        """Pide un riego a CENTRAL. Devuelve (se_envio, mensaje_para_el_operario)."""
        ws_id = str(ws_id).strip()
        if not PATRON_ID_VALIDO.match(ws_id):
            return False, "ID de WS no valido"

        try:
            limite_s = self.parsear_tiempo(tiempo)
        except (TypeError, ValueError):
            return False, f"Tiempo no valido (entre 1 y {TIEMPO_MAXIMO_RIEGO_S} s)"

        if not self.kafka_conectado:
            return False, "Sin conexion con Kafka todavia"
        if not self.central_esta_viva():
            return False, "Imposible conectar con WM_Central"

        with self.lock:
            if ws_id in self.riegos:
                return False, f"Ya tienes un riego solicitado o en curso en la WS {ws_id}"
            self.riegos[ws_id] = RiegoEnCurso(RIEGO_SOLICITADO)

        peticion = {"idOperator": self.operario, "idWS": ws_id}
        if limite_s:
            peticion["limite"] = limite_s
        if not self.productor.send(TOPIC_IRRIGATION_REQUEST, peticion, self.pais):
            with self.lock:
                self.riegos.pop(ws_id, None)
            return False, "No se pudo enviar la peticion a Kafka"
        self.productor.flush()

        texto_log = f"Peticion de riego enviada a CENTRAL para la WS {ws_id}"
        if limite_s:
            texto_log += f" ({limite_s:.0f} s)"
        self.escribir_log(texto_log)
        return True, f"Peticion enviada para la WS {ws_id}"

    def descartar_solicitudes_sin_respuesta(self):
        """Quita las peticiones a las que CENTRAL no ha respondido a tiempo."""
        descartadas = []
        with self.lock:
            for ws_id, riego in list(self.riegos.items()):
                esperando_respuesta = riego.estado in (RIEGO_SOLICITADO, RIEGO_AUTORIZADO)
                if esperando_respuesta and time.time() - riego.ultima_actualizacion > TIMEOUT_SOLICITUD_S:
                    descartadas.append(ws_id)
                    del self.riegos[ws_id]

        for ws_id in descartadas:
            self.escribir_log(f"Sin respuesta de CENTRAL para la WS {ws_id}")

    # ------------------ Servicios desde fichero ------------------
    @staticmethod
    def leer_json_si_es_lista(texto):
        """Devuelve la lista si el texto es un JSON con una lista; si no, None."""
        try:
            datos = json.loads(texto)
        except ValueError:
            return None
        if isinstance(datos, list):
            return datos
        return None

    @staticmethod
    def pares_desde_lista_json(elementos):
        """[{"idWS": "1", "tiempo": 30}, "2"] -> [("1", 30), ("2", None)]."""
        pares = []
        for elemento in elementos:
            if isinstance(elemento, dict):
                pares.append((str(elemento.get("idWS")), elemento.get("tiempo")))
            else:
                pares.append((str(elemento), None))
        return pares

    @staticmethod
    def pares_desde_texto(texto):
        """Una linea por servicio, 'ID [segundos]' -> [("1", "30"), ("2", None)]."""
        pares = []
        for linea in texto.splitlines():
            campos = [campo for campo in re.split(r"[\s,;]+", linea.strip()) if campo]
            if not campos:
                continue
            tiempo = None
            if len(campos) > 1:
                tiempo = campos[1]
            pares.append((campos[0], tiempo))
        return pares

    @classmethod
    def parsear_servicios(cls, texto):
        """Devuelve [(idWS, tiempo_s | None)] con los servicios validos del fichero."""
        elementos = cls.leer_json_si_es_lista(texto)
        if elementos is not None:
            pares = cls.pares_desde_lista_json(elementos)
        else:
            pares = cls.pares_desde_texto(texto)

        servicios = []
        for ws_id, tiempo in pares:
            try:
                tiempo_s = cls.parsear_tiempo(tiempo)
            except (TypeError, ValueError):
                continue        # un tiempo invalido descarta ese servicio
            if PATRON_ID_VALIDO.match(ws_id):
                servicios.append((ws_id, tiempo_s))
        return servicios

    def lanzar_servicios(self, servicios):
        """Empieza a procesar la lista en un hilo. Devuelve (se_lanzo, mensaje)."""
        if not servicios:
            return False, "El fichero no contiene servicios validos"
        if not self.kafka_conectado:
            return False, "Sin conexion con Kafka todavia"

        with self.lock:
            if self.lista_activa:
                return False, "Ya hay una lista de servicios en curso"
            self.servicios = [Servicio(ws_id, tiempo_s) for ws_id, tiempo_s in servicios]
            self.lista_activa = True
            self.cancelar_lista = False

        ids = ", ".join(ws_id for ws_id, _ in servicios)
        self.escribir_log(f"Lista cargada: {len(servicios)} servicios -> {ids}")
        threading.Thread(target=self.procesar_servicios, daemon=True).start()
        return True, f"Lista cargada: {len(servicios)} servicios"

    def procesar_servicios(self):
        """Hilo: pide los servicios uno a uno; entre uno y otro espera unos segundos."""
        servicios = self.servicios
        total = len(servicios)

        for posicion, servicio in enumerate(servicios):
            if self.cancelar_lista:
                for pendiente in servicios[posicion:]:
                    pendiente.estado = SERVICIO_CANCELADO
                break

            self.procesar_un_servicio(servicio, posicion, total)

            es_el_ultimo = posicion == total - 1
            if not es_el_ultimo and not self.cancelar_lista:
                self.escribir_log(f"Siguiente servicio en {ESPERA_ENTRE_SERVICIOS_S} s...")
                time.sleep(ESPERA_ENTRE_SERVICIOS_S)

        if self.cancelar_lista:
            self.escribir_log("Lista de servicios cancelada")
        else:
            self.escribir_log("Lista de servicios completada")
        with self.lock:
            self.lista_activa = False

    def procesar_un_servicio(self, servicio, posicion, total):
        """Pide un servicio y espera a que termine (ticket o denegacion)."""
        ws_id = servicio.ws_id
        self.escribir_log(f"--- Servicio {posicion + 1}/{total}: WS {ws_id} ---")

        servicio.estado = SERVICIO_EN_CURSO
        self.servicio_terminado.clear()
        self.resultado_del_servicio_actual = None
        self.ws_del_servicio_actual = ws_id

        if self.esperar_a_central(ESPERA_CENTRAL_ANTES_DE_SERVICIO_S):
            se_envio, mensaje_resultado = self.solicitar(ws_id, servicio.tiempo_s)
        else:
            se_envio, mensaje_resultado = False, "Imposible conectar con WM_Central"

        if not se_envio:
            servicio.estado = SERVICIO_ERROR
            self.escribir_log(mensaje_resultado)
        elif not self.servicio_terminado.wait(TIMEOUT_SERVICIO_S):
            servicio.estado = SERVICIO_SIN_RESPUESTA
            self.escribir_log(f"Sin respuesta de CENTRAL para la WS {ws_id}. Paso al siguiente")
        else:
            # Si se cancelo la lista mientras esperaba, no hay resultado
            servicio.estado = self.resultado_del_servicio_actual or SERVICIO_CANCELADO

        self.ws_del_servicio_actual = None

    def cancelar_servicios(self):
        with self.lock:
            if not self.lista_activa:
                return False, "No hay ninguna lista en curso"
            self.cancelar_lista = True
        self.servicio_terminado.set()
        return True, "Lista cancelada (el riego ya iniciado seguira hasta terminar)"

    # ------------------ Estado para el panel ------------------
    def estado_para_panel(self):
        self.descartar_solicitudes_sin_respuesta()
        with self.lock:
            riegos = [{"idWS": ws_id,
                       "estado": riego.estado_visible(),
                       "caudal": round(riego.caudal, 1),
                       "volumen": round(riego.volumen, 2),
                       "duracion": round(riego.duracion)}
                      for ws_id, riego in self.riegos.items()]
            servicios = [{"idWS": servicio.ws_id,
                          "tiempo": servicio.tiempo_s,
                          "estado": servicio.estado}
                         for servicio in self.servicios]
            return {"operario": self.operario,
                    "pais": self.pais,
                    "kafka": self.kafka_conectado,
                    "central": self.central_esta_viva(),
                    "ws": list(self.ws_disponibles),
                    "riegos": riegos,
                    "tickets": self.tickets[:MAX_TICKETS_EN_PANEL],
                    "secuencia": {"activa": self.lista_activa, "items": servicios},
                    "logs": list(self.logs)[-LINEAS_LOG_EN_PANEL:]}

    # ------------------ Arranque ------------------
    def run(self, fichero_servicios=None):
        threading.Thread(target=self.arrancar_kafka, args=(fichero_servicios,), daemon=True).start()
        threading.Thread(target=self.vigilar_central, daemon=True).start()

        try:
            servidor_web = ThreadingHTTPServer(("0.0.0.0", self.puerto_web), crear_manejador_http(self))
        except OSError as error:
            self.escribir_log(f"No se pudo abrir el puerto {self.puerto_web} del panel web: {error}")
            sys.exit(1)
        self.escribir_log(f"Panel del operario en http://localhost:{self.puerto_web}")
        try:
            servidor_web.serve_forever()
        except KeyboardInterrupt:
            pass


# =====================================================================
# Servidor HTTP del panel
# =====================================================================
HTML = r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>WM_FO</title>
  <style>
    :root {
      --fondo: #0f1a1f;
      --panel: #16252c;
      --texto: #e6f0f2;
      --texto-suave: #8aa0a8;
      --linea: #27404a;
      --verde-oscuro: #1f5c2e;
      --verde-claro: #3fbf5f;
    }
    * { box-sizing: border-box; }

    body {
      margin: 0;
      padding: 16px;
      background: var(--fondo);
      color: var(--texto);
      font-family: system-ui, sans-serif;
    }
    h1 { font-size: 18px; margin: 0; }
    h1 span { color: var(--texto-suave); font-weight: 400; }
    h2 {
      font-size: 14px;
      margin: 0 0 10px;
      color: var(--texto-suave);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: .04em;
    }

    header {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 14px;
    }

    /* Indicadores de conexion (Kafka / CENTRAL) */
    .indicadores { display: flex; gap: 8px; flex-wrap: wrap; }
    .indicador {
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid var(--linea);
      background: var(--panel);
    }
    .indicador.ok { background: var(--verde-oscuro); border-color: var(--verde-claro); }
    .indicador.ko { background: #8b1e1e; border-color: #cc2244; }

    /* Rejilla de secciones */
    .rejilla { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
    @media (max-width: 800px) {
      .rejilla { grid-template-columns: 1fr; }
    }
    section {
      background: var(--panel);
      border: 1px solid var(--linea);
      border-radius: 8px;
      padding: 14px;
      min-width: 0;
    }
    .ancha { grid-column: 1 / -1; }

    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { padding: 8px 6px; text-align: left; border-bottom: 1px solid var(--linea); }
    th { color: var(--texto-suave); font-weight: 500; }
    .contenedor-tabla { overflow-x: auto; }

    button {
      background: #1d3a46;
      color: var(--texto);
      border: 1px solid #3a6577;
      padding: 9px 14px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 14px;
    }
    button:hover { border-color: #6fb3c8; }
    button.principal { background: var(--verde-oscuro); border-color: var(--verde-claro); }

    input[type=text] {
      background: #0a1114;
      color: var(--texto);
      border: 1px solid var(--linea);
      border-radius: 6px;
      padding: 9px;
      width: 110px;
    }
    .fila { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .vacio { color: var(--texto-suave); font-size: 14px; padding: 6px 0; }

    /* Tarjetas de "Mis riegos" */
    .tarjeta {
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 10px;
      border: 1px solid var(--linea);
      background: #1b2f38;
    }
    .tarjeta.REGANDO { animation: parpadeo 1s infinite; border-color: var(--verde-claro); }
    .tarjeta.SIN_DATOS { border-color: #cc2244; background: #3a1d1d; }
    .tarjeta b { font-size: 16px; }
    .tarjeta .detalle { color: var(--texto-suave); font-size: 13px; margin-top: 4px; }
    .tarjeta.REGANDO .detalle { color: #d7ffe1; }

    @keyframes parpadeo {
      0%, 100% { background: #1f5c2e; }
      50%      { background: #2f8f48; }
    }

    /* Etiquetas de la lista de servicios */
    .etiqueta {
      display: inline-block;
      padding: 3px 9px;
      border-radius: 999px;
      font-size: 12px;
      margin: 2px;
      border: 1px solid var(--linea);
    }
    .etiqueta.COMPLETADO { background: var(--verde-oscuro); }
    .etiqueta.DENEGADO, .etiqueta.ERROR, .etiqueta.INTERRUMPIDO { background: #8b1e1e; }
    .etiqueta.EN\ CURSO { background: #9a5b0f; }
    .etiqueta.SIN\ RESPUESTA, .etiqueta.CANCELADO { background: #4a4f52; }

    /* Resultado en el historial */
    .resultado-COMPLETADO { color: #6fe28f; }
    .resultado-INTERRUMPIDO, .resultado-DENEGADO { color: #ff8a8a; }

    /* Registro de mensajes */
    #registro {
      background: #0a1114;
      border: 1px solid var(--linea);
      border-radius: 6px;
      padding: 10px;
      height: 230px;
      overflow-y: auto;
      font: 12px/1.5 ui-monospace, Menlo, Consolas, monospace;
      color: #bfe8c8;
      white-space: pre-wrap;
    }

    /* Aviso emergente */
    #aviso {
      position: fixed;
      bottom: 16px;
      left: 50%;
      transform: translateX(-50%);
      background: #1d3a46;
      border: 1px solid #6fb3c8;
      padding: 10px 16px;
      border-radius: 8px;
      display: none;
      max-width: 90%;
    }
  </style>
</head>
<body>
  <header>
    <h1>WM_FO <span>· Operario <b id="operario"></b> · País <b id="pais"></b></span></h1>
    <div class="indicadores">
      <span class="indicador" id="indicador-kafka">Kafka</span>
      <span class="indicador" id="indicador-central">CENTRAL</span>
    </div>
  </header>

  <div class="rejilla">
    <section>
      <h2>Estaciones disponibles</h2>
      <div class="contenedor-tabla">
        <table>
          <thead>
            <tr><th>WS</th><th>Ubicación</th><th></th></tr>
          </thead>
          <tbody id="estaciones"></tbody>
        </table>
      </div>
      <div id="estaciones-vacio" class="vacio"></div>

      <div class="fila" style="margin-top:12px">
        <input type="text" id="id-manual" placeholder="ID de WS" maxlength="10">
        <input type="text" id="tiempo-manual" placeholder="Segundos (opc.)" maxlength="5" style="width:130px">
        <button onclick="pedirRiego(document.getElementById('id-manual').value, document.getElementById('tiempo-manual').value)">Solicitar por ID</button>
      </div>
    </section>

    <section>
      <h2>Mis riegos</h2>
      <div id="riegos"></div>
      <div id="riegos-vacio" class="vacio">No tienes ningún riego en curso.</div>
    </section>

    <section class="ancha">
      <h2>Servicios desde fichero</h2>
      <div class="fila">
        <input type="file" id="fichero" accept=".json,.txt">
        <button class="principal" onclick="cargarFichero()">Cargar y lanzar</button>
        <button onclick="cancelarLista()">Cancelar lista</button>
      </div>
      <div id="servicios" style="margin-top:10px"></div>
    </section>

    <section class="ancha">
      <h2>Mensajes del proceso</h2>
      <div id="registro"></div>
    </section>

    <section class="ancha">
      <h2>Historial de riegos</h2>
      <div class="contenedor-tabla">
        <table>
          <thead>
            <tr>
              <th>Hora</th><th>WS</th><th>Resultado</th><th>Motivo</th>
              <th>Volumen (L)</th><th>Duración (s)</th>
            </tr>
          </thead>
          <tbody id="historial"></tbody>
        </table>
      </div>
      <div id="historial-vacio" class="vacio">Todavía no hay riegos.</div>
    </section>
  </div>

  <div id="aviso"></div>

  <script>
    const TEXTO_DE_ESTADO_DE_RIEGO = {
      SOLICITADO: 'Solicitado, esperando a CENTRAL...',
      AUTORIZADO: 'Autorizado, abriendo la electroválvula...',
      REGANDO: 'Regando',
      SIN_DATOS: 'Sin datos de la estación (posible avería). Esperando el resumen final...',
    };

    const CARACTERES_HTML = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' };

    /** Evita que un texto recibido se interprete como HTML. */
    function escapar(texto) {
      return String(texto).replace(/[&<>"]/g, caracter => CARACTERES_HTML[caracter]);
    }

    // ------------------------------ Avisos y peticiones ------------------------------
    let temporizadorDelAviso = null;

    function mostrarAviso(texto) {
      const aviso = document.getElementById('aviso');
      aviso.textContent = texto;
      aviso.style.display = 'block';
      clearTimeout(temporizadorDelAviso);
      temporizadorDelAviso = setTimeout(() => { aviso.style.display = 'none'; }, 3500);
    }

    async function enviar(url, cuerpo) {
      try {
        const respuesta = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(cuerpo || {}),
        });
        const resultado = await respuesta.json();
        mostrarAviso(resultado.msg || resultado.error || '');
        actualizar();
      } catch (error) {
        mostrarAviso('No se pudo contactar con la aplicación');
      }
    }

    function pedirRiego(idDeLaWS, segundos) {
      const id = String(idDeLaWS || '').trim();
      if (!id) {
        mostrarAviso('Escribe el ID de la WS');
        return;
      }
      enviar('/api/solicitar', { idWS: id, tiempo: segundos || '' });
    }

    function cancelarLista() {
      enviar('/api/cancelar');
    }

    function cargarFichero() {
      const fichero = document.getElementById('fichero').files[0];
      if (!fichero) {
        mostrarAviso('Elige un fichero primero');
        return;
      }
      const lector = new FileReader();
      lector.onload = () => enviar('/api/servicios', { texto: lector.result });
      lector.readAsText(fichero);
    }

    // ------------------------------ Pintado ------------------------------
    function pintarIndicador(idDelElemento, conectado, textoConectado, textoDesconectado) {
      const elemento = document.getElementById(idDelElemento);
      if (conectado) {
        elemento.className = 'indicador ok';
        elemento.textContent = textoConectado;
      } else {
        elemento.className = 'indicador ko';
        elemento.textContent = textoDesconectado;
      }
    }

    function pintarEstacionesDisponibles(estado) {
      document.getElementById('estaciones').innerHTML = estado.ws.map(estacion => `
        <tr>
          <td><b>${escapar(estacion.idWS)}</b></td>
          <td>${escapar(estacion.ubicacion)}</td>
          <td><button class="principal" onclick="pedirRiego('${escapar(estacion.idWS)}')">Solicitar riego</button></td>
        </tr>`).join('');

      let textoSiNoHay = '';
      if (estado.ws.length === 0) {
        if (estado.central) {
          textoSiNoHay = 'No hay estaciones disponibles ahora mismo.';
        } else {
          textoSiNoHay = 'Sin datos de CENTRAL.';
        }
      }
      document.getElementById('estaciones-vacio').textContent = textoSiNoHay;
    }

    function pintarRiego(riego) {
      let textoDelEstado = TEXTO_DE_ESTADO_DE_RIEGO[riego.estado];
      if (!textoDelEstado) {
        textoDelEstado = escapar(riego.estado);
      }

      let detalle = '';
      if (riego.estado === 'REGANDO') {
        detalle = `<div class="detalle">Caudal: ${riego.caudal} L/min · Volumen: ${riego.volumen} L · Tiempo: ${riego.duracion} s</div>`;
      }

      return `
        <div class="tarjeta ${riego.estado}">
          <b>WS ${escapar(riego.idWS)}</b> · ${textoDelEstado}
          ${detalle}
        </div>`;
    }

    function pintarMisRiegos(estado) {
      document.getElementById('riegos').innerHTML = estado.riegos.map(pintarRiego).join('');

      let visibilidad = 'block';
      if (estado.riegos.length > 0) {
        visibilidad = 'none';
      }
      document.getElementById('riegos-vacio').style.display = visibilidad;
    }

    function pintarServicio(servicio, posicion) {
      let tiempo = '';
      if (servicio.tiempo) {
        tiempo = ' (' + escapar(servicio.tiempo) + ' s)';
      }
      return `<span class="etiqueta ${escapar(servicio.estado)}">${posicion + 1}. WS ${escapar(servicio.idWS)}${tiempo} · ${escapar(servicio.estado)}</span>`;
    }

    function pintarListaDeServicios(estado) {
      const servicios = estado.secuencia.items;
      let html = '';

      if (servicios.length === 0) {
        html = '<div class="vacio">Sin lista cargada. Formatos: JSON [{"idWS":"1","tiempo":30}] o una línea «ID [segundos]».</div>';
      } else {
        html = servicios.map(pintarServicio).join('');
        if (estado.secuencia.activa) {
          html += '<div class="vacio">Lista en curso...</div>';
        }
      }
      document.getElementById('servicios').innerHTML = html;
    }

    function pintarHistorial(estado) {
      document.getElementById('historial').innerHTML = estado.tickets.map(ticket => `
        <tr>
          <td>${escapar(ticket.hora)}</td>
          <td>${escapar(ticket.idWS)}</td>
          <td class="resultado-${escapar(ticket.estado)}"><b>${escapar(ticket.estado)}</b></td>
          <td>${escapar(ticket.motivo)}</td>
          <td>${ticket.volumen}</td>
          <td>${ticket.duracion}</td>
        </tr>`).join('');

      let visibilidad = 'block';
      if (estado.tickets.length > 0) {
        visibilidad = 'none';
      }
      document.getElementById('historial-vacio').style.display = visibilidad;
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
      document.getElementById('operario').textContent = estado.operario;
      document.getElementById('pais').textContent = estado.pais;
      pintarIndicador('indicador-kafka', estado.kafka, 'Kafka: conectado', 'Kafka: sin conexión');
      pintarIndicador('indicador-central', estado.central, 'CENTRAL: en línea', 'CENTRAL: sin respuesta');
      pintarEstacionesDisponibles(estado);
      pintarMisRiegos(estado);
      pintarListaDeServicios(estado);
      pintarHistorial(estado);
      pintarRegistro(estado);
    }

    async function actualizar() {
      try {
        const respuesta = await fetch('/api/state');
        pintar(await respuesta.json());
      } catch (error) {
        // Sin conexión con la aplicación: se mantiene lo que ya se estaba mostrando
      }
    }

    setInterval(actualizar, 1000);
    actualizar();
  </script>
</body>
</html>
"""


def crear_manejador_http(fo):
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

        def responder_json(self, codigo, datos):
            self.responder(codigo, json.dumps(datos).encode("utf-8"), "application/json")

        def ejecutar_sin_romper(self, accion):
            """Un fallo atendiendo una peticion no debe tumbar el servidor del panel."""
            try:
                accion()
            except (BrokenPipeError, ConnectionResetError):
                pass        # el navegador cerro la conexion
            except Exception as error:
                fo.escribir_log(f"Error en el panel web ({error!r})")
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
                self.responder_json(200, fo.estado_para_panel())
            elif self.path in ("/", "/index.html"):
                self.responder(200, pagina_html, "text/html; charset=utf-8")
            else:
                self.responder(404, b"not found", "text/plain")

        def atender_post(self):
            try:
                longitud = int(self.headers.get("Content-Length", 0))
                datos = json.loads(self.rfile.read(longitud) or b"{}")
            except Exception:
                return self.responder_json(400, {"ok": False, "error": "peticion no valida"})

            if self.path == "/api/solicitar":
                ok, texto = fo.solicitar(datos.get("idWS", ""), datos.get("tiempo"))
            elif self.path == "/api/servicios":
                servicios = fo.parsear_servicios(str(datos.get("texto", "")))
                ok, texto = fo.lanzar_servicios(servicios)
            elif self.path == "/api/cancelar":
                ok, texto = fo.cancelar_servicios()
            else:
                return self.responder_json(404, {"ok": False, "error": "no encontrado"})

            self.responder_json(200, {"ok": ok, "msg": texto})

    return ManejadorHttp


def main():
    if len(sys.argv) < 3:
        print("Uso: python WM_FO.py <broker_ip:puerto> <id_operador> [fichero_servicios]")
        sys.exit(1)

    broker = sys.argv[1]
    operario = sys.argv[2]
    fichero_servicios = None
    if len(sys.argv) > 3:
        fichero_servicios = sys.argv[3]

    pais = os.environ.get("PAIS", PAIS_POR_DEFECTO).strip().upper()
    if not es_pais_valido(pais):
        print(f"PAIS no valido: {pais!r}. Opciones: {', '.join(PAISES)}")
        sys.exit(1)

    try:
        puerto_web = int(os.environ.get("PORT", 8081))
        if not 1 <= puerto_web <= 65535:
            raise ValueError
    except ValueError:
        print(f"PORT no valido: {os.environ.get('PORT')!r}")
        sys.exit(1)

    FieldOperator(broker, operario, pais, puerto_web).run(fichero_servicios)


if __name__ == "__main__":
    main()
