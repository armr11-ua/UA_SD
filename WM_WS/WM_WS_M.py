"""WM_WS_M - Monitor de la Watering Station.

Uso: python WM_WS_M.py <puerto_engine> <central_ip> <central_port> <ws_id> [ubicacion]
     (el pais de la WS se indica con la variable de entorno PAIS; por defecto el primero de topics.PAISES)

- Habla SOLO por sockets (protocolos.py); no usa Kafka, como marca la especificacion.
- Se autentica/registra en WM_Central por sockets (enviando su ID, ubicacion y pais).
- Ofrece un servidor de sockets al Engine (WM_WS_E) y le pregunta cada segundo por su salud.
  En esa misma pregunta le indica si WM_Central esta alcanzable (HEALTH#CENTRAL_OK/KO).
- Si el Engine no esta conectado, no responde o responde KO, avisa a CENTRAL (LEAK).
  Al recuperarse, avisa (RECOVERED).
"""
import os
import queue
import socket
import sys
import threading
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from protocol import (
    CENTRAL_KO, CENTRAL_OK, HB, HEALTH, ID, LEAK, OK, RECOVERED, REGISTER, REGISTER_OK, SEP,
    accept_handshake, end_session, handshake, mensaje, receive_message, request, send_message,
)
from topics import PAIS_POR_DEFECTO, PAISES, es_pais_valido    # solo la lista de paises: el Monitor no usa Kafka

# ------------------------- Tiempos (segundos) -------------------------
TIMEOUT_CONEXION_CENTRAL_S = 5          # al conectar y al hablar con Central
ESPERA_TRAS_REGISTRO_RECHAZADO_S = 5    # antes de reintentar un registro rechazado
ESPERA_TRAS_PERDER_CENTRAL_S = 3        # antes de reintentar la conexion con Central
ESPERA_TRAS_ERROR_ACCEPT_S = 1          # antes de volver a aceptar tras un error en accept()
PERIODO_LATIDO_S = 2                    # sin nada que contar a Central, se manda un latido
TIMEOUT_HANDSHAKE_ENGINE_S = 5          # al aceptar la conexion del Engine
TIMEOUT_RESPUESTA_ENGINE_S = 2          # espera de la respuesta de salud del Engine
PERIODO_COMPROBACION_SALUD_S = 1        # cada cuanto se pregunta por la salud del Engine


class Monitor:
    def __init__(self, puerto_engine, central_ip, central_puerto, ws_id, ubicacion, pais):
        self.puerto_engine = puerto_engine
        self.central_ip = central_ip
        self.central_puerto = central_puerto
        self.ws_id = ws_id
        self.ubicacion = ubicacion
        self.pais = pais

        # Averiada hasta que el Engine conecte y responda OK
        self.hay_averia = True

        # Mensajes (LEAK / RECOVERED) pendientes de enviar a Central
        self.mensajes_para_central = queue.Queue()

        self.registrado_en_central = threading.Event()   # se activa la primera vez que Central nos acepta
        self.central_alcanzable = threading.Event()      # hay sesion activa con Central ahora mismo

    def escribir_log(self, texto):
        print(f"[WS_M {self.ws_id}] {texto}", flush=True)

    # ------------------ Conexion con CENTRAL ------------------
    def mantener_conexion_con_central(self):
        """Hilo: conecta con Central, se registra y le envia mensajes/latidos. Si la conexion
        se pierde, reintenta indefinidamente."""
        while True:
            sock = None
            try:
                sock = socket.create_connection((self.central_ip, self.central_puerto),
                                                timeout=TIMEOUT_CONEXION_CENTRAL_S)
                sock.settimeout(TIMEOUT_CONEXION_CENTRAL_S)
                handshake(sock)

                if not self.registrarse_en_central(sock):
                    end_session(sock)
                    time.sleep(ESPERA_TRAS_REGISTRO_RECHAZADO_S)
                    continue

                self.enviar_mensajes_a_central(sock)

            except OSError as error:
                self.central_alcanzable.clear()
                self.escribir_log(f"Imposible conectar con WM_Central ({error}). Reconectando...")
            except Exception as error:
                # Fallo inesperado (por ejemplo un mensaje mal formado): no debe matar el hilo
                self.central_alcanzable.clear()
                self.escribir_log(f"Error inesperado con WM_Central ({error!r}). Reconectando...")
            finally:
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
            time.sleep(ESPERA_TRAS_PERDER_CENTRAL_S)

    def registrarse_en_central(self, sock):
        """Envia REGISTER. Devuelve True si Central acepta (y deja sincronizado el estado)."""
        respuesta = request(sock, mensaje(REGISTER, self.ws_id, self.ubicacion, self.pais))

        if not respuesta.startswith(REGISTER_OK):
            self.central_alcanzable.clear()
            self.escribir_log(f"Autenticacion rechazada por CENTRAL: {respuesta}. Reintentando en 5 s...")
            return False

        self.escribir_log(f"Autenticado y registrado en CENTRAL ({self.central_ip}:{self.central_puerto})")

        # Lo pendiente de antes de la caida ya no vale: se re-sincroniza el estado actual
        with self.mensajes_para_central.mutex:
            self.mensajes_para_central.queue.clear()
        if self.hay_averia:
            request(sock, mensaje(LEAK, self.ws_id))

        self.central_alcanzable.set()
        self.registrado_en_central.set()
        return True

    def enviar_mensajes_a_central(self, sock):
        """Bucle de la sesion con Central: envia lo pendiente o, si no hay nada, un latido."""
        while True:
            try:
                texto = self.mensajes_para_central.get(timeout=PERIODO_LATIDO_S)
            except queue.Empty:
                texto = mensaje(HB, self.ws_id)
            request(sock, texto)

    # ------------------ Salud del Engine ------------------
    def cambiar_averia(self, hay_averia, motivo):
        """Actualiza el estado de averia y, solo si cambia, avisa a Central."""
        if hay_averia == self.hay_averia:
            return
        self.hay_averia = hay_averia

        if hay_averia:
            self.escribir_log(f"AVERIA detectada: {motivo}. Notificando a CENTRAL")
            self.mensajes_para_central.put(mensaje(LEAK, self.ws_id))
        else:
            self.escribir_log("Engine recuperado. Notificando a CENTRAL")
            self.mensajes_para_central.put(mensaje(RECOVERED, self.ws_id))

    def estado_de_central_para_el_engine(self):
        if self.central_alcanzable.is_set():
            return CENTRAL_OK
        return CENTRAL_KO

    def preguntar_salud_al_engine(self, conexion):
        """Envia HEALTH y devuelve la respuesta del Engine (OK / KO). Lanza OSError si no responde."""
        conexion.settimeout(TIMEOUT_RESPUESTA_ENGINE_S)
        send_message(conexion, mensaje(HEALTH, self.estado_de_central_para_el_engine()))
        return receive_message(conexion)

    def atender_engine(self, conexion):
        """Atiende a un Engine conectado hasta que se desconecte o deje de responder."""
        try:
            conexion.settimeout(TIMEOUT_HANDSHAKE_ENGINE_S)
            accept_handshake(conexion)
            send_message(conexion, mensaje(ID, self.ws_id, self.pais))
            self.escribir_log("Engine conectado")

            while True:
                try:
                    respuesta = self.preguntar_salud_al_engine(conexion)
                except OSError:
                    self.cambiar_averia(True, "ENGINE no responde")
                    break

                if respuesta == OK:
                    self.cambiar_averia(False, "")
                else:
                    self.cambiar_averia(True, "ENGINE reporta KO")
                time.sleep(PERIODO_COMPROBACION_SALUD_S)

        except OSError as error:
            self.escribir_log(f"Error con el Engine: {error}")
        except Exception as error:
            self.escribir_log(f"Error inesperado con el Engine: {error!r}")
        finally:
            self.cambiar_averia(True, "ENGINE desconectado")
            end_session(conexion)
            try:
                conexion.close()
            except OSError:
                pass

    # ------------------ Arranque ------------------
    def crear_servidor_para_el_engine(self):
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind(("0.0.0.0", self.puerto_engine))
        servidor.listen(1)
        return servidor

    def run(self):
        try:
            servidor = self.crear_servidor_para_el_engine()
        except OSError as error:
            self.escribir_log(f"No se pudo abrir el puerto {self.puerto_engine} para el Engine: {error}")
            sys.exit(1)
        threading.Thread(target=self.mantener_conexion_con_central, daemon=True).start()

        self.registrado_en_central.wait()       # no se acepta al Engine hasta estar autenticados
        self.escribir_log(f"Esperando al Engine en el puerto {self.puerto_engine}...")
        while True:
            try:
                conexion, direccion = servidor.accept()
            except OSError as error:
                self.escribir_log(f"Error al aceptar la conexion del Engine ({error})")
                time.sleep(ESPERA_TRAS_ERROR_ACCEPT_S)
                continue

            self.atender_engine(conexion)
            self.escribir_log("Engine desconectado, esperando nueva conexion...")


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
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_M.py <puerto_engine> <central_ip> <central_port> <ws_id> [ubicacion]")
        sys.exit(1)

    puerto_engine = leer_puerto(sys.argv[1], "puerto_engine")
    central_ip = sys.argv[2]
    central_puerto = leer_puerto(sys.argv[3], "central_port")
    ws_id = sys.argv[4]

    if len(sys.argv) > 5:
        ubicacion = sys.argv[5]
    else:
        ubicacion = f"Zona-{ws_id}"
    ubicacion = ubicacion.replace(SEP, "-")      # el separador de campos no puede ir dentro de un campo

    pais = os.environ.get("PAIS", PAIS_POR_DEFECTO).strip().upper()
    if not es_pais_valido(pais):
        print(f"PAIS no valido: {pais!r}. Validos: {', '.join(PAISES)}")
        sys.exit(1)

    try:
        Monitor(puerto_engine, central_ip, central_puerto, ws_id, ubicacion, pais).run()
    except KeyboardInterrupt:
        print("\nMonitor cerrado por el usuario")


if __name__ == "__main__":
    main()
