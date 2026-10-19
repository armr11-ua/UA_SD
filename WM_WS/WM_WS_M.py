"""WM_WS_M - Monitor de la Watering Station.

Uso: python WM_WS_M.py <puerto_engine> <central_ip> <central_port> <ws_id> [ubicacion]

- Se autentica/registra en WM_Central por sockets.
- Ofrece un servidor de sockets al Engine (WM_WS_E) y le pregunta cada segundo por su salud.
- Si el Engine no responde o responde KO, avisa a CENTRAL (FUGA). Al recuperarse, avisa (RECUPERADO).
"""
import queue
import socket
import os
import sys
import threading
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from protocol import *


class Monitor:
    def __init__(self, engine_port, central_ip, central_port, ws_id, ubicacion):
        self.engine_port = engine_port
        self.central_ip = central_ip
        self.central_port = central_port
        self.ws_id = ws_id
        self.ubicacion = ubicacion
        self.fault = False
        self.out = queue.Queue()          # mensajes pendientes hacia CENTRAL
        self.registered = threading.Event()

    def escribir_log(self, mensaje):
        print(f"[WS_M {self.ws_id}] {mensaje}", flush=True)

    # ------------------ Conexion con CENTRAL ------------------
    def conectar_central(self):
        while True:
            sock = None
            try:
                sock = socket.create_connection((self.central_ip, self.central_port), timeout=5)
                sock.settimeout(5)
                handshake(sock)
                resp = request(sock, f"REGISTER#{self.ws_id}#{self.ubicacion}")
                if not resp.startswith("REGISTER_OK"):
                    self.escribir_log(f"Autenticacion rechazada por CENTRAL: {resp}. Reintentando en 5 s...")
                    sock.close()
                    time.sleep(5)
                    continue
                self.escribir_log(f"Autenticado y registrado en CENTRAL ({self.central_ip}:{self.central_port})")
                self.registered.set()
                if self.fault:                      # re-sincronizar estado tras reconexion
                    request(sock, f"LEAK#{self.ws_id}")
                while True:
                    try:
                        msg = self.out.get(timeout=2)
                    except queue.Empty:
                        msg = f"HB#{self.ws_id}"   # heartbeat
                    request(sock, msg)
            except OSError as e:
                self.escribir_log(f"Conexion con CENTRAL perdida ({e}). Reconectando...")
            finally:
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
            time.sleep(3)

    # ------------------ Salud del Engine ------------------
    def cambiar_averia(self, hay_averia, motivo):
        if hay_averia == self.fault:
            return
        self.fault = hay_averia
        if hay_averia:
            self.escribir_log(f"AVERIA detectada: {motivo}. Notificando a CENTRAL")
            self.out.put(f"LEAK#{self.ws_id}")
        else:
            self.escribir_log("Engine recuperado. Notificando a CENTRAL")
            self.out.put(f"RECOVERED#{self.ws_id}")

    def atender_engine(self, conn):
        try:
            conn.settimeout(5)
            accept_handshake(conn)
            send_message(conn, f"ID#{self.ws_id}")
            self.escribir_log("Engine conectado")
            while True:
                try:
                    conn.settimeout(2)
                    send_message(conn, "HEALTH")
                    reply = receive_message(conn)
                except OSError:
                    self.cambiar_averia(True, "ENGINE no responde")
                    break
                if reply == "OK":
                    self.cambiar_averia(False, "")
                else:
                    self.cambiar_averia(True, "ENGINE reporta KO")
                time.sleep(1)
        except OSError as e:
            self.escribir_log(f"Error con el Engine: {e}")
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.engine_port))
        srv.listen(1)
        threading.Thread(target=self.conectar_central, daemon=True).start()
        self.registered.wait()
        self.escribir_log(f"Esperando al Engine en el puerto {self.engine_port}...")
        while True:
            conn, addr = srv.accept()
            self.atender_engine(conn)
            self.escribir_log("Engine desconectado, esperando nueva conexion...")


def main():
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_M.py <puerto_engine> <central_ip> <central_port> <ws_id> [ubicacion]")
        sys.exit(1)
    engine_port = int(sys.argv[1])
    central_ip = sys.argv[2]
    central_port = int(sys.argv[3])
    ws_id = sys.argv[4]
    ubicacion = sys.argv[5] if len(sys.argv) > 5 else f"Zona-{ws_id}"
    if "#" in ubicacion:
        ubicacion = ubicacion.replace("#", "-")
    try:
        Monitor(engine_port, central_ip, central_port, ws_id, ubicacion).run()
    except KeyboardInterrupt:
        print("\nMonitor cerrado por el usuario")


if __name__ == "__main__":
    main()