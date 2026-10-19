"""WM_WS_E - Engine de la Watering Station (caudalimetro + electrovalvula simulados).

Uso: python WM_WS_E.py <broker_ip:puerto> <monitor_ip:puerto>

- Se conecta al Monitor por sockets; el Monitor le asigna el ID (ya autenticado en CENTRAL)
  y le pregunta cada segundo por su salud (responde OK / KO).
- Recibe ordenes de CENTRAL por Kafka (START / BLOCK / ACTIVATE) y publica estado,
  consumo (cada segundo) y ticket final.
- Menu por consola: 1 = simular fuga (KO) / reparar, 2 = riego manual,
  3 = finalizar riego, 4 = estado, q = salir.
"""
import json
import random
import os
import sys
import threading
import time

import socket

from confluent_kafka import KafkaError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from protocol import *


class Motor:
    def __init__(self, broker, mon_ip, mon_port):
        self.broker = broker
        self.mon_ip = mon_ip
        self.mon_port = mon_port
        self.ws_id = None
        self.id_event = threading.Event()
        self.lock = threading.RLock()
        self.estado = DISPONIBLE          # DISPONIBLE | REGANDO | FUERA_DE_SERVICIO
        self.ko = False                   # fuga simulada
        self.sesion = None                  # sesion de riego en curso
        self.producer = None

    def escribir_log(self, mensaje):
        print(f"[WS_E {self.ws_id or '?'}] {mensaje}", flush=True)

    # ------------------ Monitor (sockets) ------------------
    def escuchar_monitor(self):
        while True:
            sock = None
            try:
                sock = socket.create_connection((self.mon_ip, self.mon_port), timeout=5)
                sock.settimeout(None)               # el Monitor puede tardar en autenticarse
                handshake(sock)
                msg = receive_message(sock)
                if msg.startswith("ID#"):
                    self.ws_id = msg.split("#")[1]
                    self.id_event.set()
                    self.escribir_log("Conectado al Monitor, ID asignado")
                sock.settimeout(10)
                while True:
                    msg = receive_message(sock)
                    if msg == "HEALTH":
                        send_message(sock, "KO" if self.ko else "OK")
            except OSError as e:
                self.escribir_log(f"Conexion con el Monitor perdida ({e}). Reintentando...")
            finally:
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
            time.sleep(3)

    # ------------------ Kafka ------------------
    def iniciar_kafka(self):
        self.ready = threading.Event()
        while True:
            try:
                ensure_topics(self.broker)
                self.producer = JsonProducer(self.broker)
                self.consumer = make_consumer(self.broker, [WS_CONTROL],
                                              f"wm_ws_{self.ws_id}", self.ready)
                return
            except Exception as e:
                self.escribir_log(f"Broker Kafka no disponible ({e}), reintentando en 3 s...")
                time.sleep(3)

    def enviar_estado(self):
        with self.lock:
            msg = {"idWS": self.ws_id, "estado": self.estado, "averia": self.ko,
                   "operario": self.sesion["operario"] if self.sesion else ""}
        self.producer.send(WS_STATUS, msg)

    def mantener_estado(self):
        while True:
            self.enviar_estado()
            time.sleep(5)

    def escuchar_ordenes(self):
        while True:
            m = self.consumer.poll(1.0)
            if m is None:
                continue
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    self.escribir_log(f"Kafka: {m.error()}")
                continue
            d = kafka_loads(m)
            if not isinstance(d, dict) or str(d.get("idWS")) != str(self.ws_id):
                continue
            cmd = d.get("cmd")
            if cmd == "START":
                self.iniciar_riego(
                    str(d.get("idOperator", "?")), float(d.get("limite", 20)))
            elif cmd == "BLOCK":
                self.bloquear()
            elif cmd == "ACTIVATE":
                self.activar()

    # ------------------ Logica de riego ------------------
    def confirmar_orden(self, operario, aceptada, motivo=""):
        self.producer.send(
            WS_ACK,
            {"idWS": self.ws_id, "idOperator": operario, "ok": aceptada, "motivo": motivo})
        self.producer.flush()

    def iniciar_riego(self, operario, limite):
        motivo = ""
        with self.lock:
            if self.ko:
                motivo = "Fuga detectada en la estacion"
            elif self.estado == FUERA_DE_SERVICIO:
                motivo = "Estacion fuera de servicio"
            elif self.estado == REGANDO:
                motivo = "Estacion ya esta regando"
            else:
                self.estado = REGANDO
                sesion = {"operario": operario, "limite": limite, "inicio": time.time(),
                        "volumen": 0.0, "stop": threading.Event()}
                self.sesion = sesion
        if motivo:
            self.escribir_log(f"Riego DENEGADO para {operario}: {motivo}")
            self.confirmar_orden(operario, False, motivo)
            return
        self.escribir_log(
            f"Autorizado por CENTRAL. Abriendo electrovalvula para {operario} "
            f"(limite {limite:.0f} s)")
        self.confirmar_orden(operario, True)
        self.enviar_estado()
        threading.Thread(target=self.regar, args=(sesion,), daemon=True).start()

    def regar(self, sesion):
        while not sesion["stop"].wait(1.0):
            with self.lock:
                if self.sesion is not sesion:
                    return
                caudal = round(random.uniform(8.0, 12.0), 1)           # L/min
                sesion["volumen"] += caudal / 60.0
                duracion = time.time() - sesion["inicio"]
                mensaje = {"idWS": self.ws_id, "idOperator": sesion["operario"], "caudal": caudal,
                           "volumen": round(sesion["volumen"], 2),
                           "duracion": round(duracion, 1)}
            self.producer.send(WS_CONSUMPTION, mensaje)
            self.escribir_log(
                f"Regando: {caudal} L/min | {mensaje['volumen']} L | "
                f"{mensaje['duracion']} s")
            if duracion >= sesion["limite"]:
                self.terminar_riego("COMPLETADO", "LIMITE_TIEMPO", sesion=sesion)
                return

    def terminar_riego(self, estado, motivo, siguiente_estado=None, sesion=None):
        with self.lock:
            riego = self.sesion
            if riego is None or (sesion is not None and riego is not sesion):
                return False
            self.sesion = None
            riego["stop"].set()
            self.estado = siguiente_estado or DISPONIBLE
            ticket = {"idWS": self.ws_id, "idOperator": riego["operario"], "estado": estado,
                      "motivo": motivo, "volumen": round(riego["volumen"], 2),
                      "duracion": round(time.time() - riego["inicio"], 1)}
        self.producer.send(WS_SUPPLY_COMPLETE, {"ticket": ticket})
        self.producer.flush()
        self.escribir_log(f"Riego finalizado ({estado}, {motivo}): {ticket['volumen']} L en {ticket['duracion']} s")
        self.enviar_estado()
        return True

    def bloquear(self):
        self.terminar_riego(
            "INTERRUMPIDO", "BLOQUEO_CENTRAL", siguiente_estado=FUERA_DE_SERVICIO)
        with self.lock:
            self.estado = FUERA_DE_SERVICIO
        self.escribir_log("FUERA DE SERVICIO por orden de CENTRAL")
        self.enviar_estado()

    def activar(self):
        with self.lock:
            if self.estado == FUERA_DE_SERVICIO:
                self.estado = DISPONIBLE
        self.escribir_log("Estacion ACTIVADA por CENTRAL")
        self.enviar_estado()

    def cambiar_averia(self):
        with self.lock:
            self.ko = not self.ko
            ko = self.ko
        if ko:
            self.escribir_log("*** FUGA simulada: reportando KO al Monitor ***")
            self.terminar_riego("INTERRUMPIDO", "FUGA")
        else:
            self.escribir_log("Fuga reparada: reportando OK al Monitor")
        self.enviar_estado()

    # ------------------ Menu ------------------
    def show(self):
        with self.lock:
            e = "FUGA" if self.ko else self.estado
            extra = ""
            if self.sesion:
                extra = (
                    f" | operario {self.sesion['operario']} | "
                    f"{self.sesion['volumen']:.2f} L")
        self.escribir_log(f"Estado: {e}{extra}")

    def menu(self):
        print("\n1) Simular fuga / reparar   2) Riego manual   3) Finalizar riego   4) Estado   q) Salir")
        while True:
            try:
                opcion = input().strip().lower()
            except (EOFError, KeyboardInterrupt):
                break
            if opcion == "1":
                self.cambiar_averia()
            elif opcion == "2":
                self.producer.send(FO_REQUEST, {"idOperator": "LOCAL", "idWS": self.ws_id})
                self.producer.flush()
                self.escribir_log("Riego manual solicitado a CENTRAL")
            elif opcion == "3":
                if not self.terminar_riego("COMPLETADO", "FIN_MANUAL"):
                    self.escribir_log("No hay riego en curso")
            elif opcion == "4":
                self.show()
            elif opcion == "q":
                break

    def run(self):
        threading.Thread(target=self.escuchar_monitor, daemon=True).start()
        self.escribir_log("Esperando asignacion de ID por el Monitor...")
        self.id_event.wait()
        self.iniciar_kafka()
        threading.Thread(target=self.escuchar_ordenes, daemon=True).start()
        self.ready.wait(30)          # esperar a tener las particiones asignadas
        threading.Thread(target=self.mantener_estado, daemon=True).start()
        self.menu()
        try:
            self.terminar_riego("INTERRUMPIDO", "ENGINE_CERRADO")
        except Exception:
            pass


def main():
    if len(sys.argv) < 3:
        print("Uso: python WM_WS_E.py <broker_ip:puerto> <monitor_ip:puerto>")
        sys.exit(1)
    broker = sys.argv[1]
    mon_ip, mon_port = sys.argv[2].rsplit(":", 1)
    Motor(broker, mon_ip, int(mon_port)).run()


if __name__ == "__main__":
    main()