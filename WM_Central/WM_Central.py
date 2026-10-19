"""WM_Central - Sistema central de WaterManagement.

Uso: python WM_Central.py <puerto_sockets> <broker_ip:puerto> [fichero_bd]

- Servidor de sockets para los Monitores (registro/autenticacion, heartbeat, fugas).
- Kafka (confluent_kafka): peticiones de operarios, estado/consumo/tickets de las WS, ordenes a las WS.
- BD SQLite: WS, operarios e historico de riegos.
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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import KafkaError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from protocol import *

DEFAULT_LIMIT_S = 20      # limite de tiempo de un riego
HB_TIMEOUT = 10           # s sin heartbeat => WS desconectada
PENDING_TIMEOUT = 5       # s esperando respuesta de la WS a un START
ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,10}$")


class Central:
    def __init__(self, port, broker, db_path, web_port):
        self.port = port
        self.broker = broker
        self.web_port = web_port
        self.lock = threading.RLock()
        self.db_lock = threading.Lock()
        self.logs = deque(maxlen=300)
        self.ws = {}
        self.iniciar_bd(db_path)

    # ------------------ Logs ------------------
    def escribir_log(self, mensaje):
        line = f"[{time.strftime('%H:%M:%S')}] {mensaje}"
        with self.lock:
            self.logs.append(line)
        print(line, flush=True)

    # ------------------ Base de datos ------------------
    def iniciar_bd(self, ruta):
        self.db = sqlite3.connect(ruta, check_same_thread=False)
        with self.db_lock:
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS WS (
                    idWS VARCHAR(10) PRIMARY KEY,
                    estado TEXT NOT NULL CHECK (estado IN
                        ('DISPONIBLE','REGANDO','FUGA','FUERA_DE_SERVICIO','DESCONECTADA')),
                    ubicacion VARCHAR(100) NOT NULL
                );
                CREATE TABLE IF NOT EXISTS OPERARIO (
                    idOperario VARCHAR(20) PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS RIEGO (
                    idRiego INTEGER PRIMARY KEY AUTOINCREMENT,
                    operario VARCHAR(20) NOT NULL,
                    ws VARCHAR(10) NOT NULL,
                    estado TEXT NOT NULL CHECK (estado IN ('COMPLETADO','INTERRUMPIDO')),
                    motivo TEXT,
                    volumen DECIMAL(10,2) NOT NULL,
                    duracion DECIMAL(10,1) NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ws) REFERENCES WS(idWS)
                );
            """)
            # Al (re)iniciar, todas las WS conocidas pasan a DESCONECTADA
            self.db.execute("UPDATE WS SET estado='DESCONECTADA'")
            self.db.commit()
            rows = self.db.execute("SELECT idWS, ubicacion FROM WS ORDER BY idWS").fetchall()
        for ws_id, ubic in rows:
            self.ws[ws_id] = self.crear_ws(ws_id, ubic)
        self.escribir_log(
            f"BD cargada: {len(rows)} WS registradas (todas DESCONECTADA hasta que conecten)")

    def ejecutar_bd(self, sql, params=()):
        with self.db_lock:
            self.db.execute(sql, params)
            self.db.commit()

    @staticmethod
    def crear_ws(ws_id, ubicacion):
        return {"id": ws_id, "ubicacion": ubicacion, "conn": None, "connected": False,
                "mon_leak": False, "eng_estado": DISPONIBLE, "eng_ko": False,
                "caudal": 0.0, "volumen": 0.0, "operario": "",
                "pending": False, "timer": None, "last_db": DESCONECTADA}

    @staticmethod
    def estado_real(ws):
        if not ws["connected"]:
            return DESCONECTADA
        if ws["mon_leak"] or ws["eng_ko"]:
            return FUGA
        return ws["eng_estado"]

    def guardar_estado_bd(self, w):
        est = self.estado_real(w)
        if est != w["last_db"]:
            w["last_db"] = est
            self.ejecutar_bd("UPDATE WS SET estado=? WHERE idWS=?", (est, w["id"]))

    # ------------------ Servidor de sockets (Monitores) ------------------
    def servidor_sockets(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.port))
        srv.listen(20)
        self.escribir_log(f"Servidor de sockets escuchando en el puerto {self.port}")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.atender_monitor, args=(conn, addr), daemon=True).start()

    def atender_monitor(self, conn, addr):
        ws_id = None
        try:
            conn.settimeout(HB_TIMEOUT)
            while True:
                parts = receive_message(conn).split("#")
                cmd = parts[0]
                if cmd == "REGISTER" and len(parts) >= 3:
                    ok, reason = self.registrar_ws(parts[1], parts[2], conn)
                    if ok:
                        ws_id = parts[1]
                        ans = "REGISTER_OK"
                    else:
                        ans = f"REGISTER_KO#{reason}"
                elif ws_id is None:
                    ans = "KO#NO_AUTENTICADO"
                elif cmd == "HB":
                    ans = "OK"
                elif cmd == "LEAK":
                    self.cambiar_averia_monitor(ws_id, True)
                    ans = "OK"
                elif cmd == "RECOVERED":
                    self.cambiar_averia_monitor(ws_id, False)
                    ans = "OK"
                else:
                    ans = "KO#DESCONOCIDO"
                send_message(conn, ans)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass
            if ws_id:
                self.marcar_desconectada(ws_id, conn)

    def registrar_ws(self, ws_id, ubicacion, conn):
        if not ID_RE.match(ws_id):
            self.escribir_log(f"Registro rechazado: ID invalido '{ws_id}'")
            return False, "ID_INVALIDO"
        with self.lock:
            w = self.ws.get(ws_id)
            if w and w["connected"] and w["conn"] is not conn:
                self.escribir_log(f"Registro rechazado: WS {ws_id} ya conectada")
                return False, "YA_CONECTADA"
            if not w:
                w = self.crear_ws(ws_id, ubicacion or f"Zona-{ws_id}")
                self.ws[ws_id] = w
                self.ejecutar_bd("INSERT OR IGNORE INTO WS(idWS, estado, ubicacion) VALUES (?,?,?)",
                             (ws_id, DESCONECTADA, w["ubicacion"]))
                self.escribir_log(f"Nueva WS {ws_id} dada de alta en '{w['ubicacion']}'")
            w.update(conn=conn, connected=True, mon_leak=False, eng_ko=False,
                     eng_estado=DISPONIBLE, caudal=0.0, volumen=0.0, operario="", pending=False)
            self.guardar_estado_bd(w)
        self.escribir_log(f"WS {ws_id} autenticada y conectada ({w['ubicacion']})")
        return True, ""

    def marcar_desconectada(self, ws_id, conn):
        with self.lock:
            w = self.ws.get(ws_id)
            if not w or w["conn"] is not conn:
                return
            w.update(conn=None, connected=False, mon_leak=False, pending=False,
                     caudal=0.0)
            self.guardar_estado_bd(w)
        self.escribir_log(f"WS {ws_id} DESCONECTADA (monitor sin conexion)")

    def cambiar_averia_monitor(self, ws_id, hay_averia):
        with self.lock:
            w = self.ws.get(ws_id)
            if not w:
                return
            w["mon_leak"] = hay_averia
            self.guardar_estado_bd(w)
        self.escribir_log(f"WS {ws_id}: " + ("FUGA/averia reportada por el Monitor" if hay_averia
                                    else "averia resuelta, vuelve a estar disponible"))

    # ------------------ Kafka ------------------

    def iniciar_kafka(self):
        while True:
            try:
                ensure_topics(self.broker)
                self.producer = JsonProducer(self.broker)
                self.ready = threading.Event()
                self.consumer = make_consumer(
                    self.broker,
                    [FO_REQUEST, WS_STATUS, WS_CONSUMPTION, WS_ACK, WS_SUPPLY_COMPLETE],
                    "wm_central", self.ready)
                self.escribir_log(f"Conectado a Kafka ({self.broker})")

                # Una vez conectados con éxito a Kafka, arrancamos los bucles que consumen/producen
                threading.Thread(target=self.escuchar_kafka, daemon=True).start()
                threading.Thread(target=self.enviar_lista_ws, daemon=True).start()
                return
            except Exception as e:
                self.escribir_log(f"Broker Kafka no disponible ({e}), reintentando en 3 s...")
                time.sleep(3)
        

    def escuchar_kafka(self):
        while True:
            m = self.consumer.poll(1.0)
            if m is None:
                continue
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    self.escribir_log(f"Kafka: {m.error()}")
                continue
            d = kafka_loads(m)
            if not isinstance(d, dict):
                continue
            topic = m.topic()
            try:
                if topic == FO_REQUEST:
                    self.gestionar_peticion_riego(
                        str(d.get("idOperator", "?")), str(d.get("idWS", "")))
                elif topic == WS_STATUS:
                    self.recibir_estado(d)
                elif topic == WS_CONSUMPTION:
                    self.recibir_consumo(d)
                elif topic == WS_ACK:
                    self.recibir_confirmacion(d)
                elif topic == WS_SUPPLY_COMPLETE:
                    self.guardar_ticket(d.get("ticket", {}))
            except Exception as e:
                self.escribir_log(f"Error procesando mensaje de {topic}: {e}")

    def enviar_lista_ws(self):
        while True:
            with self.lock:
                lst = [{"idWS": w["id"], "ubicacion": w["ubicacion"]}
                       for w in self.ws.values() if self.estado_real(w) == DISPONIBLE]
            self.producer.send(WS_LIST, lst)
            time.sleep(2)

    # ------------------ Logica de riego ------------------
    def avisar_operario(self, operario, ws_id, texto):
        self.producer.send(FO_NOTIFY, {"idOperator": operario, "idWS": ws_id, "msg": texto})
        self.escribir_log(f"[-> operario {operario}] {texto}")

    def denegar_riego(self, operario, ws_id, motivo):
        self.producer.send(FO_AUTH, {"idOperator": operario, "idWS": ws_id,
                                     "authorize": "NO", "motivo": motivo})
        self.producer.flush()
        self.escribir_log(f"Riego DENEGADO a {operario} en WS {ws_id}: {motivo}")

    def gestionar_peticion_riego(self, operario, estacion, limite=DEFAULT_LIMIT_S):
        self.escribir_log(f"Peticion de riego de {operario} para WS {estacion}")
        self.ejecutar_bd("INSERT OR IGNORE INTO OPERARIO(idOperario) VALUES (?)", (operario,))
        self.avisar_operario(
            operario, estacion, "Peticion recibida por CENTRAL. Comprobando disponibilidad...")
        with self.lock:
            w = self.ws.get(estacion)
            if not w:
                motivo = "La WS no existe"
            elif w["pending"]:
                motivo = "La WS tiene otra solicitud en curso"
            elif self.estado_real(w) != DISPONIBLE:
                motivo = f"WS no disponible (estado: {self.estado_real(w)})"
            else:
                motivo = ""
                w["pending"] = True
                w["operario"] = operario
                w["timer"] = threading.Timer(
                    PENDING_TIMEOUT, self.terminar_solicitud, args=(estacion, operario))
                w["timer"].daemon = True
                w["timer"].start()
        if motivo:
            self.denegar_riego(operario, estacion, motivo)
            return
        self.avisar_operario(
            operario, estacion, "WS disponible. Solicitando autorizacion a la estacion...")
        self.producer.send(WS_CONTROL, {"cmd": "START", "idWS": estacion,
                                        "idOperator": operario, "limite": limite})
        self.producer.flush()

    def terminar_solicitud(self, ws_id, operario):
        with self.lock:
            w = self.ws.get(ws_id)
            if not w or not w["pending"]:
                return
            w["pending"] = False
            w["operario"] = ""
        self.denegar_riego(operario, ws_id, "La WS no responde")

    def recibir_confirmacion(self, datos):
        ws_id, operario = str(datos.get("idWS")), str(datos.get("idOperator"))
        with self.lock:
            w = self.ws.get(ws_id)
            if not w or not w["pending"]:
                return
            w["pending"] = False
            if w["timer"]:
                w["timer"].cancel()
            if not datos.get("ok"):
                w["operario"] = ""
        if datos.get("ok"):
            self.producer.send(FO_AUTH, {"idOperator": operario, "idWS": ws_id,
                                         "authorize": "YES", "motivo": ""})
            self.producer.flush()
            self.avisar_operario(operario, ws_id, "Riego AUTORIZADO. Abriendo electrovalvula")
        else:
            self.denegar_riego(
                operario, ws_id, datos.get("motivo", "Denegado por la estacion"))

    def recibir_estado(self, datos):
        ws_id = str(datos.get("idWS"))
        with self.lock:
            w = self.ws.get(ws_id)
            if not w or not w["connected"]:
                return
            prev = self.estado_real(w)
            if datos.get("estado") in (DISPONIBLE, REGANDO, FUERA_DE_SERVICIO):
                w["eng_estado"] = datos["estado"]
            w["eng_ko"] = bool(datos.get("averia"))
            if w["eng_estado"] != REGANDO:
                w["caudal"] = 0.0
                if not w["pending"]:
                    w["operario"] = ""
            else:
                w["operario"] = str(datos.get("operario", w["operario"]))
            self.guardar_estado_bd(w)
            new = self.estado_real(w)
        if new != prev:
            self.escribir_log(f"WS {ws_id}: {prev} -> {new}")

    def recibir_consumo(self, datos):
        ws_id = str(datos.get("idWS"))
        with self.lock:
            w = self.ws.get(ws_id)
            if not w:
                return
            w["eng_estado"] = REGANDO
            w["caudal"] = float(datos.get("caudal", 0))
            w["volumen"] = float(datos.get("volumen", 0))
            w["operario"] = str(datos.get("idOperator", ""))
            self.guardar_estado_bd(w)

    def guardar_ticket(self, ticket):
        ws_id, operario = str(ticket.get("idWS")), str(ticket.get("idOperator"))
        with self.lock:
            w = self.ws.get(ws_id)
            if w:
                w["caudal"] = 0.0
                w["volumen"] = 0.0
                w["operario"] = ""
        self.ejecutar_bd("INSERT OR IGNORE INTO OPERARIO(idOperario) VALUES (?)", (operario,))
        self.ejecutar_bd("INSERT INTO RIEGO(operario, ws, estado, motivo, volumen, duracion) "
                     "VALUES (?,?,?,?,?,?)",
                     (operario, ws_id, ticket.get("estado", "COMPLETADO"), ticket.get("motivo", ""),
                      ticket.get("volumen", 0), ticket.get("duracion", 0)))
        self.producer.send(FO_TICKET, {"ticket": ticket})
        self.producer.flush()
        self.escribir_log(f"Riego {ticket.get('estado')} en WS {ws_id} (operario {operario}): "
                 f"{ticket.get('volumen')} L, {ticket.get('duracion')} s, "
                 f"motivo {ticket.get('motivo')}")

    # ------------------ Ordenes manuales de CENTRAL ------------------
    def ejecutar_orden(self, comando, objetivo):
        with self.lock:
            ids = list(self.ws) if objetivo == "ALL" else [objetivo]
            ids = [i for i in ids if i in self.ws]
        for ws_id in ids:
            if comando == "start":
                self.gestionar_peticion_riego("CENTRAL", ws_id)
            elif comando == "block":
                self.producer.send(WS_CONTROL, {"cmd": "BLOCK", "idWS": ws_id})
                self.escribir_log(f"Orden BLOQUEAR enviada a WS {ws_id}")
            elif comando == "activate":
                self.producer.send(WS_CONTROL, {"cmd": "ACTIVATE", "idWS": ws_id})
                self.escribir_log(f"Orden ACTIVAR enviada a WS {ws_id}")
        self.producer.flush()

    def obtener_estado(self):
        with self.lock:
            ws = [{"id": w["id"], "ubicacion": w["ubicacion"], "estado": self.estado_real(w),
                   "caudal": round(w["caudal"], 1), "volumen": round(w["volumen"], 2),
                   "operario": w["operario"]} for w in sorted(self.ws.values(), key=lambda x: x["id"])]
            logs = list(self.logs)[-120:]
        return {"ws": ws, "logs": logs}

    # ------------------ Arranque ------------------
    def run(self):
        # 1. Arrancar el servidor de sockets de inmediato (para que los Monitores puedan conectar SIEMPRE)
        threading.Thread(target=self.servidor_sockets, daemon=True).start()

        # 2. Intentar conectar con Kafka en segundo plano
        threading.Thread(target=self.iniciar_kafka, daemon=True).start()

        # 3. Arrancar servidor Web del panel
        handler = make_handler(self)
        httpd = ThreadingHTTPServer(("0.0.0.0", self.web_port), handler)
        self.escribir_log(f"Panel de monitorizacion en http://0.0.0.0:{self.web_port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            self.escribir_log("CENTRAL detenida por el usuario")

# ------------------ Panel web ------------------
HTML = r"""<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WM_Central</title>
<style>
:root{--bg:#0f1a1f;--panel:#16252c;--fg:#e6f0f2;--mut:#8aa0a8;--line:#27404a}
*{box-sizing:border-box}
body{margin:0;padding:16px;background:var(--bg);color:var(--fg);font-family:system-ui,sans-serif}
h1{font-size:18px;margin:0 0 12px}h1 span{color:var(--mut);font-weight:400}
.bar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;align-items:center}
button{background:var(--panel);color:var(--fg);border:1px solid var(--line);padding:8px 12px;border-radius:6px;cursor:pointer}
button:hover{border-color:#4a7a8a}
label{color:var(--mut);font-size:13px}
.sum{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
.chip{padding:4px 10px;border-radius:999px;font-size:12px;border:1px solid var(--line);background:var(--panel)}
.wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;background:var(--panel);font-size:14px}
th,td{padding:8px 10px;text-align:left;border-bottom:1px solid var(--line)}
th{color:var(--mut);font-weight:500}
tr{cursor:pointer}tr.sel td{outline:2px solid #6fb3c8;outline-offset:-2px}
.DISPONIBLE td{background:#1f5c2e}
.REGANDO td{animation:blink 1s infinite}
.FUGA td{background:#8b1e1e}
.FUERA_DE_SERVICIO td{background:#9a5b0f}
.DESCONECTADA td{background:#4a4f52;color:#cfd6d9}
@keyframes blink{0%,100%{background:#1f5c2e}50%{background:#3fbf5f;color:#06210f}}
#log{margin-top:12px;background:#0a1114;border:1px solid var(--line);border-radius:6px;padding:10px;
height:240px;overflow-y:auto;font:12px/1.5 ui-monospace,Menlo,Consolas,monospace;color:#bfe8c8;white-space:pre-wrap}
</style></head><body>
<h1>WM_Central <span>· Irrigation Monitoring Panel</span></h1>
<div class="bar">
<button onclick="cmd('start',false)">Iniciar riego</button>
<button onclick="cmd('block',false)">Bloquear WS</button>
<button onclick="cmd('activate',false)">Activar WS</button>
<span style="width:12px"></span>
<button onclick="cmd('start',true)">Iniciar en todas</button>
<button onclick="cmd('block',true)">Bloquear todas</button>
<button onclick="cmd('activate',true)">Activar todas</button>
<label id="selinfo">Selecciona una WS en la tabla</label>
</div>
<div class="sum" id="sum"></div>
<div class="wrap"><table><thead><tr><th>ID</th><th>Ubicación</th><th>Estado</th>
<th>Caudal (L/min)</th><th>Volumen (L)</th><th>Operario</th></tr></thead><tbody id="tb"></tbody></table></div>
<div id="log"></div>
<script>
let sel=null;
const L={DISPONIBLE:'DISPONIBLE',REGANDO:'REGANDO',FUGA:'FUGA',FUERA_DE_SERVICIO:'FUERA DE SERVICIO',DESCONECTADA:'DESCONECTADA'};
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
function render(s){
  const cnt={};s.ws.forEach(w=>cnt[w.estado]=(cnt[w.estado]||0)+1);
  document.getElementById('sum').innerHTML=Object.keys(L).map(k=>`<span class="chip">${L[k]}: ${cnt[k]||0}</span>`).join('');
  document.getElementById('tb').innerHTML=s.ws.map(w=>{
    const act=w.estado==='REGANDO';
    return `<tr class="${w.estado}${w.id===sel?' sel':''}" onclick="pick('${esc(w.id)}')"><td>${esc(w.id)}</td><td>${esc(w.ubicacion)}</td>
    <td><b>${L[w.estado]}</b></td><td>${act?w.caudal:'—'}</td><td>${act?w.volumen:'—'}</td><td>${act?esc(w.operario):'—'}</td></tr>`}).join('');
  const lg=document.getElementById('log');
  const bottom=lg.scrollTop+lg.clientHeight>=lg.scrollHeight-20;
  lg.textContent=s.logs.join('\n');
  if(bottom)lg.scrollTop=lg.scrollHeight;
}
function pick(id){sel=id;document.getElementById('selinfo').textContent='Seleccionada: WS '+id;refresh();}
async function refresh(){try{render(await (await fetch('/api/state')).json());}catch(e){}}
async function cmd(c,all){
  const t=all?'ALL':sel;if(!t){alert('Selecciona una WS');return;}
  await fetch('/api/cmd',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({cmd:c,target:t})});
  refresh();
}
setInterval(refresh,1000);refresh();
</script></body></html>"""


def make_handler(central):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/api/state":
                self._send(200, json.dumps(central.obtener_estado()).encode(), "application/json")
            elif self.path in ("/", "/index.html"):
                self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self):
            if self.path != "/api/cmd":
                return self._send(404, b"not found", "text/plain")
            try:
                n = int(self.headers.get("Content-Length", 0))
                d = json.loads(self.rfile.read(n))
                cmd, target = d["cmd"], str(d["target"])
                if cmd not in ("start", "block", "activate"):
                    raise ValueError("cmd")
            except Exception:
                return self._send(400, b"bad request", "text/plain")
            central.ejecutar_orden(cmd, target)
            self._send(200, b'{"ok":true}', "application/json")

    return Handler


def main():
    if len(sys.argv) < 3:
        print("Uso: python WM_Central.py <puerto_sockets> <broker_ip:puerto> [fichero_bd]")
        sys.exit(1)
    port = int(sys.argv[1])
    broker = sys.argv[2]
    db_path = sys.argv[3] if len(sys.argv) > 3 else "Base_Datos.sqlite"
    web_port = int(os.environ.get("PORT", 8080))
    Central(port, broker, db_path, web_port).run()


if __name__ == "__main__":
    main()