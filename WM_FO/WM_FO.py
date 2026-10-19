"""WM_FO - Field Operator (aplicacion del operario de campo).

Uso: python WM_FO.py <broker_ip:puerto> <id_operador> [fichero_servicios]

- Pide riegos a CENTRAL por Kafka y muestra cada paso del proceso: peticion, comprobaciones,
  autorizacion o denegacion, caudal y volumen en tiempo real y ticket final.
- Tiene un panel web (puerto de la variable PORT, 8081 por defecto) para que el jardinero
  lo use desde el navegador del PC o del movil. Funciona igual dentro de Docker.
- Puede leer un fichero con los servicios a solicitar (desde el panel o como tercer argumento):
  pide el primero y, cuando termina (con exito o fracaso), espera 4 s y pide el siguiente.
  Formatos: JSON [{"idWS": "1"}, {"idWS": "2"}] o texto con un id por linea. tiene que ser ID Estacion y litros o tiempo
"""
import json
import os
import re
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import KafkaError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from protocol import *

ESPERA_ENTRE_SERVICIOS = 4      # s entre el final de un servicio y la peticion del siguiente
TIMEOUT_SERVICIO = 120          # s maximos esperando a que acabe un servicio del fichero
TIMEOUT_SOLICITUD = 30          # s esperando la respuesta de CENTRAL a una peticion
LISTA_CADUCADA = 10             # s sin recibir la lista de WS => CENTRAL no responde
ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,10}$")

MOTIVOS = {
    "LIMITE_TIEMPO": "limite de tiempo alcanzado",
    "FIN_MANUAL": "finalizado desde la estacion",
    "FUGA": "fuga detectada en la estacion",
    "BLOQUEO_CENTRAL": "estacion bloqueada por CENTRAL",
    "ENGINE_CERRADO": "estacion apagada",
}


class FieldOperator:
    def __init__(self, broker, operario, web_port):
        self.broker = broker
        self.operario = str(operario)
        self.web_port = web_port
        self.producer = None
        self.consumer = None
        self.kafka_ok = False
        self.ready = threading.Event()
        self.lock = threading.RLock()
        self.logs = deque(maxlen=300)
        self.ws_disponibles = []            # [{"idWS", "ubicacion"}]
        self.ultima_lista = 0.0
        self.riegos = {}                    # idWS -> estado del riego en curso
        self.tickets = []                   # historial (el mas reciente primero)
        # Servicios leidos de un fichero
        self.secuencia = {"activa": False, "items": []}
        self.cancelar = False
        self.fin_servicio = threading.Event()
        self.ws_actual = None
        self.resultado_actual = None

    def escribir_log(self, mensaje):
        linea = f"[{time.strftime('%H:%M:%S')}] {mensaje}"
        with self.lock:
            self.logs.append(linea)
        print(f"[FO {self.operario}] {linea}", flush=True)

    # ------------------ Kafka ------------------
    def iniciar_kafka(self):
        while True:
            try:
                ensure_topics(self.broker)
                self.producer = JsonProducer(self.broker)
                self.consumer = make_consumer(
                    self.broker,
                    [FO_NOTIFY, FO_AUTH, FO_TICKET, WS_CONSUMPTION, WS_LIST],
                    f"wm_fo_{self.operario}", self.ready)
                return
            except Exception as e:
                self.escribir_log(f"Broker Kafka no disponible ({e}), reintentando en 3 s...")
                time.sleep(3)

    def arrancar_kafka(self, fichero=None):
        self.iniciar_kafka()
        threading.Thread(target=self.escuchar, daemon=True).start()
        self.ready.wait(30)              # esperar a tener las particiones asignadas
        self.kafka_ok = True
        self.escribir_log(f"Conectado a Kafka ({self.broker})")
        if fichero:
            try:
                with open(fichero, "r", encoding="utf-8") as f:
                    ok, msg = self.lanzar_servicios(self.parsear_servicios(f.read()))
                self.escribir_log(msg)
            except OSError as e:
                self.escribir_log(f"No se pudo leer el fichero '{fichero}': {e}")

    def escuchar(self):
        while True:
            m = self.consumer.poll(1.0)
            if m is None:
                continue
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    self.escribir_log(f"Kafka: {m.error()}")
                continue
            datos = kafka_loads(m)
            topic = m.topic()
            try:
                if topic == WS_LIST:
                    self.recibir_lista(datos)
                elif isinstance(datos, dict):
                    if topic == FO_NOTIFY:
                        self.recibir_aviso(datos)
                    elif topic == FO_AUTH:
                        self.recibir_autorizacion(datos)
                    elif topic == WS_CONSUMPTION:
                        self.recibir_consumo(datos)
                    elif topic == FO_TICKET:
                        self.recibir_ticket(datos.get("ticket", {}))
            except Exception as e:
                self.escribir_log(f"Error procesando mensaje de {topic}: {e}")

    # ------------------ Mensajes recibidos ------------------
    def recibir_lista(self, datos):
        if not isinstance(datos, list):
            return
        with self.lock:
            self.ws_disponibles = [{"idWS": str(w.get("idWS")), "ubicacion": str(w.get("ubicacion", ""))}
                                   for w in datos if isinstance(w, dict)]
            self.ultima_lista = time.time()

    def recibir_aviso(self, datos):
        if str(datos.get("idOperator")) != self.operario:
            return
        self.escribir_log(f"CENTRAL: {datos.get('msg')} (WS {datos.get('idWS')})")

    def recibir_autorizacion(self, datos):
        if str(datos.get("idOperator")) != self.operario:
            return
        ws_id = str(datos.get("idWS"))
        if datos.get("authorize") == "YES":
            with self.lock:
                if ws_id in self.riegos:
                    self.riegos[ws_id]["estado"] = "AUTORIZADO"
            self.escribir_log(f"Riego AUTORIZADO en WS {ws_id}. Esperando datos de la estacion...")
        else:
            motivo = datos.get("motivo", "")
            with self.lock:
                self.riegos.pop(ws_id, None)
                self.tickets.insert(0, self.fila_historial(ws_id, "DENEGADO", motivo, 0, 0))
            self.escribir_log(f"Riego DENEGADO en WS {ws_id}: {motivo}")
            self.finalizar_servicio(ws_id, "DENEGADO")

    def recibir_consumo(self, datos):
        if str(datos.get("idOperator")) != self.operario:
            return
        ws_id = str(datos.get("idWS"))
        caudal = float(datos.get("caudal", 0))
        volumen = float(datos.get("volumen", 0))
        duracion = float(datos.get("duracion", 0))
        with self.lock:
            self.riegos[ws_id] = {"estado": "REGANDO", "caudal": caudal, "volumen": volumen,
                                  "duracion": duracion, "t": time.time()}
        print(f"[FO {self.operario}] WS {ws_id} regando | {caudal:.1f} L/min | {volumen:.2f} L | "
              f"{duracion:.0f} s", flush=True)

    def recibir_ticket(self, ticket):
        if str(ticket.get("idOperator")) != self.operario:
            return
        ws_id = str(ticket.get("idWS"))
        estado = str(ticket.get("estado", ""))
        motivo = MOTIVOS.get(ticket.get("motivo"), ticket.get("motivo", ""))
        volumen = float(ticket.get("volumen", 0))
        duracion = float(ticket.get("duracion", 0))
        with self.lock:
            self.riegos.pop(ws_id, None)
            self.tickets.insert(0, self.fila_historial(ws_id, estado, motivo, volumen, duracion))
        self.escribir_log(f"TICKET WS {ws_id}: riego {estado} ({motivo}) - "
                          f"{volumen:.2f} L en {duracion:.1f} s")
        self.finalizar_servicio(ws_id, estado)

    @staticmethod
    def fila_historial(ws_id, estado, motivo, volumen, duracion):
        return {"hora": time.strftime("%H:%M:%S"), "idWS": ws_id, "estado": estado,
                "motivo": motivo, "volumen": round(volumen, 2), "duracion": round(duracion, 1)}

    def finalizar_servicio(self, ws_id, resultado):
        """Un servicio del fichero acaba al llegar la denegacion o el ticket de su WS."""
        if self.ws_actual == ws_id:
            self.resultado_actual = resultado
            self.fin_servicio.set()

    # ------------------ Peticiones ------------------
    def solicitar(self, ws_id):
        ws_id = str(ws_id).strip()
        if not ID_RE.match(ws_id):
            return False, "ID de WS no valido"
        if not self.kafka_ok:
            return False, "Sin conexion con Kafka todavia"
        with self.lock:
            if ws_id in self.riegos:
                return False, f"Ya tienes un riego solicitado o en curso en la WS {ws_id}"
            self.riegos[ws_id] = {"estado": "SOLICITADO", "caudal": 0, "volumen": 0,
                                  "duracion": 0, "t": time.time()}
        self.producer.send(FO_REQUEST, {"idOperator": self.operario, "idWS": ws_id})
        self.producer.flush()
        self.escribir_log(f"Peticion de riego enviada a CENTRAL para la WS {ws_id}")
        return True, f"Peticion enviada para la WS {ws_id}"

    def limpiar_solicitudes(self):
        """Quita las peticiones a las que CENTRAL no ha respondido a tiempo."""
        caducadas = []
        with self.lock:
            for ws_id, r in list(self.riegos.items()):
                if r["estado"] in ("SOLICITADO", "AUTORIZADO") and time.time() - r["t"] > TIMEOUT_SOLICITUD:
                    caducadas.append(ws_id)
                    del self.riegos[ws_id]
        for ws_id in caducadas:
            self.escribir_log(f"Sin respuesta de CENTRAL para la WS {ws_id}")

    # ------------------ Servicios desde fichero ------------------
    @staticmethod
    def parsear_servicios(texto):
        try:
            datos = json.loads(texto)
            if not isinstance(datos, list):
                raise ValueError("el JSON debe ser una lista de servicios")
            servicios = [str(d.get("idWS")) if isinstance(d, dict) else str(d) for d in datos]
        except json.JSONDecodeError:
            servicios = [linea.strip() for linea in texto.splitlines() if linea.strip()]
        return [s for s in servicios if ID_RE.match(s)]

    def lanzar_servicios(self, servicios):
        if not servicios:
            return False, "El fichero no contiene servicios validos"
        if not self.kafka_ok:
            return False, "Sin conexion con Kafka todavia"
        with self.lock:
            if self.secuencia["activa"]:
                return False, "Ya hay una lista de servicios en curso"
            self.secuencia = {"activa": True,
                              "items": [{"idWS": s, "estado": "PENDIENTE"} for s in servicios]}
            self.cancelar = False
        self.escribir_log(f"Lista cargada: {len(servicios)} servicios -> {', '.join(servicios)}")
        threading.Thread(target=self.procesar_servicios, daemon=True).start()
        return True, f"Lista cargada: {len(servicios)} servicios"

    def procesar_servicios(self):
        items = self.secuencia["items"]
        total = len(items)
        for i, item in enumerate(items):
            if self.cancelar:
                for resto in items[i:]:
                    resto["estado"] = "CANCELADO"
                break
            ws_id = item["idWS"]
            self.escribir_log(f"--- Servicio {i + 1}/{total}: WS {ws_id} ---")
            item["estado"] = "EN CURSO"
            self.fin_servicio.clear()
            self.resultado_actual = None
            self.ws_actual = ws_id
            ok, msg = self.solicitar(ws_id)
            if not ok:
                item["estado"] = "ERROR"
                self.escribir_log(msg)
            elif not self.fin_servicio.wait(TIMEOUT_SERVICIO):
                item["estado"] = "SIN RESPUESTA"
                self.escribir_log(f"Sin respuesta de CENTRAL para la WS {ws_id}. Paso al siguiente")
            else:
                item["estado"] = self.resultado_actual or "CANCELADO"
            self.ws_actual = None
            if i < total - 1 and not self.cancelar:
                self.escribir_log(f"Siguiente servicio en {ESPERA_ENTRE_SERVICIOS} s...")
                time.sleep(ESPERA_ENTRE_SERVICIOS)
        self.escribir_log("Lista de servicios cancelada" if self.cancelar else "Lista de servicios completada")
        with self.lock:
            self.secuencia["activa"] = False

    def cancelar_servicios(self):
        with self.lock:
            if not self.secuencia["activa"]:
                return False, "No hay ninguna lista en curso"
            self.cancelar = True
        self.fin_servicio.set()
        return True, "Lista cancelada (el riego ya iniciado seguira hasta terminar)"

    # ------------------ Estado para el panel ------------------
    def snapshot(self):
        self.limpiar_solicitudes()
        with self.lock:
            return {
                "operario": self.operario,
                "kafka": self.kafka_ok,
                "central": time.time() - self.ultima_lista < LISTA_CADUCADA,
                "ws": list(self.ws_disponibles),
                "riegos": [dict(idWS=k, estado=v["estado"], caudal=round(v["caudal"], 1),
                                volumen=round(v["volumen"], 2), duracion=round(v["duracion"]))
                           for k, v in self.riegos.items()],
                "tickets": self.tickets[:50],
                "secuencia": {"activa": self.secuencia["activa"],
                              "items": [dict(i) for i in self.secuencia["items"]]},
                "logs": list(self.logs)[-120:],
            }

    def run(self, fichero=None):
        threading.Thread(target=self.arrancar_kafka, args=(fichero,), daemon=True).start()
        httpd = ThreadingHTTPServer(("0.0.0.0", self.web_port), make_handler(self))
        self.escribir_log(f"Panel del operario en http://localhost:{self.web_port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass


# ------------------ Panel web ------------------
HTML = r"""<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WM_FO</title>
<style>
:root{--bg:#0f1a1f;--panel:#16252c;--fg:#e6f0f2;--mut:#8aa0a8;--line:#27404a;--ok:#1f5c2e;--okl:#3fbf5f}
*{box-sizing:border-box}
body{margin:0;padding:16px;background:var(--bg);color:var(--fg);font-family:system-ui,sans-serif}
h1{font-size:18px;margin:0}h1 span{color:var(--mut);font-weight:400}
h2{font-size:14px;margin:0 0 10px;color:var(--mut);font-weight:600;text-transform:uppercase;letter-spacing:.04em}
header{display:flex;gap:12px;flex-wrap:wrap;align-items:center;justify-content:space-between;margin-bottom:14px}
.chips{display:flex;gap:8px;flex-wrap:wrap}
.chip{padding:4px 10px;border-radius:999px;font-size:12px;border:1px solid var(--line);background:var(--panel)}
.chip.ok{background:var(--ok);border-color:var(--okl)}.chip.ko{background:#8b1e1e;border-color:#c24}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:800px){.grid{grid-template-columns:1fr}}
section{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px;min-width:0}
.wide{grid-column:1/-1}
table{width:100%;border-collapse:collapse;font-size:14px}
th,td{padding:8px 6px;text-align:left;border-bottom:1px solid var(--line)}
th{color:var(--mut);font-weight:500}
.wrap{overflow-x:auto}
button{background:#1d3a46;color:var(--fg);border:1px solid #3a6577;padding:9px 14px;border-radius:6px;cursor:pointer;font-size:14px}
button:hover{border-color:#6fb3c8}
button.main{background:var(--ok);border-color:var(--okl)}
input[type=text]{background:#0a1114;color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:9px;width:110px}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.empty{color:var(--mut);font-size:14px;padding:6px 0}
.card{border-radius:8px;padding:12px;margin-bottom:10px;border:1px solid var(--line);background:#1b2f38}
.card.REGANDO{animation:blink 1s infinite;border-color:var(--okl)}
.card b{font-size:16px}.card .d{color:var(--mut);font-size:13px;margin-top:4px}
.card.REGANDO .d{color:#d7ffe1}
@keyframes blink{0%,100%{background:#1f5c2e}50%{background:#2f8f48}}
.pill{display:inline-block;padding:3px 9px;border-radius:999px;font-size:12px;margin:2px;border:1px solid var(--line)}
.pill.COMPLETADO{background:var(--ok)}.pill.DENEGADO,.pill.ERROR,.pill.INTERRUMPIDO{background:#8b1e1e}
.pill.EN\ CURSO{background:#9a5b0f}.pill.SIN\ RESPUESTA,.pill.CANCELADO{background:#4a4f52}
.st-COMPLETADO{color:#6fe28f}.st-INTERRUMPIDO,.st-DENEGADO{color:#ff8a8a}
#log{background:#0a1114;border:1px solid var(--line);border-radius:6px;padding:10px;height:230px;overflow-y:auto;
font:12px/1.5 ui-monospace,Menlo,Consolas,monospace;color:#bfe8c8;white-space:pre-wrap}
#toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);background:#1d3a46;border:1px solid #6fb3c8;
padding:10px 16px;border-radius:8px;display:none;max-width:90%}
</style></head><body>
<header>
<h1>WM_FO <span>· Operario <b id="op"></b></span></h1>
<div class="chips"><span class="chip" id="c-kafka">Kafka</span><span class="chip" id="c-central">CENTRAL</span></div>
</header>
<div class="grid">
<section><h2>Estaciones disponibles</h2>
<div class="wrap"><table><thead><tr><th>WS</th><th>Ubicación</th><th></th></tr></thead><tbody id="ws"></tbody></table></div>
<div id="ws-empty" class="empty"></div>
<div class="row" style="margin-top:12px"><input type="text" id="manual" placeholder="ID de WS" maxlength="10">
<button onclick="pedir(document.getElementById('manual').value)">Solicitar por ID</button></div>
</section>
<section><h2>Mis riegos</h2><div id="riegos"></div><div id="riegos-empty" class="empty">No tienes ningún riego en curso.</div></section>
<section class="wide"><h2>Servicios desde fichero</h2>
<div class="row"><input type="file" id="fichero" accept=".json,.txt">
<button class="main" onclick="cargar()">Cargar y lanzar</button><button onclick="cancelar()">Cancelar lista</button></div>
<div id="seq" style="margin-top:10px"></div>
</section>
<section class="wide"><h2>Mensajes del proceso</h2><div id="log"></div></section>
<section class="wide"><h2>Historial de riegos</h2>
<div class="wrap"><table><thead><tr><th>Hora</th><th>WS</th><th>Resultado</th><th>Motivo</th><th>Volumen (L)</th><th>Duración (s)</th></tr></thead>
<tbody id="hist"></tbody></table></div><div id="hist-empty" class="empty">Todavía no hay riegos.</div></section>
</div>
<div id="toast"></div>
<script>
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const E={SOLICITADO:'Solicitado, esperando a CENTRAL...',AUTORIZADO:'Autorizado, abriendo la electroválvula...',REGANDO:'Regando'};
function toast(t){const d=document.getElementById('toast');d.textContent=t;d.style.display='block';clearTimeout(window._t);window._t=setTimeout(()=>d.style.display='none',3500);}
async function post(url,body){try{const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
 const j=await r.json();toast(j.msg||j.error||'');refresh();}catch(e){toast('No se pudo contactar con la aplicación');}}
function pedir(id){id=String(id||'').trim();if(!id){toast('Escribe el ID de la WS');return;}post('/api/solicitar',{idWS:id});}
function cancelar(){post('/api/cancelar');}
function cargar(){const f=document.getElementById('fichero').files[0];if(!f){toast('Elige un fichero primero');return;}
 const r=new FileReader();r.onload=()=>post('/api/servicios',{texto:r.result});r.readAsText(f);}
function render(s){
 document.getElementById('op').textContent=s.operario;
 const ck=document.getElementById('c-kafka'),cc=document.getElementById('c-central');
 ck.className='chip '+(s.kafka?'ok':'ko');ck.textContent='Kafka: '+(s.kafka?'conectado':'sin conexión');
 cc.className='chip '+(s.central?'ok':'ko');cc.textContent='CENTRAL: '+(s.central?'en línea':'sin respuesta');
 document.getElementById('ws').innerHTML=s.ws.map(w=>`<tr><td><b>${esc(w.idWS)}</b></td><td>${esc(w.ubicacion)}</td>
  <td><button class="main" onclick="pedir('${esc(w.idWS)}')">Solicitar riego</button></td></tr>`).join('');
 document.getElementById('ws-empty').textContent=s.ws.length?'':(s.central?'No hay estaciones disponibles ahora mismo.':'Sin datos de CENTRAL.');
 document.getElementById('riegos').innerHTML=s.riegos.map(r=>`<div class="card ${r.estado}"><b>WS ${esc(r.idWS)}</b> · ${E[r.estado]||esc(r.estado)}
  ${r.estado==='REGANDO'?`<div class="d">Caudal: ${r.caudal} L/min · Volumen: ${r.volumen} L · Tiempo: ${r.duracion} s</div>`:''}</div>`).join('');
 document.getElementById('riegos-empty').style.display=s.riegos.length?'none':'block';
 const items=s.secuencia.items;
 document.getElementById('seq').innerHTML=items.length?items.map((i,n)=>`<span class="pill ${esc(i.estado)}">${n+1}. WS ${esc(i.idWS)} · ${esc(i.estado)}</span>`).join('')
  +(s.secuencia.activa?'<div class="empty">Lista en curso...</div>':''):'<div class="empty">Sin lista cargada. Formatos: JSON [{"idWS":"1"}] o un ID por línea.</div>';
 document.getElementById('hist').innerHTML=s.tickets.map(t=>`<tr><td>${esc(t.hora)}</td><td>${esc(t.idWS)}</td>
  <td class="st-${esc(t.estado)}"><b>${esc(t.estado)}</b></td><td>${esc(t.motivo)}</td><td>${t.volumen}</td><td>${t.duracion}</td></tr>`).join('');
 document.getElementById('hist-empty').style.display=s.tickets.length?'none':'block';
 const lg=document.getElementById('log'),b=lg.scrollTop+lg.clientHeight>=lg.scrollHeight-20;
 lg.textContent=s.logs.join('\n');if(b)lg.scrollTop=lg.scrollHeight;
}
async function refresh(){try{render(await (await fetch('/api/state')).json());}catch(e){}}
setInterval(refresh,1000);refresh();
</script></body></html>"""


def make_handler(fo):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, cuerpo, tipo="application/json"):
            if isinstance(cuerpo, (dict, list)):
                cuerpo = json.dumps(cuerpo).encode("utf-8")
            elif isinstance(cuerpo, str):
                cuerpo = cuerpo.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(cuerpo)))
            self.end_headers()
            self.wfile.write(cuerpo)

        def do_GET(self):
            if self.path == "/api/state":
                self._send(200, fo.snapshot())
            elif self.path in ("/", "/index.html"):
                self._send(200, HTML, "text/html; charset=utf-8")
            else:
                self._send(404, "not found", "text/plain")

        def do_POST(self):
            try:
                n = int(self.headers.get("Content-Length", 0))
                datos = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._send(400, {"ok": False, "error": "peticion no valida"})
            if self.path == "/api/solicitar":
                ok, msg = fo.solicitar(datos.get("idWS", ""))
            elif self.path == "/api/servicios":
                ok, msg = fo.lanzar_servicios(fo.parsear_servicios(str(datos.get("texto", ""))))
            elif self.path == "/api/cancelar":
                ok, msg = fo.cancelar_servicios()
            else:
                return self._send(404, {"ok": False, "error": "no encontrado"})
            self._send(200, {"ok": ok, "msg": msg})

    return Handler


def main():
    if len(sys.argv) < 3:
        print("Uso: python WM_FO.py <broker_ip:puerto> <id_operador> [fichero_servicios]")
        sys.exit(1)
    broker = sys.argv[1]
    operario = sys.argv[2]
    fichero = sys.argv[3] if len(sys.argv) > 3 else None
    web_port = int(os.environ.get("PORT", 8081))
    FieldOperator(broker, operario, web_port).run(fichero)


if __name__ == "__main__":
    main()
