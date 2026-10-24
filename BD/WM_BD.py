"""WM_BD - Base de datos SQLite de WM_Central (WS, operarios e historico de riegos).

Las tablas son las del esquema original de la practica (WS, OPERARIO y RIEGO)."""
import sqlite3
import threading

from topics import DESCONECTADA


class BaseDatos:
    """Acceso a SQLite. Una sola conexion compartida, protegida por un lock.

    Si una operacion falla (disco lleno, fichero bloqueado...) se deja constancia en el log y
    CENTRAL sigue funcionando: un fallo de la BD no debe tumbar los hilos que atienden a las WS."""

    ESQUEMA = """
        -- Estaciones de riego (WS)
        CREATE TABLE IF NOT EXISTS WS (
            idWS VARCHAR(10) PRIMARY KEY,
            estado TEXT NOT NULL CHECK (estado IN
                ('DISPONIBLE','REGANDO','FUGA','FUERA_DE_SERVICIO','DESCONECTADA')),
            ubicacion VARCHAR(100) NOT NULL
        );

        -- Operarios de campo (FO)
        CREATE TABLE IF NOT EXISTS OPERARIO (
            idOperario VARCHAR(20) PRIMARY KEY
        );

        -- Historico de riegos finalizados
        CREATE TABLE IF NOT EXISTS RIEGO (
            idRiego INTEGER PRIMARY KEY AUTOINCREMENT,
            operario VARCHAR(20) NOT NULL,
            ws VARCHAR(10) NOT NULL,
            estado TEXT NOT NULL CHECK (estado IN ('COMPLETADO','INTERRUMPIDO')),
            motivo TEXT,
            volumen DECIMAL(10,2) NOT NULL,
            duracion DECIMAL(10,1) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (operario) REFERENCES OPERARIO(idOperario),
            FOREIGN KEY (ws) REFERENCES WS(idWS)
        );
    """

    def __init__(self, ruta, escribir_log):
        """Lanza sqlite3.Error si no se puede abrir la BD (error fatal al arrancar)."""
        self.escribir_log = escribir_log
        self.conexion = sqlite3.connect(ruta, check_same_thread=False)
        self.lock = threading.Lock()

    def preparar_y_listar_estaciones(self):
        """Crea las tablas si faltan, marca todas las WS como DESCONECTADA (todavia no han
        conectado) y devuelve [(idWS, ubicacion)]. Lanza sqlite3.Error si falla (error fatal)."""
        with self.lock:
            self.conexion.executescript(self.ESQUEMA)
            self.conexion.execute("UPDATE WS SET estado='DESCONECTADA'")
            self.conexion.commit()
            return self.conexion.execute(
                "SELECT idWS, ubicacion FROM WS ORDER BY idWS").fetchall()

    def ejecutar(self, sql, parametros=()):
        """Ejecuta una sentencia. Devuelve False (y lo deja en el log) si falla."""
        try:
            with self.lock:
                self.conexion.execute(sql, parametros)
                self.conexion.commit()
            return True
        except sqlite3.Error as error:
            self.escribir_log(f"Error en la BD ({error}): no se ha podido guardar un dato")
            return False

    def dar_de_alta_estacion(self, ws_id, ubicacion):
        return self.ejecutar("INSERT OR IGNORE INTO WS(idWS, estado, ubicacion) VALUES (?,?,?)",
                             (ws_id, DESCONECTADA, ubicacion))

    def guardar_estado_estacion(self, ws_id, estado):
        return self.ejecutar("UPDATE WS SET estado=? WHERE idWS=?", (estado, ws_id))

    def registrar_operario(self, operario):
        return self.ejecutar("INSERT OR IGNORE INTO OPERARIO(idOperario) VALUES (?)", (operario,))

    def guardar_riego(self, operario, ws_id, estado, motivo, volumen, duracion):
        return self.ejecutar("INSERT INTO RIEGO(operario, ws, estado, motivo, volumen, duracion) "
                             "VALUES (?,?,?,?,?,?)",
                             (operario, ws_id, estado, motivo, volumen, duracion))