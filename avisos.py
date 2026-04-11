import logging
import os
import sys
import time
import signal
import atexit
import threading
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# ============================================================
# CONFIGURACIÓN GLOBAL
# ============================================================

BASE_URL = "https://gestiona7.madrid.org/ctac_cita"
DISPONIBILIDAD_ENDPOINT = "/cita/obtieneDiasDisponibles"
REFERER = "https://gestiona7.madrid.org/ctac_cita/registro#"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
TELEGRAM_API_URL = "https://api.telegram.org"

LOG_FILE_PATH = "avisos.log"
DEFAULT_POLL_SECONDS = 3600  # 1 hora

HEARTBEAT_HOUR = 14       # Reporte diario a las 14:00 hora Madrid
HEARTBEAT_ENABLED = True

# ============================================================
# MONITORES
#
# mode = "madrid"  → avisa SOLO si hay fechas en los meses filtrados
# mode = "leganes" → avisa cuando el servicio se abre (cualquier fecha)
#
# Para Madrid: busca el idGrupo e idServicio en la URL del portal
# gestiona7.madrid.org al hacer la consulta de citas manualmente.
# ============================================================

MONITORS: List[Dict[str, Any]] = [
    {
        "label": "Registro Civil de Madrid - CITA PREJURAS",
        "center": "Registro Civil de Madrid",
        "service_name": "CITA PREJURAS",
        "id_grupo": 1202,
        "id_servicio": 9891,
        "tiempo_cita_seconds": 30,
        "mode": "madrid",
        # Meses a vigilar: 4=Abril, 5=Mayo (año actual)
        # Se actualizan automáticamente: mes en curso + mes siguiente
        "filter_months": None,  # Se calcula en runtime
    },
    {
        "label": "Registro Civil de Leganés - CITA PREJURAS",
        "center": "Registro Civil de Leganés",
        "service_name": "CITA PREJURAS",
        "id_grupo": 203,
        "id_servicio": 2274,
        "tiempo_cita_seconds": 20,
        "mode": "leganes",
        "filter_months": None,  # Sin filtro: avisa cuando se abra
    },
]


# ============================================================
# HORA MADRID
# ============================================================

def get_madrid_time() -> datetime:
    try:
        import zoneinfo
        return datetime.now(zoneinfo.ZoneInfo("Europe/Madrid"))
    except ImportError:
        now_utc = datetime.now(timezone.utc)
        year = now_utc.year
        summer_start = datetime(year, 3, 25, 1, 0, 0, tzinfo=timezone.utc)
        summer_end = datetime(year, 10, 25, 1, 0, 0, tzinfo=timezone.utc)
        offset = 2 if summer_start <= now_utc < summer_end else 1
        return now_utc + timedelta(hours=offset)


def get_target_months() -> List[int]:
    """Devuelve [mes_actual, mes_siguiente] para filtrar citas de Madrid."""
    now = get_madrid_time()
    current = now.month
    nxt = current % 12 + 1
    return [current, nxt]


# ============================================================
# ENV / LOGGING
# ============================================================

def load_dotenv(env_path: Path = Path(".env")) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_env_variable_any(keys: List[str], default: Optional[str] = None) -> str:
    for key in keys:
        value = os.getenv(key, default)
        if value is not None and value != "":
            return value
    raise RuntimeError(f"Ninguna de las variables {keys} está configurada")


def setup_logging(log_file: str, level: str = "INFO") -> None:
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram_message(token: str, chat_id: str, text: str) -> None:
    url = f"{TELEGRAM_API_URL}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    response = requests.post(url, json=payload, timeout=20)
    response.raise_for_status()


# ============================================================
# HTTP SESSION
# ============================================================

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Referer": REFERER,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    })
    return session


# ============================================================
# DISPONIBILIDAD
# ============================================================

def fetch_availability(session: requests.Session, monitor: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}{DISPONIBILIDAD_ENDPOINT}"
    params = {
        "idServicio": monitor["id_servicio"],
        "idGrupo": monitor["id_grupo"],
        "tiempoCita": monitor["tiempo_cita_seconds"],
    }
    logging.info("[%s] Consultando disponibilidad...", monitor["label"])
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return {
        "dias_disponibles": data.get("diasDisponibles", []),
        "dias_no_disponibles": data.get("diasNoDisponibles", []),
        "dias_festivos": data.get("diasFestivos", []),
        "dias_ocupados": data.get("diasOcupados", []),
    }


def normalize_date(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        try:
            return time.strftime("%Y-%m-%d", time.localtime(value / 1000))
        except Exception:
            return str(value)
    return str(value)


def parse_date(value: Any) -> Optional[datetime]:
    """Intenta parsear la fecha a un objeto datetime."""
    raw = normalize_date(value)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def filter_by_months(dates: List[Any], months: List[int]) -> List[Any]:
    """Filtra fechas que correspondan a los meses indicados (año actual o siguiente)."""
    result = []
    now = get_madrid_time()
    for d in dates:
        dt = parse_date(d)
        if dt is None:
            continue
        # Acepta el año actual o el siguiente (por si el mes siguiente es enero)
        if dt.month in months and dt.year in (now.year, now.year + 1):
            result.append(d)
    return result


def format_dates(dates: List[Any], max_items: int = 15) -> str:
    if not dates:
        return "ninguna"
    shown = [normalize_date(d) for d in dates[:max_items]]
    extra = len(dates) - max_items
    text = ", ".join(shown)
    if extra > 0:
        text += f" (+{extra} más)"
    return text


def month_name(month: int) -> str:
    names = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo",
             6: "Junio", 7: "Julio", 8: "Agosto", 9: "Septiembre",
             10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
    return names.get(month, str(month))


# ============================================================
# HEARTBEAT (reporte diario)
# ============================================================

class HeartbeatManager:
    def __init__(self, bot_token: str, chat_id: str, hour: int = 14):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.hour = hour
        self._timer: Optional[threading.Timer] = None
        self._schedule_next()

    def _seconds_to_next(self) -> float:
        now = get_madrid_time()
        next_run = now.replace(hour=self.hour, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)
        return (next_run - now).total_seconds()

    def _schedule_next(self):
        seconds = self._seconds_to_next()
        self._timer = threading.Timer(seconds, self._send)
        self._timer.daemon = True
        self._timer.start()
        logging.debug("Próximo heartbeat en %.0f s", seconds)

    def _send(self):
        now = get_madrid_time()
        target_months = get_target_months()
        month_list = " / ".join(month_name(m) for m in target_months)

        active_monitors = [m for m in MONITORS if m["id_grupo"] != 0]
        monitor_lines = "\n".join(
            f"  • {m['center']} ({m['mode'].upper()})" for m in active_monitors
        )

        message = (
            f"✅ <b>AVISO CITAS - REPORTE DIARIO</b>\n"
            f"─────────────────────\n"
            f"🟢 Estado: <b>Funcionando correctamente</b>\n"
            f"📅 Fecha: {now.strftime('%d/%m/%Y')} — {now.strftime('%H:%M')} Madrid\n"
            f"📋 Monitores activos:\n{monitor_lines}\n"
            f"🗓️ Meses vigilados (Madrid): {month_list}\n"
            f"🔄 Intervalo: {DEFAULT_POLL_SECONDS // 60} min\n"
            f"─────────────────────\n"
            f"<i>El bot está activo y monitoreando.</i>"
        )
        try:
            send_telegram_message(self.bot_token, self.chat_id, message)
            logging.info("Heartbeat enviado.")
        except Exception as e:
            logging.error("Error enviando heartbeat: %s", e)
        self._schedule_next()

    def stop(self):
        if self._timer:
            self._timer.cancel()


# ============================================================
# SHUTDOWN HANDLER
# ============================================================

class ShutdownHandler:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._sent = False
        atexit.register(self._on_exit)
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)

    def _on_signal(self, signum, _frame):
        self._send(f"⚠️ <b>BOT DETENIDO</b>\nSeñal recibida: {signum}")
        sys.exit(0)

    def _on_exit(self):
        self._send(
            "⚠️ <b>BOT DETENIDO</b>\n"
            "El bot se ha detenido inesperadamente.\n"
            "Revisa el servidor o reinicia el servicio."
        )

    def _send(self, message: str):
        if self._sent:
            return
        self._sent = True
        try:
            send_telegram_message(self.bot_token, self.chat_id, message)
        except Exception as e:
            logging.error("Error enviando aviso de apagado: %s", e)


# ============================================================
# LÓGICA DE CADA MONITOR
# ============================================================

class MonitorState:
    def __init__(self, monitor: Dict[str, Any]):
        self.monitor = monitor
        self.last_available = False  # True si en el último ciclo había citas


def check_monitor(
    state: MonitorState,
    session: requests.Session,
    bot_token: str,
    chat_id: str,
) -> None:
    m = state.monitor
    mode = m["mode"]

    data = fetch_availability(session, m)
    dias = data["dias_disponibles"]

    if mode == "madrid":
        # Solo nos interesan citas en el mes actual y el siguiente
        target_months = get_target_months()
        dias_validos = filter_by_months(dias, target_months)

        if dias_validos and not state.last_available:
            month_list = " / ".join(month_name(mo) for mo in target_months)
            formatted = format_dates(dias_validos)
            message = (
                f"🎉 <b>¡CITA DISPONIBLE EN MADRID!</b>\n"
                f"─────────────────────\n"
                f"📋 Servicio: {m['service_name']}\n"
                f"📍 Centro: {m['center']}\n"
                f"🗓️ Meses vigilados: {month_list}\n"
                f"📅 Fechas encontradas: {formatted}\n"
                f"─────────────────────\n"
                f"🔗 <a href=\"https://gestiona7.madrid.org/ctac_cita/registro#\">Reservar cita</a>"
            )
            send_telegram_message(bot_token, chat_id, message)
            logging.info("[MADRID] ¡Citas en %s encontradas! → Notificación enviada.", month_list)
            state.last_available = True
        elif not dias_validos and state.last_available:
            logging.info("[MADRID] Las citas de %s ya no están disponibles.", "/".join(month_name(mo) for mo in target_months))
            state.last_available = False
        elif dias_validos:
            logging.info("[MADRID] Siguen habiendo citas en los meses objetivo.")
        else:
            logging.info("[MADRID] Sin citas en abril/mayo por ahora.")

    elif mode == "leganes":
        is_open = len(dias) > 0

        if is_open and not state.last_available:
            # El servicio acaba de abrirse
            message = (
                f"🟢 <b>¡LEGANÉS ABIERTO!</b>\n"
                f"─────────────────────\n"
                f"📋 Servicio: {m['service_name']}\n"
                f"📍 Centro: {m['center']}\n"
                f"📅 Fechas disponibles: {format_dates(dias)}\n"
                f"─────────────────────\n"
                f"🔗 <a href=\"https://gestiona7.madrid.org/ctac_cita/registro#\">Reservar cita</a>"
            )
            send_telegram_message(bot_token, chat_id, message)
            logging.info("[LEGANÉS] ¡Servicio abierto! → Notificación enviada.")
            state.last_available = True
        elif not is_open and state.last_available:
            logging.info("[LEGANÉS] El servicio volvió a cerrarse.")
            state.last_available = False
        elif is_open:
            logging.info("[LEGANÉS] Sigue abierto.")
        else:
            logging.info("[LEGANÉS] Cerrado, sin fechas disponibles.")


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    global DEFAULT_POLL_SECONDS

    load_dotenv()

    log_file = os.getenv("LOG_FILE_PATH", LOG_FILE_PATH)
    log_level = os.getenv("LOG_LEVEL", "INFO")
    setup_logging(log_file, log_level)

    bot_token = get_env_variable_any(["TELEGRAM_BOT_TOKEN", "TG_TOKEN"])
    chat_id = get_env_variable_any(["TELEGRAM_CHAT_ID", "TG_CHAT_ID"])

    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", str(DEFAULT_POLL_SECONDS)))
    DEFAULT_POLL_SECONDS = poll_interval

    # Filtrar monitores con IDs configurados
    active_monitors = [m for m in MONITORS if m["id_grupo"] != 0 and m["id_servicio"] != 0]
    skipped = [m for m in MONITORS if m["id_grupo"] == 0 or m["id_servicio"] == 0]

    if skipped:
        for m in skipped:
            logging.warning("Monitor DESACTIVADO (IDs sin configurar): %s", m["label"])

    if not active_monitors:
        logging.error("No hay ningún monitor activo. Configura los IDs en MONITORS.")
        sys.exit(1)

    target_months = get_target_months()
    month_str = " / ".join(month_name(m) for m in target_months)

    logging.info("=" * 55)
    logging.info("AVISO CITAS - INICIANDO")
    logging.info("Monitores activos: %d", len(active_monitors))
    for m in active_monitors:
        logging.info("  • [%s] %s", m["mode"].upper(), m["label"])
    logging.info("Meses vigilados (Madrid): %s", month_str)
    logging.info("Intervalo de consulta: %d s", poll_interval)
    logging.info("=" * 55)

    session = build_session()
    states = [MonitorState(m) for m in active_monitors]

    ShutdownHandler(bot_token, chat_id)
    if HEARTBEAT_ENABLED:
        HeartbeatManager(bot_token, chat_id, HEARTBEAT_HOUR)

    # Mensaje de inicio
    monitor_lines = "\n".join(
        f"  • {m['center']} ({m['mode'].upper()})" for m in active_monitors
    )
    startup_msg = (
        f"🚀 <b>AVISO CITAS - INICIADO</b>\n"
        f"─────────────────────\n"
        f"📋 Monitores:\n{monitor_lines}\n"
        f"🗓️ Meses objetivo (Madrid): {month_str}\n"
        f"🔄 Intervalo: {poll_interval // 60} min\n"
        f"❤️ Reporte diario: {HEARTBEAT_HOUR}:00 Madrid\n"
        f"─────────────────────\n"
        f"<i>Monitoreando disponibilidad de citas...</i>"
    )
    try:
        send_telegram_message(bot_token, chat_id, startup_msg)
        logging.info("Notificación de inicio enviada.")
    except Exception:
        logging.exception("Error enviando notificación de inicio.")

    # Bucle principal
    try:
        while True:
            for state in states:
                try:
                    check_monitor(state, session, bot_token, chat_id)
                except requests.exceptions.RequestException as e:
                    logging.error("[%s] Error de red: %s", state.monitor["label"], e)
                except Exception:
                    logging.exception("[%s] Error inesperado.", state.monitor["label"])

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        logging.info("Bot detenido por el usuario.")
        sys.exit(0)


if __name__ == "__main__":
    main()
