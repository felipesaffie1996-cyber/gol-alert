"""
⚽ GOL ALERT SYSTEM — Monitor de goles en minuto 89+
Descripción: Monitorea partidos en vivo en 21 ligas y envía alertas
             por Telegram cuando hay alta probabilidad de gol tardío.

SISTEMA DE PUNTOS:
  Criterio 1  — Deuda goles tardíos (últimos 1-3 partidos sin gol en 89+):  2 pts
  Criterio 2  — Momentum (gol entre min 80-88):                             1 pt
  Criterio 3  — Over frustrado (top vs colista, 0-0 al 85+):                1 pt
  Criterio 4  — Partido parejo despertó (gol entre min 60-80):              1 pt
  Criterio 5  — Local perdiendo al min 80+:                                 1 pt
  Criterio 6  — Dominio estadístico (tiros 6+, posesión 60%+ o córners 6+): 1 pt
  Criterio 7  — Jornada simultánea (2+ partidos a la misma hora):           1 pt
  Criterio 8  — Deuda BTTS (< 25% partidos con ambos anotando):             2 pts
  Criterio 9  — Marcador dominante de jornada (50%+ repiten marcador):      1 pt
  Criterio 10 — Resistencia de goles/techo (3 toques=1pt, 4+ toques=2pts): 1-2 pts

NIVELES DE ALERTA (mínimo 2 puntos para alertar):
  2 pts → 🟠 ALERT
  3 pts → 🔴 MÁXIMA
  4+ pts → 🚨 TOTAL
"""

import requests
import time
import json
import base64
from datetime import datetime
from collections import defaultdict

# ============================================================
# GOOGLE SHEETS — Base de datos persistente de alertas
# ============================================================
SHEET_ID = "1FWWX7eMEExqUnBY7tHcHVB83rii2DKTSuVogvDnK458"

import os
_creds_raw = os.environ.get("GOOGLE_CREDS_JSON", "{}")
try:
    GOOGLE_CREDS = json.loads(_creds_raw)
except Exception as e:
    print(f"[SHEETS] Error leyendo GOOGLE_CREDS_JSON: {e}")
    GOOGLE_CREDS = {}

_sheets_token = None
_sheets_token_expiry = 0

def get_sheets_token():
    """Obtiene token OAuth2 para Google Sheets usando JWT."""
    global _sheets_token, _sheets_token_expiry
    ahora = time.time()
    if _sheets_token and ahora < _sheets_token_expiry - 60:
        return _sheets_token
    try:
        import urllib.request, urllib.parse
        import hmac, hashlib
        from base64 import urlsafe_b64encode

        def b64(data):
            if isinstance(data, str):
                data = data.encode()
            return urlsafe_b64encode(data).rstrip(b"=").decode()

        header  = b64(json.dumps({"alg": "RS256", "typ": "JWT"}))
        now_int = int(ahora)
        payload = b64(json.dumps({
            "iss":   GOOGLE_CREDS["client_email"],
            "scope": "https://www.googleapis.com/auth/spreadsheets",
            "aud":   "https://oauth2.googleapis.com/token",
            "exp":   now_int + 3600,
            "iat":   now_int
        }))

        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend

        private_key = serialization.load_pem_private_key(
            GOOGLE_CREDS["private_key"].encode(),
            password=None,
            backend=default_backend()
        )
        sig_input = f"{header}.{payload}".encode()
        signature = private_key.sign(sig_input, padding.PKCS1v15(), hashes.SHA256())
        jwt_token = f"{header}.{payload}.{b64(signature)}"

        data = urllib.parse.urlencode({
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_token
        }).encode()
        req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            _sheets_token = result["access_token"]
            _sheets_token_expiry = ahora + result.get("expires_in", 3600)
            return _sheets_token
    except Exception as e:
        print(f"[SHEETS TOKEN ERROR] {e}")
        return None

def sheets_append(fila):
    """Agrega una fila al Google Sheet."""
    try:
        token = get_sheets_token()
        if not token:
            return
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/Sheet1!A1:append?valueInputOption=RAW"
        payload = json.dumps({"values": [fila]}).encode()
        req = __import__("urllib.request", fromlist=["Request"]).Request(
            url, data=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
        )
        with __import__("urllib.request", fromlist=["urlopen"]).urlopen(req) as resp:
            print(f"[SHEETS OK] Fila agregada")
    except Exception as e:
        print(f"[SHEETS ERROR] {e}")

def sheets_update_resultado(fixture_id, resultado_final, gol_89):
    """Busca la fila del partido y actualiza resultado y acierto."""
    try:
        token = get_sheets_token()
        if not token:
            return
        import urllib.request
        # Leer todas las filas
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/Sheet1!A:L"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
        rows = data.get("values", [])
        # Buscar fila con fixture_id (columna L si la guardamos, o buscar por partido)
        for i, row in enumerate(rows[1:], start=2):  # skip header
            # fixture_id está en columna L (índice 11)
            if len(row) > 11 and str(fixture_id) == str(row[11]):
                acertado = "SI" if gol_89 else "NO"
                range_upd = f"Sheet1!I{i}:K{i}"
                url2 = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{range_upd}?valueInputOption=RAW"
                payload = json.dumps({"values": [[resultado_final, "SI" if gol_89 else "NO", acertado]]}).encode()
                req2 = urllib.request.Request(
                    url2, data=payload,
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    method="PUT"
                )
                with urllib.request.urlopen(req2) as resp2:
                    print(f"[SHEETS OK] Resultado actualizado: {resultado_final} gol89={gol_89}")
                break
    except Exception as e:
        print(f"[SHEETS UPDATE ERROR] {e}")

# ============================================================
# CONFIGURACIÓN
# ============================================================
API_KEY          = "7aa252fd9c63236a40e473bb6d518319"  # api-sports.io (PRO, 7500 req/día)
RAPIDAPI_KEY     = "caedee4c5dmshd0541b53eb081a0p1ad27djsn100a066c5efa"  # RapidAPI (backup)
TELEGRAM_TOKEN   = "8760870045:AAHXGZJGXHTsuLukgWYnVv34bLDZd21RZA4"
TELEGRAM_CHAT_ID = "1491964944"

INTERVALO_NORMAL  = 60   # segundos normalmente
INTERVALO_URGENTE = 30   # segundos cuando hay partido en min 84+
MINUTO_URGENTE    = 84

SEASON = 2025

PROMEDIO_GOLES_TARDIOS = {
    "Premier League":         2.5,
    "Championship":           2.0,
    "Egyptian Premier League": 2.0,
    "La Liga":                2.5,
    "Serie A":                2.5,
    "Bundesliga":             2.0,
    "Ligue 1":                2.0,
    "Eredivisie":             2.5,
    "Primeira Liga":          2.0,
    "Super League":           2.0,
    "Pro League":             2.0,
    "Superliga":              2.0,
    "Eliteserien":            2.0,
    "Primera División":       2.0,
    "Liga 1":                 2.0,
    "Liga MX":                2.5,
    "Série A":                2.5,
    "Ekstraklasa":            2.0,
    "Saudi Pro League":       2.5,
    "Israeli Premier League": 2.0,
    "DEFAULT":                2.0,
}

LIGAS = {
    39:  "Premier League",
    40:  "Championship",           # Inglaterra 2da división
    233: "Egyptian Premier League",
    140: "La Liga",
    135: "Serie A",
    78:  "Bundesliga",
    61:  "Ligue 1",
    88:  "Eredivisie",
    94:  "Primeira Liga",
    207: "Super League",
    144: "Pro League",
    119: "Superliga",
    103: "Eliteserien",
    265: "Primera División",
    242: "Liga 1",
    262: "Liga MX",
    71:  "Série A",
    347: "Primera División",
    106: "Ekstraklasa",
    307: "Saudi Pro League",
    384: "Israeli Premier League",
}

# ============================================================
# ESTADO GLOBAL
# ============================================================
# ============================================================
# ESTADO GLOBAL
# ============================================================
jornadas         = defaultdict(lambda: defaultdict(lambda: {"goles_89": 0, "terminados": 0, "total": 0}))
# Archivo para persistir alertas entre reinicios de Railway
ALERTAS_FILE = "/tmp/alertas_enviadas.txt"

# Archivo para registrar alertas del día con resultado final
REGISTRO_FILE = "/tmp/registro_alertas.json"

def cargar_registro():
    try:
        import json
        with open(REGISTRO_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def guardar_registro(registro):
    try:
        import json
        with open(REGISTRO_FILE, "w") as f:
            json.dump(registro, f)
    except Exception as e:
        print(f"[REGISTRO ERROR] {e}")

def registrar_alerta(fixture_id, liga, local, visita, minuto, marcador, nivel, puntos, criterios):
    """Registra una alerta en memoria Y en Google Sheets."""
    registro = cargar_registro()
    clave = str(fixture_id)
    if clave not in registro:
        registro[clave] = {
            "liga": liga,
            "partido": f"{local} vs {visita}",
            "minuto_alerta": minuto,
            "marcador_alerta": marcador,
            "nivel": nivel,
            "puntos": puntos,
            "gol_tardio": None,
            "resultado_final": None,
            "fecha": datetime.now().strftime("%Y-%m-%d"),
            "fila_sheet": None  # Se guarda para actualizar después
        }
        guardar_registro(registro)
        # Escribir fila en Google Sheets
        fila = [
            datetime.now().strftime("%Y-%m-%d %H:%M"),  # A: fecha
            liga,                                         # B: liga
            f"{local} vs {visita}",                       # C: partido
            minuto,                                       # D: minuto_alerta
            marcador,                                     # E: marcador_alerta
            nivel,                                        # F: nivel
            puntos,                                       # G: puntos
            criterios,                                    # H: criterios
            "",                                           # I: resultado_final
            "",                                           # J: gol_89
            "",                                           # K: acertado
            str(fixture_id)                               # L: fixture_id (oculto, para update)
        ]
        sheets_append(fila)

def actualizar_resultados(partidos_terminados):
    """Actualiza el resultado final de partidos alertados que ya terminaron."""
    registro = cargar_registro()
    actualizado = False
    for p in partidos_terminados:
        fid = str(p["fixture"]["id"])
        if fid in registro and registro[fid]["resultado_final"] is None:
            g_h = p["goals"]["home"] or 0
            g_a = p["goals"]["away"] or 0
            # Verificar si hubo gol en 89+
            eventos = obtener_eventos_partido(p["fixture"]["id"])
            gol_tardio = False
            for ev in eventos:
                if ev.get("type") == "Goal" and ev.get("detail") != "Missed Penalty":
                    min_ev = ev.get("time", {}).get("elapsed", 0) + (ev.get("time", {}).get("extra", 0) or 0)
                    if min_ev >= 89:
                        gol_tardio = True
                        break
            resultado_str = f"{g_h}-{g_a}"
            registro[fid]["resultado_final"] = resultado_str
            registro[fid]["gol_tardio"] = gol_tardio
            actualizado = True
            sheets_update_resultado(int(fid), resultado_str, gol_tardio)
    if actualizado:
        guardar_registro(registro)

def enviar_informe_diario():
    """Envía resumen diario de alertas y su efectividad."""
    registro = cargar_registro()
    hoy = datetime.now().strftime("%Y-%m-%d")
    alertas_hoy = {k: v for k, v in registro.items() if v.get("fecha") == hoy}

    if not alertas_hoy:
        return

    total      = len(alertas_hoy)
    con_result = [v for v in alertas_hoy.values() if v["resultado_final"] is not None]
    aciertos   = [v for v in con_result if v["gol_tardio"]]

    pct = round(len(aciertos) / len(con_result) * 100) if con_result else 0

    lineas = [f"📊 <b>INFORME DIARIO — {hoy}</b>\n"]
    lineas.append(f"Total alertas: {total} | Con resultado: {len(con_result)} | Aciertos: {len(aciertos)} ({pct}%)\n")

    for v in alertas_hoy.values():
        if v["resultado_final"]:
            icono = "✅" if v["gol_tardio"] else "❌"
            lineas.append(f"{icono} {v['liga']} — {v['partido']}")
            lineas.append(f"   Alerta min {v['minuto_alerta']} ({v['marcador_alerta']}) → Final: {v['resultado_final']}")
        else:
            lineas.append(f"⏳ {v['liga']} — {v['partido']} (sin resultado aún)")

    enviar_telegram("\n".join(lineas))

    # Limpiar registro de días anteriores
    registro_limpio = {k: v for k, v in registro.items() if v.get("fecha") == hoy}
    guardar_registro(registro_limpio)


def cargar_alertas():
    try:
        with open(ALERTAS_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except:
        return set()

def guardar_alerta(clave):
    try:
        with open(ALERTAS_FILE, "a") as f:
            f.write(clave + "\n")
    except Exception as e:
        print(f"[ALERTA FILE ERROR] {e}")

alertas_enviadas = cargar_alertas()
cache_posiciones = {}




# ============================================================
# API
# ============================================================
def api_get(endpoint, params={}):
    # Intenta múltiples dominios de api-sports.io hasta que uno funcione
    dominios = [
        "https://v3.football.api-sports.io",
        "https://v3.api-sports.io",
        "https://api-football-v1.p.rapidapi.com/v3",
    ]
    for dominio in dominios:
        if "rapidapi" in dominio:
            headers = {
                "x-rapidapi-key": RAPIDAPI_KEY,
                "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
            }
        else:
            headers = {"x-apisports-key": API_KEY}
        try:
            r = requests.get(f"{dominio}/{endpoint}", headers=headers, params=params, timeout=10)
            data = r.json()
            if "response" in data:
                print(f"[API OK] usando {dominio}")
                return data["response"]
        except Exception as e:
            print(f"[API FALLO] {dominio} — {e}")
            continue
    return []


def obtener_partidos_en_vivo():
    response = api_get("fixtures", {"live": "all"})
    return [f for f in response if f["league"]["id"] in LIGAS]


def obtener_partidos_ronda(liga_id, ronda):
    return api_get("fixtures", {"league": liga_id, "season": SEASON, "round": ronda})


def obtener_eventos_partido(fixture_id):
    """Obtiene los eventos (goles) de un partido — siempre frescos, sin cache."""
    return api_get("fixtures/events", {"fixture": fixture_id})


def obtener_estadisticas_partido(fixture_id):
    """Obtiene estadísticas en vivo de un partido (posesión, tiros, córners).
    La API devuelve siempre [local, visita] en ese orden."""
    response = api_get("fixtures/statistics", {"fixture": fixture_id})
    stats = {}
    lados = ["home", "away"]
    for i, equipo in enumerate(response[:2]):
        lado = lados[i]
        datos = {}
        for stat in equipo.get("statistics", []):
            datos[stat["type"]] = stat["value"]
        stats[lado] = datos
    return stats


def obtener_posiciones(liga_id):
    response = api_get("standings", {"league": liga_id, "season": SEASON})
    posiciones = {}
    try:
        standings = response[0]["league"]["standings"][0]
        for equipo in standings:
            posiciones[equipo["team"]["id"]] = equipo["rank"]
    except:
        pass
    return posiciones


def get_posicion(liga_id, equipo_id):
    if liga_id not in cache_posiciones:
        cache_posiciones[liga_id] = obtener_posiciones(liga_id)
    return cache_posiciones[liga_id].get(equipo_id)


# ============================================================
# TELEGRAM
# ============================================================
def enviar_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": mensaje, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            print(f"[TELEGRAM OK] Mensaje enviado")
        else:
            print(f"[TELEGRAM ERROR] {r.text}")
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")


# ============================================================
# JORNADAS
# ============================================================
def actualizar_jornada(liga_id, ronda):
    partidos = obtener_partidos_ronda(liga_id, ronda)
    if not partidos:
        return

    total      = len(partidos)
    terminados = 0
    goles_89   = 0
    btts       = 0

    detalle_goles_89 = []  # lista de strings "Local X-Y Visita (min Z')"
    for p in partidos:
        if p["fixture"]["status"]["short"] in ["FT", "AET", "PEN"]:
            terminados += 1
            goles_local  = p["goals"]["home"] or 0
            goles_visita = p["goals"]["away"] or 0
            nombre_local  = p["teams"]["home"]["name"]
            nombre_visita = p["teams"]["away"]["name"]
            if goles_local >= 1 and goles_visita >= 1:
                btts += 1
            for ev in obtener_eventos_partido(p["fixture"]["id"]):
                if ev.get("type") == "Goal" and ev.get("detail") != "Missed Penalty":
                    min_ev = ev.get("time", {}).get("elapsed", 0) + (ev.get("time", {}).get("extra", 0) or 0)
                    if min_ev >= 89:
                        goles_89 += 1
                        extra = ev.get("time", {}).get("extra", 0) or 0
                        min_txt = f"90'+{extra}" if extra else f"{min_ev}'"
                        detalle_goles_89.append(f"{nombre_local} {goles_local}-{goles_visita} {nombre_visita} ({min_txt})")

    # Criterio 9 — Marcador dominante (normalizado: siempre mayor-menor)
    from collections import Counter
    marcadores = []
    for p in partidos:
        if p["fixture"]["status"]["short"] in ["FT", "AET", "PEN"]:
            g_h = p["goals"]["home"] or 0
            g_a = p["goals"]["away"] or 0
            marcador_norm = (max(g_h, g_a), min(g_h, g_a))
            marcadores.append(marcador_norm)
    conteo_marcadores = Counter(marcadores)
    marcador_dominante = None
    max_repeticiones = 0
    for marcador, count in conteo_marcadores.items():
        if count > max_repeticiones:
            max_repeticiones = count
            marcador_dominante = marcador
    # Solo es dominante si supera el 40% de partidos terminados
    if marcador_dominante and max_repeticiones / max(terminados, 1) < 0.4:
        marcador_dominante = None
        max_repeticiones = 0

    # Criterio 10 — Techo de goles (resistencia)
    techo_goles = 0
    partidos_en_techo = 0
    techo_roto = False
    if terminados > 0:
        totales = []
        for p in partidos:
            if p["fixture"]["status"]["short"] in ["FT", "AET", "PEN"]:
                g_h = p["goals"]["home"] or 0
                g_a = p["goals"]["away"] or 0
                totales.append(g_h + g_a)
        if totales:
            techo_goles = max(totales)
            partidos_en_techo = sum(1 for t in totales if t == techo_goles)

    jornadas[liga_id][ronda] = {
        "goles_89":           goles_89,
        "detalle_goles_89":   detalle_goles_89,
        "terminados":         terminados,
        "total":              total,
        "btts":               btts,
        "marcador_dominante": marcador_dominante,
        "rep_marcador":       max_repeticiones,
        "techo_goles":        techo_goles,
        "partidos_en_techo":  partidos_en_techo,
    }
    liga_nombre = LIGAS.get(liga_id, liga_id)
    print(f"  → [{liga_nombre}] {ronda}: {terminados}/{total} terminados, {goles_89} goles en 89+")


# ============================================================
# SCORING
# ============================================================
def contar_goles_entre(fixture_id, min_inicio, min_fin):
    count = 0
    for ev in obtener_eventos_partido(fixture_id):
        if ev.get("type") == "Goal" and ev.get("detail") != "Missed Penalty":
            min_ev = ev.get("time", {}).get("elapsed", 0) + (ev.get("time", {}).get("extra", 0) or 0)
            if min_inicio <= min_ev <= min_fin:
                count += 1
    return count


def calcular_alerta(fixture_id, liga_id, minuto, goles_local, goles_visita, pos_local, pos_visita, datos_jornada, partidos_simultaneos=1):
    promedio   = PROMEDIO_GOLES_TARDIOS.get(LIGAS.get(liga_id, ""), PROMEDIO_GOLES_TARDIOS["DEFAULT"])
    goles_89   = datos_jornada["goles_89"]
    terminados = datos_jornada["terminados"]
    total      = datos_jornada["total"]
    restantes  = total - terminados

    diferencia_pos   = abs(pos_local - pos_visita) if pos_local and pos_visita else None
    partido_parejo   = diferencia_pos is not None and diferencia_pos <= 8
    partido_desigual = diferencia_pos is not None and diferencia_pos >= 10

    puntos  = 0
    motivos = []

    # Criterio 1 — Deuda de jornada: 2 pts (solo últimos 1-3 partidos de la ronda)
    deuda = promedio - goles_89
    if deuda >= promedio and 1 <= restantes <= 3 and terminados > 0:
        puntos += 2
        motivos.append(f"⚠️ <b>Deuda jornada</b>: {goles_89} goles en 89+ en {terminados} partidos — esperado {promedio:.0f} (quedan {restantes})")

    # Criterio 2 — Momentum: 1 pt (gol entre min 75-88)
    if minuto >= 75:
        goles_momentum = contar_goles_entre(fixture_id, 75, 88)
        if goles_momentum >= 1:
            puntos += 1
            motivos.append(f"⚡ <b>Momentum</b>: {goles_momentum} gol(es) entre min 75-88")

    # Criterio 3 — Over frustrado: 1 pt
    if partido_desigual and (goles_local + goles_visita) == 0 and minuto >= 85:
        puntos += 1
        motivos.append(f"💥 <b>Over frustrado</b>: dif. {diferencia_pos} puestos y 0-0 al min {minuto}")

    # Criterio 4 — Partido parejo despertó: 1 pt
    if partido_parejo and minuto >= 80:
        goles_60_80 = contar_goles_entre(fixture_id, 60, 80)
        if goles_60_80 >= 1:
            puntos += 1
            motivos.append(f"🔄 <b>Partido parejo despertó</b>: dif. {diferencia_pos} puestos, gol entre min 60-80")

    # Criterio 7 — Partidos simultáneos en la misma liga: 1 pt
    # Cuando 2+ partidos empezaron a la misma hora, hay presión extra por resultados paralelos
    if partidos_simultaneos >= 2:
        puntos += 1
        motivos.append(f"⏰ <b>Jornada simultánea</b>: {partidos_simultaneos} partidos a la misma hora — presión por resultados paralelos")

    # Criterio 5 — Local perdiendo: 1 pt
    if minuto >= 80 and goles_local < goles_visita:
        puntos += 1
        motivos.append(f"🏠 <b>Local perdiendo</b>: {goles_local}-{goles_visita} — presión máxima")

    # Criterio 6 — Dominio estadístico: 1 pt
    # Tiros al arco 6+ (obligatorio) + posesión 60%+ O córners 6+
    if minuto >= 80:
        try:
            stats = obtener_estadisticas_partido(fixture_id)
            home_stats = stats.get("home", {})

            tiros_arco  = int(home_stats.get("Shots on Goal") or 0)
            corners     = int(home_stats.get("Corner Kicks") or 0)
            posesion_raw = home_stats.get("Ball Possession") or "0%"
            posesion    = int(str(posesion_raw).replace("%", "").strip() or 0)

            tiros_ok   = tiros_arco >= 6
            posesion_ok = posesion >= 60
            corners_ok  = corners >= 6

            if tiros_ok and (posesion_ok or corners_ok):
                puntos += 1
                detalles = f"tiros al arco: {tiros_arco}"
                if posesion_ok:
                    detalles += f", posesión: {posesion}%"
                if corners_ok:
                    detalles += f", córners: {corners}"
                motivos.append(f"📊 <b>Dominio estadístico</b>: {detalles}")
        except Exception as e:
            print(f"[STATS ERROR] {e}")

    # Criterio 8 — Deuda BTTS: 1 pt
    # Si BTTS reales < 25% de partidos terminados y quedan 1-3 partidos
    btts           = datos_jornada.get("btts", 0)
    btts_esperados = terminados * 0.25
    if btts < btts_esperados and 1 <= restantes <= 3 and terminados > 0:
        puntos += 2
        motivos.append(f"🎯 <b>Deuda BTTS</b>: {btts} de {terminados} partidos con ambos equipos anotando (esperado {btts_esperados:.1f})")

    # Criterio 9 — Marcador dominante de jornada: 1 pt
    # Si 50%+ partidos terminados tienen el mismo marcador (normalizado)
    # y el partido en vivo replica ese marcador en min 80+
    marcador_dominante = datos_jornada.get("marcador_dominante")
    rep_marcador       = datos_jornada.get("rep_marcador", 0)
    if marcador_dominante and minuto >= 80 and terminados > 0:
        marcador_vivo_norm = (max(goles_local, goles_visita), min(goles_local, goles_visita))
        if marcador_vivo_norm == marcador_dominante and rep_marcador >= 2:
            puntos += 1
            motivos.append(
                f"🔁 <b>Marcador dominante</b>: {marcador_dominante[0]}-{marcador_dominante[1]} "
                f"se repite en {rep_marcador} partidos de la jornada — alta probabilidad de variación"
            )

    # Criterio 10 — Techo de goles (resistencia): 1-2 pts
    # Solo ligas con 7+ partidos por jornada
    # Nadie ha roto el techo aún (ni terminados ni en vivo)
    techo_goles       = datos_jornada.get("techo_goles", 0)
    partidos_en_techo = datos_jornada.get("partidos_en_techo", 0)
    if (total >= 7 and techo_goles > 0 and minuto >= 80
            and terminados >= total * 0.5
            and partidos_en_techo >= 3):

        goles_vivo = goles_local + goles_visita

        # Verificar si algún partido en vivo ya rompió el techo (invalida criterio)
        techo_roto_en_vivo = False
        # (se pasa desde el loop principal via datos_jornada)
        techo_roto_en_vivo = datos_jornada.get("techo_roto_en_vivo", False)

        if not techo_roto_en_vivo and goles_vivo == techo_goles:
            if partidos_en_techo >= 4:
                pts_techo = 2
            else:
                pts_techo = 1
            puntos += pts_techo
            motivos.append(
                f"🧱 <b>Resistencia de goles</b>: {partidos_en_techo} partidos tocaron el techo de "
                f"{techo_goles} goles — alta presión para romperlo (+{pts_techo}pts)"
            )

    if puntos < 2:
        return 0, None, []

    if puntos >= 4:
        nivel = "TOTAL"
    elif puntos == 3:
        nivel = "MÁXIMA"
    else:
        nivel = "ALERT"

    return puntos, nivel, motivos


# ============================================================
# MENSAJE
# ============================================================
def construir_mensaje(fixture, liga_id, puntos, nivel, motivos, datos_jornada, pos_local, pos_visita):
    liga_nombre  = LIGAS.get(liga_id, "Liga desconocida")
    local        = fixture["teams"]["home"]["name"]
    visita       = fixture["teams"]["away"]["name"]
    goles_local  = fixture["goals"]["home"] or 0
    goles_visita = fixture["goals"]["away"] or 0
    minuto       = fixture["fixture"]["status"]["elapsed"] or 0
    ronda        = fixture["league"]["round"]
    goles_89     = datos_jornada["goles_89"]
    terminados   = datos_jornada["terminados"]
    total        = datos_jornada["total"]
    promedio     = PROMEDIO_GOLES_TARDIOS.get(liga_nombre, PROMEDIO_GOLES_TARDIOS["DEFAULT"])

    iconos = {"ALERT": "🟠", "MÁXIMA": "🔴", "TOTAL": "🚨"}
    icono  = iconos.get(nivel, "⚪")

    pos_txt = ""
    if pos_local and pos_visita:
        pos_txt = f"\n📋 Tabla: <b>{local} #{pos_local}</b> vs <b>{visita} #{pos_visita}</b>"

    motivos_txt = "\n".join(f"• {m}" for m in motivos)

    detalle_89 = datos_jornada.get("detalle_goles_89", [])
    detalle_89_txt = ""
    if detalle_89:
        detalle_89_txt = "\n" + "\n".join(f"  🕐 {d}" for d in detalle_89)

    return f"""{icono} <b>ALERTA {nivel} [{puntos} pts] — {liga_nombre}</b>
⚽ <b>{local} {goles_local}-{goles_visita} {visita}</b>
⏱ Minuto: <b>{minuto}'</b>{pos_txt}

<b>📊 {ronda} — Goles en 89+:</b>
• Terminados: {terminados} de {total} | Tardíos: <b>{goles_89}</b>
• Promedio esperado: {promedio:.0f} por jornada{detalle_89_txt}

<b>🎯 Criterios activos:</b>
{motivos_txt}

⏰ {datetime.now().strftime("%H:%M:%S")}""".strip()


# ============================================================
# LOOP PRINCIPAL
# ============================================================
def main():
    print("=" * 60)
    print("⚽ GOL ALERT SYSTEM — Iniciando...")
    print(f"🏆 Ligas: {len(LIGAS)} | Normal: {INTERVALO_NORMAL}s | Urgente (min {MINUTO_URGENTE}+): {INTERVALO_URGENTE}s")
    print("=" * 60)

    enviar_telegram(
        f"⚽ <b>Gol Alert System iniciado</b>\n"
        f"Monitoreando {len(LIGAS)} ligas en tiempo real\n"
        f"⏱ {INTERVALO_NORMAL}s normal / {INTERVALO_URGENTE}s en min {MINUTO_URGENTE}+"
    )

    ciclo = 0
    ultima_actualizacion_jornada = 0
    ultimo_informe_dia = ""

    while True:
        ciclo += 1
        ahora = time.time()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Ciclo #{ciclo}")

        partidos_vivos = obtener_partidos_en_vivo()
        print(f"  → {len(partidos_vivos)} partidos en vivo")

        # Actualizar jornadas:
        # - Cada 10 minutos para todas las ligas en vivo
        # - Cada ciclo para ligas con partidos en min 80+ (datos críticos)
        rondas_vistas = set()
        for fixture in partidos_vivos:
            liga_id = fixture["league"]["id"]
            ronda   = fixture["league"]["round"]
            minuto_f = fixture["fixture"]["status"]["elapsed"] or 0
            clave_ronda = (liga_id, ronda)

            # Siempre actualizar si hay partido en min 80+
            if minuto_f >= 80 and clave_ronda not in rondas_vistas:
                actualizar_jornada(liga_id, ronda)
                rondas_vistas.add(clave_ronda)
            # Actualizar el resto cada 10 minutos
            elif ahora - ultima_actualizacion_jornada > 600 and clave_ronda not in rondas_vistas:
                actualizar_jornada(liga_id, ronda)
                rondas_vistas.add(clave_ronda)

        if ahora - ultima_actualizacion_jornada > 600:
            ultima_actualizacion_jornada = ahora

        # Detectar si hay partido en minuto crítico → intervalo urgente
        hay_urgente = any(
            (f["fixture"]["status"]["elapsed"] or 0) >= MINUTO_URGENTE
            for f in partidos_vivos
        )
        intervalo = INTERVALO_URGENTE if hay_urgente else INTERVALO_NORMAL
        if hay_urgente:
            print(f"  ⚡ Partido en min {MINUTO_URGENTE}+ — intervalo reducido a {INTERVALO_URGENTE}s")

        # Detectar si algún partido en vivo ya rompió el techo de goles
        for f in partidos_vivos:
            lid   = f["league"]["id"]
            ronda = f["league"]["round"]
            techo = jornadas[lid][ronda].get("techo_goles", 0)
            if techo > 0:
                g_vivo = (f["goals"]["home"] or 0) + (f["goals"]["away"] or 0)
                if g_vivo > techo:
                    jornadas[lid][ronda]["techo_roto_en_vivo"] = True

        # Calcular partidos simultáneos por liga usando la ronda completa
        # (no solo los partidos en vivo — incluye los ya terminados de la misma ronda)
        from collections import Counter
        simultaneos_por_liga = {}
        for f in partidos_vivos:
            lid   = f["league"]["id"]
            ronda = f["league"]["round"]
            if lid in simultaneos_por_liga:
                continue
            # Usar todos los partidos de la ronda (terminados + en vivo)
            todos = obtener_partidos_ronda(lid, ronda)
            horas = [p["fixture"].get("date", "")[:16] for p in todos]
            if horas:
                conteo = Counter(horas)
                simultaneos_por_liga[lid] = max(conteo.values())
            else:
                simultaneos_por_liga[lid] = 1

        # Evaluar cada partido desde min 80
        for fixture in partidos_vivos:
            liga_id    = fixture["league"]["id"]
            fixture_id = fixture["fixture"]["id"]
            minuto     = fixture["fixture"]["status"]["elapsed"] or 0
            ronda      = fixture["league"]["round"]

            if minuto < 80:
                continue

            goles_local  = fixture["goals"]["home"] or 0
            goles_visita = fixture["goals"]["away"] or 0
            pos_local    = get_posicion(liga_id, fixture["teams"]["home"]["id"])
            pos_visita   = get_posicion(liga_id, fixture["teams"]["away"]["id"])
            datos_jornada = jornadas[liga_id].get(ronda, {"goles_89": 0, "terminados": 0, "total": 0})

            puntos, nivel, motivos = calcular_alerta(
                fixture_id, liga_id, minuto,
                goles_local, goles_visita,
                pos_local, pos_visita,
                datos_jornada,
                partidos_simultaneos=simultaneos_por_liga.get(liga_id, 1)
            )

            if nivel is None:
                continue

            # Una alerta por partido por nivel — solo re-alerta si SUBE de nivel
            clave = f"{fixture_id}_{nivel}"
            if clave in alertas_enviadas:
                continue
            # Marcar ANTES de enviar para evitar doble envío en ciclos rápidos
            alertas_enviadas.add(clave)
            guardar_alerta(clave)

            local  = fixture["teams"]["home"]["name"]
            visita = fixture["teams"]["away"]["name"]
            print(f"\n  {nivel} [{puntos}pts]: {local} vs {visita} (min {minuto})")

            mensaje = construir_mensaje(
                fixture, liga_id, puntos, nivel, motivos,
                datos_jornada, pos_local, pos_visita
            )
            enviar_telegram(mensaje)
            criterios_txt = " | ".join([m.replace("<b>","").replace("</b>","") for m in motivos])
            registrar_alerta(
                fixture_id, LIGAS.get(liga_id, ""), local, visita,
                minuto, f"{goles_local}-{goles_visita}", nivel, puntos, criterios_txt
            )

        # Actualizar resultados de partidos alertados que aún no tienen resultado
        # Consulta directamente por fixture_id — no depende de partidos_vivos
        registro_actual = cargar_registro()
        pendientes = [fid for fid, v in registro_actual.items() if v["resultado_final"] is None]
        if pendientes:
            for fid in pendientes:
                datos_fixture = api_get("fixtures", {"id": fid})
                if datos_fixture:
                    p = datos_fixture[0]
                    if p["fixture"]["status"]["short"] in ["FT", "AET", "PEN"]:
                        actualizar_resultados([p])

        # Informe diario a las 23:59 hora Lima (UTC-5) — solo una vez por día
        hora_lima = (datetime.utcnow().hour - 5) % 24
        minuto_actual = datetime.utcnow().minute
        hoy_str = datetime.utcnow().strftime("%Y-%m-%d")
        if hora_lima == 23 and minuto_actual == 59 and ultimo_informe_dia != hoy_str:
            enviar_informe_diario()
            ultimo_informe_dia = hoy_str

        if len(alertas_enviadas) > 2000:
            alertas_enviadas.clear()
            try:
                open(ALERTAS_FILE, "w").close()
            except:
                pass

        time.sleep(intervalo)


if __name__ == "__main__":
    main()
