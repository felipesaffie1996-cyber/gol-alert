"""
⚽ GOL ALERT SYSTEM — Monitor de goles en minuto 89+
Autor: Sistema personalizado
Descripción: Monitorea partidos en vivo en 19 ligas y envía alertas
             por Telegram cuando hay alta probabilidad de gol tardío.
"""

import requests
import time
import json
import os
from datetime import datetime, date
from collections import defaultdict

# ============================================================
# CONFIGURACIÓN — Edita estos valores
# ============================================================
API_KEY = "7aa252fd9c63236a40e473bb6d518319"  # API key de api-sports.io
RAPIDAPI_KEY = "caedee4c5dmshd0541b53eb081a0p1ad27djsn100a066c5efa"  # RapidAPI key (backup)
TELEGRAM_TOKEN = "8760870045:AAHXGZJGXHTsuLukgWYnVv34bLDZd21RZA4"
TELEGRAM_CHAT_ID = "1491964944"

# Intervalo de consulta en segundos (cada 60 seg = ~1440 requests/día)
INTERVALO_SEGUNDOS = 60

# Promedio histórico de goles en min 89+ por jornada (por liga)
# Ajusta estos valores según tu investigación
PROMEDIO_GOLES_TARDIOS = {
    "Premier League":       2.5,
    "La Liga":              2.5,
    "Serie A":              2.5,
    "Bundesliga":           2.0,
    "Ligue 1":              2.0,
    "Eredivisie":           2.5,
    "Primeira Liga":        2.0,
    "Super League":         2.0,  # Suiza
    "Pro League":           2.0,  # Bélgica
    "Superliga":            2.0,  # Dinamarca
    "Eliteserien":          2.0,  # Noruega
    "Primera División":     2.0,  # Chile / Paraguay
    "Liga 1":               2.0,  # Perú
    "Liga MX":              2.5,  # México
    "Série A":              2.5,  # Brasil
    "Ekstraklasa":          2.0,  # Polonia
    "Saudi Pro League":     2.5,  # Arabia Saudita
    "Israeli Premier League": 2.0,
    "DEFAULT":              2.0,
}

# IDs de ligas en API-Football
LIGAS = {
    39:  "Premier League",       # Inglaterra
    140: "La Liga",              # España
    135: "Serie A",              # Italia
    78:  "Bundesliga",           # Alemania
    61:  "Ligue 1",              # Francia
    88:  "Eredivisie",           # Holanda
    94:  "Primeira Liga",        # Portugal
    207: "Super League",         # Suiza
    144: "Pro League",           # Bélgica
    119: "Superliga",            # Dinamarca
    103: "Eliteserien",          # Noruega
    265: "Primera División",     # Chile
    242: "Liga 1",               # Perú
    262: "Liga MX",              # México
    71:  "Série A",              # Brasil
    347: "Primera División",     # Paraguay
    106: "Ekstraklasa",          # Polonia
    307: "Saudi Pro League",     # Arabia Saudita
    384: "Israeli Premier League", # Israel
}

# ============================================================
# ESTADO GLOBAL (en memoria mientras el script corre)
# ============================================================
# Estructura: { liga_id: { fixture_id: { datos del partido } } }
estado_partidos = defaultdict(dict)

# Contador de goles tardíos por jornada/liga
# Estructura: { liga_id: { round: { "goles": N, "partidos_terminados": N, "total_partidos": N } } }
jornadas = defaultdict(lambda: defaultdict(lambda: {"goles_89": 0, "terminados": 0, "total": 0}))

# Alertas ya enviadas (para no repetir)
alertas_enviadas = set()


# ============================================================
# FUNCIONES DE API-FOOTBALL
# ============================================================
def api_get(endpoint, params={}):
    """Hace una llamada a API-Football via RapidAPI y retorna el JSON."""
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }
    url = f"https://api-football-v1.p.rapidapi.com/v3/{endpoint}"
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        data = r.json()
        return data.get("response", [])
    except Exception as e:
        print(f"[ERROR API] {e}")
        return []


def obtener_partidos_en_vivo():
    """Obtiene todos los partidos en vivo de las ligas configuradas."""
    response = api_get("fixtures", {"live": "all"})
    partidos = []
    for fixture in response:
        liga_id = fixture["league"]["id"]
        if liga_id in LIGAS:
            partidos.append(fixture)
    return partidos


def obtener_partidos_del_dia():
    """Obtiene todos los partidos de hoy (para calcular deuda de jornada)."""
    hoy = date.today().strftime("%Y-%m-%d")
    response = api_get("fixtures", {"date": hoy, "timezone": "America/Lima"})
    partidos = []
    for fixture in response:
        liga_id = fixture["league"]["id"]
        if liga_id in LIGAS:
            partidos.append(fixture)
    return partidos


def obtener_eventos_partido(fixture_id):
    """Obtiene los eventos (goles) de un partido específico."""
    return api_get("fixtures/events", {"fixture": fixture_id})


# ============================================================
# FUNCIONES DE TELEGRAM
# ============================================================
def enviar_telegram(mensaje):
    """Envía un mensaje por Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": mensaje,
        "parse_mode": "HTML"
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            print(f"[TELEGRAM ✓] Mensaje enviado")
        else:
            print(f"[TELEGRAM ERROR] {r.text}")
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")


# ============================================================
# LÓGICA DE ALERTAS
# ============================================================
def calcular_nivel_alerta(fixture_id, liga_id, minuto, marcador_local, marcador_visita, 
                           pos_local, pos_visita, total_equipos, datos_jornada):
    """
    Retorna el nivel de alerta y el motivo.
    Niveles: None, "WATCH", "ALERT", "MAXIMA"
    """
    goles_total = marcador_local + marcador_visita
    promedio_esperado = PROMEDIO_GOLES_TARDIOS.get(
        LIGAS.get(liga_id, ""), 
        PROMEDIO_GOLES_TARDIOS["DEFAULT"]
    )
    
    goles_tardios_jornada = datos_jornada["goles_89"]
    partidos_terminados = datos_jornada["terminados"]
    total_partidos = datos_jornada["total"]
    partidos_restantes = total_partidos - partidos_terminados
    
    # Calcular deuda
    deuda = promedio_esperado - goles_tardios_jornada
    porcentaje_jugado = partidos_terminados / max(total_partidos, 1)
    
    # Diferencia de posición en tabla (proximidad = partido parejo)
    diferencia_pos = abs(pos_local - pos_visita) if pos_local and pos_visita else None
    partido_parejo = diferencia_pos is not None and diferencia_pos <= 8
    partido_desigual = diferencia_pos is not None and diferencia_pos >= 10

    motivos = []
    nivel = None

    # --- CRITERIO PRINCIPAL: Deuda de jornada ---
    if deuda >= promedio_esperado and porcentaje_jugado >= 0.5:
        motivos.append(f"⚠️ Deuda MÁXIMA: {goles_tardios_jornada} goles tardíos en {partidos_terminados} partidos (esperado {promedio_esperado:.0f}-{promedio_esperado+0.5:.0f})")
        nivel = "MAXIMA"
    elif deuda > 0 and porcentaje_jugado >= 0.4:
        motivos.append(f"📊 Deuda activa: {goles_tardios_jornada} goles tardíos en {partidos_terminados} partidos")
        nivel = "ALERT"

    # --- CRITERIO C: Partido parejo que despertó ---
    if partido_parejo and goles_total == 1 and 60 <= minuto <= 80:
        motivos.append(f"🔄 Partido parejo (dif. {diferencia_pos} pos.) con gol entre min 60-80")
        nivel = max_nivel(nivel, "ALERT")

    # --- CRITERIO D: Over frustrado (top vs colista, 0-0 al 85+) ---
    if partido_desigual and goles_total == 0 and minuto >= 85:
        motivos.append(f"💥 Top vs débil (dif. {diferencia_pos} pos.) y 0-0 al min {minuto}")
        nivel = max_nivel(nivel, "MAXIMA")

    # Mínimo de minuto para alertar (solo a partir del min 80)
    if minuto < 80:
        return None, []

    return nivel, motivos


def max_nivel(actual, nuevo):
    """Retorna el nivel de alerta más alto."""
    orden = [None, "WATCH", "ALERT", "MAXIMA"]
    return nuevo if orden.index(nuevo) > orden.index(actual) else actual


def construir_mensaje(fixture, liga_id, nivel, motivos, datos_jornada, pos_local, pos_visita):
    """Construye el mensaje de Telegram."""
    liga_nombre = LIGAS.get(liga_id, "Liga desconocida")
    equipo_local = fixture["teams"]["home"]["name"]
    equipo_visita = fixture["teams"]["away"]["name"]
    goles_local = fixture["goals"]["home"] or 0
    goles_visita = fixture["goals"]["away"] or 0
    minuto = fixture["fixture"]["status"]["elapsed"] or 0
    ronda = fixture["league"]["round"]

    goles_89 = datos_jornada["goles_89"]
    terminados = datos_jornada["terminados"]
    total = datos_jornada["total"]
    promedio = PROMEDIO_GOLES_TARDIOS.get(liga_nombre, PROMEDIO_GOLES_TARDIOS["DEFAULT"])

    iconos = {"WATCH": "🟡", "ALERT": "🟠", "MAXIMA": "🔴"}
    icono = iconos.get(nivel, "⚪")

    pos_txt = ""
    if pos_local and pos_visita:
        pos_txt = f"\n📋 Posición en tabla: {equipo_local} #{pos_local} vs {equipo_visita} #{pos_visita}"

    mensaje = f"""
{icono} <b>ALERTA {nivel} — {liga_nombre}</b>
⚽ <b>{equipo_local} {goles_local}-{goles_visita} {equipo_visita}</b>
⏱ Minuto: <b>{minuto}'</b>
{pos_txt}

<b>📊 Jornada {ronda} — Goles en min 89+:</b>
• Partidos terminados: {terminados} de {total}
• Goles en min 89+: <b>{goles_89}</b>
• Promedio esperado esta liga: {promedio:.0f}-{promedio+0.5:.0f} por jornada
• Partidos restantes (incluyendo este): {total - terminados}

<b>🎯 Por qué entrar:</b>
{chr(10).join(motivos)}

⏰ {datetime.now().strftime("%H:%M:%S")}
""".strip()

    return mensaje


# ============================================================
# ACTUALIZAR ESTADO DE JORNADA
# ============================================================
def actualizar_jornadas(partidos_del_dia):
    """
    Actualiza el contador de goles tardíos por jornada/liga
    basándose en partidos ya terminados.
    """
    # Agrupar por liga y ronda
    por_liga_ronda = defaultdict(list)
    for p in partidos_del_dia:
        liga_id = p["league"]["id"]
        ronda = p["league"]["round"]
        por_liga_ronda[(liga_id, ronda)].append(p)

    for (liga_id, ronda), partidos in por_liga_ronda.items():
        total = len(partidos)
        terminados = 0
        goles_89 = 0

        for p in partidos:
            status = p["fixture"]["status"]["short"]
            if status in ["FT", "AET", "PEN"]:  # Partido terminado
                terminados += 1
                fixture_id = p["fixture"]["id"]
                # Obtener eventos para contar goles en 89+
                eventos = obtener_eventos_partido(fixture_id)
                for evento in eventos:
                    if evento.get("type") == "Goal" and evento.get("detail") != "Missed Penalty":
                        minuto_gol = evento.get("time", {}).get("elapsed", 0)
                        extra = evento.get("time", {}).get("extra", 0) or 0
                        minuto_real = minuto_gol + extra
                        if minuto_real >= 89:
                            goles_89 += 1

        jornadas[liga_id][ronda] = {
            "goles_89": goles_89,
            "terminados": terminados,
            "total": total,
            "ronda": ronda
        }


# ============================================================
# OBTENER POSICIONES EN TABLA
# ============================================================
def obtener_posiciones(liga_id, season=2024):
    """Obtiene las posiciones actuales en la tabla de la liga."""
    response = api_get("standings", {"league": liga_id, "season": season})
    posiciones = {}
    try:
        standings = response[0]["league"]["standings"][0]
        for equipo in standings:
            posiciones[equipo["team"]["id"]] = equipo["rank"]
    except:
        pass
    return posiciones


# Cache de posiciones (se actualiza una vez por sesión)
cache_posiciones = {}

def get_posicion(liga_id, equipo_id):
    if liga_id not in cache_posiciones:
        cache_posiciones[liga_id] = obtener_posiciones(liga_id)
    return cache_posiciones[liga_id].get(equipo_id)


# ============================================================
# LOOP PRINCIPAL
# ============================================================
def main():
    print("=" * 60)
    print("⚽ GOL ALERT SYSTEM — Iniciando...")
    print(f"🕐 Intervalo de consulta: {INTERVALO_SEGUNDOS} segundos")
    print(f"🏆 Ligas monitoreadas: {len(LIGAS)}")
    print("=" * 60)

    enviar_telegram("⚽ <b>Gol Alert System iniciado</b>\nMonitoreando 19 ligas en tiempo real...")

    ciclo = 0
    ultima_actualizacion_jornada = 0

    while True:
        ciclo += 1
        ahora = time.time()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Ciclo #{ciclo}")

        # Actualizar estado de jornada cada 10 minutos
        if ahora - ultima_actualizacion_jornada > 600:
            print("  → Actualizando estado de jornadas...")
            partidos_dia = obtener_partidos_del_dia()
            actualizar_jornadas(partidos_dia)
            ultima_actualizacion_jornada = ahora
            print(f"  → {len(partidos_dia)} partidos encontrados hoy")

        # Obtener partidos en vivo
        partidos_vivos = obtener_partidos_en_vivo()
        print(f"  → {len(partidos_vivos)} partidos en vivo")

        for fixture in partidos_vivos:
            liga_id = fixture["league"]["id"]
            fixture_id = fixture["fixture"]["id"]
            minuto = fixture["fixture"]["status"]["elapsed"] or 0
            ronda = fixture["league"]["round"]

            # Solo nos interesan partidos desde el minuto 80
            if minuto < 80:
                continue

            goles_local = fixture["goals"]["home"] or 0
            goles_visita = fixture["goals"]["away"] or 0
            equipo_local_id = fixture["teams"]["home"]["id"]
            equipo_visita_id = fixture["teams"]["away"]["id"]

            # Posiciones en tabla
            pos_local = get_posicion(liga_id, equipo_local_id)
            pos_visita = get_posicion(liga_id, equipo_visita_id)
            total_equipos = len(cache_posiciones.get(liga_id, {}))

            # Datos de jornada
            datos_jornada = jornadas[liga_id].get(ronda, {
                "goles_89": 0, "terminados": 0, "total": 0
            })

            # Calcular nivel de alerta
            nivel, motivos = calcular_nivel_alerta(
                fixture_id, liga_id, minuto,
                goles_local, goles_visita,
                pos_local, pos_visita, total_equipos,
                datos_jornada
            )

            if nivel != "MAXIMA":
                continue

            # Clave única para evitar alertas repetidas
            clave = f"{fixture_id}_{nivel}_{minuto // 5}"  # Cada 5 minutos puede re-alertar
            if clave in alertas_enviadas:
                continue

            # Construir y enviar alerta
            mensaje = construir_mensaje(
                fixture, liga_id, nivel, motivos,
                datos_jornada, pos_local, pos_visita
            )
            
            print(f"\n  🚨 ALERTA {nivel}: {fixture['teams']['home']['name']} vs {fixture['teams']['away']['name']} (min {minuto})")
            enviar_telegram(mensaje)
            alertas_enviadas.add(clave)

        # Limpiar alertas viejas (evitar memoria infinita)
        if len(alertas_enviadas) > 1000:
            alertas_enviadas.clear()

        time.sleep(INTERVALO_SEGUNDOS)


if __name__ == "__main__":
    main()
