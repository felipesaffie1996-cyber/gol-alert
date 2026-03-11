"""
⚽ GOL ALERT SYSTEM — Monitor de goles en minuto 89+
Descripción: Monitorea partidos en vivo en 19 ligas y envía alertas
             por Telegram cuando hay alta probabilidad de gol tardío.

SISTEMA DE PUNTOS:
  Criterio 1 — Deuda de jornada (últimos 2-3 partidos sin gol en 89+): 2 pts
  Criterio 2 — Momentum (gol entre min 80-88):                         1 pt
  Criterio 3 — Over frustrado (top vs colista, 0-0 al 85+):            1 pt
  Criterio 4 — Partido parejo con gol entre min 60-80:                 1 pt
  Criterio 5 — Local perdiendo al min 80+:                             1 pt

NIVELES DE ALERTA (mínimo 2 puntos para alertar):
  2 pts → ALERT
  3 pts → MÁXIMA
  4+ pts → TOTAL
"""

import requests
import time
from datetime import datetime
from collections import defaultdict

# ============================================================
# CONFIGURACIÓN
# ============================================================
RAPIDAPI_KEY     = "caedee4c5dmshd0541b53eb081a0p1ad27djsn100a066c5efa"
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
alertas_enviadas = set()
cache_posiciones = {}




# ============================================================
# API
# ============================================================
def api_get(endpoint, params={}):
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
    response = api_get("fixtures", {"live": "all"})
    return [f for f in response if f["league"]["id"] in LIGAS]


def obtener_partidos_ronda(liga_id, ronda):
    return api_get("fixtures", {"league": liga_id, "season": SEASON, "round": ronda})


def obtener_eventos_partido(fixture_id):
    """Obtiene los eventos (goles) de un partido — siempre frescos, sin cache."""
    return api_get("fixtures/events", {"fixture": fixture_id})


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

    for p in partidos:
        if p["fixture"]["status"]["short"] in ["FT", "AET", "PEN"]:
            terminados += 1
            for ev in obtener_eventos_partido(p["fixture"]["id"]):
                if ev.get("type") == "Goal" and ev.get("detail") != "Missed Penalty":
                    min_ev = ev.get("time", {}).get("elapsed", 0) + (ev.get("time", {}).get("extra", 0) or 0)
                    if min_ev >= 89:
                        goles_89 += 1

    jornadas[liga_id][ronda] = {"goles_89": goles_89, "terminados": terminados, "total": total}
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


def calcular_alerta(fixture_id, liga_id, minuto, goles_local, goles_visita, pos_local, pos_visita, datos_jornada):
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

    # Criterio 2 — Momentum: 1 pt (gol entre min 80-88)
    if minuto >= 80:
        goles_momentum = contar_goles_entre(fixture_id, 80, 88)
        if goles_momentum >= 1:
            puntos += 1
            motivos.append(f"⚡ <b>Momentum</b>: {goles_momentum} gol(es) entre min 80-88")

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

    # Criterio 5 — Local perdiendo: 1 pt
    if minuto >= 80 and goles_local < goles_visita:
        puntos += 1
        motivos.append(f"🏠 <b>Local perdiendo</b>: {goles_local}-{goles_visita} — presión máxima")

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

    return f"""{icono} <b>ALERTA {nivel} [{puntos} pts] — {liga_nombre}</b>
⚽ <b>{local} {goles_local}-{goles_visita} {visita}</b>
⏱ Minuto: <b>{minuto}'</b>{pos_txt}

<b>📊 {ronda} — Goles en 89+:</b>
• Terminados: {terminados} de {total} | Tardíos: <b>{goles_89}</b>
• Promedio esperado: {promedio:.0f} por jornada

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

    while True:
        ciclo += 1
        ahora = time.time()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Ciclo #{ciclo}")

        partidos_vivos = obtener_partidos_en_vivo()
        print(f"  → {len(partidos_vivos)} partidos en vivo")

        # Actualizar jornadas cada 10 minutos
        if ahora - ultima_actualizacion_jornada > 600:
            print("  → Actualizando jornadas por ronda completa...")
            rondas_vistas = set()
            for fixture in partidos_vivos:
                liga_id = fixture["league"]["id"]
                ronda   = fixture["league"]["round"]
                if (liga_id, ronda) not in rondas_vistas:
                    actualizar_jornada(liga_id, ronda)
                    rondas_vistas.add((liga_id, ronda))
            ultima_actualizacion_jornada = ahora

        # Detectar si hay partido en minuto crítico → intervalo urgente
        hay_urgente = any(
            (f["fixture"]["status"]["elapsed"] or 0) >= MINUTO_URGENTE
            for f in partidos_vivos
        )
        intervalo = INTERVALO_URGENTE if hay_urgente else INTERVALO_NORMAL
        if hay_urgente:
            print(f"  ⚡ Partido en min {MINUTO_URGENTE}+ — intervalo reducido a {INTERVALO_URGENTE}s")

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
                datos_jornada
            )

            if nivel is None:
                continue

            # Re-alerta si sube de nivel (clave incluye puntos)
            clave = f"{fixture_id}_{puntos}_{minuto // 5}"
            if clave in alertas_enviadas:
                continue

            local  = fixture["teams"]["home"]["name"]
            visita = fixture["teams"]["away"]["name"]
            print(f"\n  {nivel} [{puntos}pts]: {local} vs {visita} (min {minuto})")

            mensaje = construir_mensaje(
                fixture, liga_id, puntos, nivel, motivos,
                datos_jornada, pos_local, pos_visita
            )
            enviar_telegram(mensaje)
            alertas_enviadas.add(clave)

        if len(alertas_enviadas) > 2000:
            alertas_enviadas.clear()

        time.sleep(intervalo)


if __name__ == "__main__":
    main()
