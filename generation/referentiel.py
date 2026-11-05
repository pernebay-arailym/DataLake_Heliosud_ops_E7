"""Référentiel commun d'HélioSud (épreuve E7) : sites, onduleurs, ensoleillement.

Partagé par generate_batch.py et simulateur_onduleurs.py pour que les fichiers
batch (météo, courbes de charge) et le flux temps réel des onduleurs décrivent
le même monde : mêmes sites, mêmes onduleurs, même nébulosité à la même heure.
Tout est déterministe : aucune dépendance au hasard de l'appelant.
"""
from datetime import date, datetime, timedelta, timezone

import numpy as np

# (commune, latitude, longitude) — coordonnées approximatives
COMMUNES = [
    ("Béziers", 43.34, 3.22), ("Narbonne", 43.18, 3.00), ("Perpignan", 42.70, 2.90),
    ("Carcassonne", 43.21, 2.35), ("Montpellier", 43.61, 3.88), ("Nîmes", 43.84, 4.36),
    ("Alès", 44.13, 4.08), ("Avignon", 43.95, 4.81), ("Carpentras", 44.06, 5.05),
    ("Orange", 44.14, 4.81), ("Arles", 43.68, 4.63), ("Salon-de-Provence", 43.64, 5.10),
    ("Aix-en-Provence", 43.53, 5.45), ("Aubagne", 43.29, 5.57), ("Toulon", 43.12, 5.93),
    ("Draguignan", 43.54, 6.46), ("Fréjus", 43.43, 6.74), ("Manosque", 43.83, 5.78),
    ("Gap", 44.56, 6.08), ("Digne-les-Bains", 44.09, 6.24), ("Sisteron", 44.19, 5.94),
    ("Montélimar", 44.56, 4.75), ("Castres", 43.61, 2.24), ("Albi", 43.93, 2.15),
    ("Montauban", 44.02, 1.35), ("Agen", 44.20, 0.62), ("Auch", 43.65, 0.59),
    ("Tarbes", 43.23, 0.07), ("Millau", 44.10, 3.08), ("Rodez", 44.35, 2.57),
    ("Mende", 44.52, 3.50), ("Lodève", 43.73, 3.32), ("Sète", 43.40, 3.69),
    ("Pézenas", 43.46, 3.42), ("Limoux", 43.05, 2.22), ("Foix", 42.97, 1.61),
    ("Céret", 42.49, 2.75), ("Istres", 43.51, 4.99), ("Brignoles", 43.41, 6.06),
    ("Apt", 43.88, 5.40),
]
PUISSANCE_ONDULEUR_KW = {"SolarTech": 50.0, "Voltaris": 60.0}
MODELE = {"SolarTech": "ST-50K", "Voltaris": "VX-60"}
N_SITES = 40


def sites():
    """Liste des 40 sites (déterministe)."""
    rng = np.random.default_rng(7)
    out = []
    for i, (commune, lat, lon) in enumerate(COMMUNES[:N_SITES], start=1):
        sol = bool(rng.random() < 0.6)
        mwc = round(float(rng.uniform(8, 25) if sol else rng.uniform(1, 5)), 1)
        constructeur = "SolarTech" if i % 2 == 1 else "Voltaris"
        mise = date(2014, 1, 1) + timedelta(days=int(rng.integers(0, 365 * 9)))
        out.append({
            "site_id": f"SITE-{i:02d}",
            "nom": f"Centrale {commune} {'Sol' if sol else 'Toiture'}",
            "commune": commune,
            "type_installation": "Sol" if sol else "Toiture",
            "puissance_mwc": mwc,
            "latitude": lat,
            "longitude": lon,
            "constructeur_onduleur": constructeur,
            "date_mise_en_service": mise.isoformat(),
            "nb_onduleurs": max(5, round(mwc * 1000 / PUISSANCE_ONDULEUR_KW[constructeur])),
        })
    return out


def onduleurs(site):
    """Onduleurs d'un site : identifiants et caractéristiques."""
    c = site["constructeur_onduleur"]
    return [{
        "onduleur_id": f"{site['site_id']}-INV-{k:03d}",
        "site_id": site["site_id"],
        "constructeur": c,
        "modele": MODELE[c],
        "puissance_nominale_kw": PUISSANCE_ONDULEUR_KW[c],
        "date_installation": site["date_mise_en_service"],
    } for k in range(1, site["nb_onduleurs"] + 1)]


def irradiance_ciel_clair(lat, lon, t_utc):
    """Irradiance globale par ciel clair (W/m²), approximation solaire simple."""
    lat = np.radians(np.asarray(lat, dtype=float))
    doy = t_utc.timetuple().tm_yday
    decl = np.radians(23.44) * np.sin(2 * np.pi * (284 + doy) / 365)
    heure_solaire = t_utc.hour + t_utc.minute / 60 + np.asarray(lon, dtype=float) / 15
    h = np.radians(15 * (heure_solaire - 12))
    sin_elev = np.sin(lat) * np.sin(decl) + np.cos(lat) * np.cos(decl) * np.cos(h)
    return 1000.0 * np.clip(sin_elev, 0, None) ** 1.15


_cache = {}


def nebulosite(site_idx, t_utc):
    """Nébulosité (0 à 1) d'un site à un instant : déterministe, continue dans l'heure."""
    jour = t_utc.toordinal()
    cle = (int(site_idx), jour)
    if cle not in _cache:
        r = np.random.default_rng([int(site_idx), jour, 11])
        base = r.beta(2, 3)
        marche = np.cumsum(r.normal(0, 0.08, 25))
        _cache[cle] = np.clip(base + marche - marche.mean(), 0.02, 0.98)
    v = _cache[cle]
    h = t_utc.hour + t_utc.minute / 60
    i = int(h)
    f = h - i
    return float(v[i] * (1 - f) + v[min(i + 1, 24)] * f)


def facteur_nuages(c):
    """Part de l'irradiance transmise pour une nébulosité c (0 à 1)."""
    return 1.0 - 0.75 * c


def temperature_ambiante(lat, t_utc, c):
    """Température ambiante (°C) : saison + cycle jour/nuit - effet des nuages."""
    doy = t_utc.timetuple().tm_yday
    saison = 14 + 11 * np.sin(2 * np.pi * (doy - 110) / 365)
    jour = 5 * np.sin(2 * np.pi * (t_utc.hour + 1 - 9) / 24)
    return float(saison + jour - 3 * c)


def utc(y, m, d, h=0, mi=0):
    return datetime(y, m, d, h, mi, tzinfo=timezone.utc)
