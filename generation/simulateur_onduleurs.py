#!/usr/bin/env python3
"""Simulateur du flux temps réel de télémétrie des onduleurs d'HélioSud (épreuve E7).

Chaque « tick » représente UNE MINUTE simulée : le simulateur émet un message par
onduleur actif, puis attend --intervalle secondes réelles avant le tick suivant
(1 s par défaut = temps accéléré x60 ; 0 = aussi vite que possible ; 60 = temps réel).

Deux constructeurs, deux schémas JSON (hétérogénéité volontaire) :
  SolarTech (sites impairs) : {"device_id","ts" (UTC, Z),"p_ac_w","v_dc","temp_c","status_code"}
  Voltaris  (sites pairs)   : {"inverter":{"id","model"},"time" (heure locale),"power_kw",
                               "dc_voltage_v","temperature","fault"}
Anomalies injectées : doublons, trous de transmission (messages isolés et pannes de
communication d'un site entier), codes de défaut, et dérive thermique progressive de
quelques onduleurs qui finissent en panne (utile pour la maintenance prédictive).

Sorties (--sink) : stdout (JSON lignes), fichier (--fichier), kafka (--bootstrap, --topic).
Les journaux vont sur stderr : stdout ne contient que les messages.

Exemples :
  python3 simulateur_onduleurs.py --minutes 5 --intervalle 0                    # affichage
  python3 simulateur_onduleurs.py --minutes 1440 --intervalle 0 --sink fichier \
          --fichier onduleurs_jour1.jsonl                                        # 1 jour d'historique
  python3 simulateur_onduleurs.py --sink kafka --bootstrap localhost:9092       # flux continu
"""
import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np

import referentiel as ref

PARIS = ZoneInfo("Europe/Paris")
# Codes de défaut. Seuls certains sont documentés dans le dictionnaire de données.
DEFAUTS_ST = [17, 42, 108, 203, 311]            # 17 = surchauffe, 42 = isolement (documentés)
DEFAUTS_VX = ["E104", "E211", "W007", "E350"]   # E104 = surchauffe (documenté)
CODE_SURCHAUFFE_ST, CODE_SURCHAUFFE_VX = 17, "E104"


def log(msg):
    print(msg, file=sys.stderr, flush=True)


class Flotte:
    def __init__(self, nb_sites, par_site, taux_degradation, demo_panne, rng):
        self.rng = rng
        lignes = []
        for si, s in enumerate(ref.sites()[:nb_sites]):
            onds = ref.onduleurs(s)
            if par_site > 0:
                onds = onds[:par_site]
            lignes += [(si, s, o) for o in onds]
        self.n = len(lignes)
        self.ids = [o["onduleur_id"] for _, _, o in lignes]
        self.site_idx = np.array([si for si, _, _ in lignes])
        self.nb_sites = nb_sites
        self.lat = np.array([s["latitude"] for _, s, _ in lignes])
        self.lon = np.array([s["longitude"] for _, s, _ in lignes])
        self.pnom = np.array([o["puissance_nominale_kw"] for _, _, o in lignes])
        self.voltaris = np.array([s["constructeur_onduleur"] == "Voltaris" for _, s, _ in lignes])
        # dérive thermique : début (minute), durée de la dérive, durée de la panne
        self.degrade = rng.random(self.n) < taux_degradation
        self.onset = rng.integers(0, 2 * 1440, self.n)
        self.rampe = rng.integers(720, 2160, self.n)
        self.duree_panne = rng.integers(30, 180, self.n)
        if demo_panne and self.n:
            k = int(rng.integers(0, self.n))
            self.degrade[k], self.onset[k], self.rampe[k], self.duree_panne[k] = True, 0, 10, 15
            log(f"[demo] dérive accélérée sur {self.ids[k]} : panne attendue vers la minute 10")
        self.hors_ligne = np.zeros(nb_sites, dtype=int)  # ticks restants de coupure par site

    def tick(self, m, t_utc, taux_trous, taux_doublons):
        """Messages (liste de dicts) pour la minute simulée m à l'instant t_utc."""
        rng = self.rng
        n = self.n
        clair = ref.irradiance_ciel_clair(self.lat, self.lon, t_utc)
        nuage_site = np.array([ref.nebulosite(i, t_utc) for i in range(self.nb_sites)])
        c = nuage_site[self.site_idx]
        amb = np.array([ref.temperature_ambiante(la, t_utc, ci) for la, ci in zip(self.lat, c)])
        prog = np.where(self.degrade, np.clip((m - self.onset) / self.rampe, 0, 1), 0.0)
        en_panne = self.degrade & (m >= self.onset + self.rampe) & \
            (m < self.onset + self.rampe + self.duree_panne)
        p = self.pnom * (clair / 1000) * (1 - 0.75 * c) * 0.92 * rng.normal(1, 0.02, n) * (1 - 0.3 * prog)
        p = np.clip(p, 0, None)
        temp = amb + 22 * (p / self.pnom) + 30 * prog + rng.normal(0, 0.5, n)
        vdc = np.where(clair > 5, 600 + 150 * clair / 1000 + rng.normal(0, 4, n), rng.normal(20, 3, n))
        sporadique = (rng.random(n) < 1.5e-5) & ~en_panne
        code_sp = rng.integers(0, 10, n)
        p = np.where(en_panne | sporadique, 0.0, p)

        # coupures de communication d'un site entier (aucun message pendant 10 à 45 minutes)
        for i in range(self.nb_sites):
            if self.hors_ligne[i] > 0:
                self.hors_ligne[i] -= 1
            elif rng.random() < 0.0004:
                self.hors_ligne[i] = int(rng.integers(10, 46))
        hl = self.hors_ligne[self.site_idx] > 0

        loc = t_utc.astimezone(PARIS).isoformat()
        iso = t_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        msgs = []
        tire = rng.random(n)
        for k in range(n):
            if hl[k] or tire[k] < taux_trous:
                continue
            if self.voltaris[k]:
                defaut = None
                if en_panne[k]:
                    defaut = CODE_SURCHAUFFE_VX
                elif sporadique[k]:
                    defaut = DEFAUTS_VX[code_sp[k] % len(DEFAUTS_VX)]
                msg = {"inverter": {"id": self.ids[k], "model": "VX-60"}, "time": loc,
                       "power_kw": round(float(p[k]), 2), "dc_voltage_v": round(float(vdc[k]), 1),
                       "temperature": round(float(temp[k]), 1), "fault": defaut}
            else:
                if en_panne[k]:
                    statut = CODE_SURCHAUFFE_ST
                elif sporadique[k]:
                    statut = DEFAUTS_ST[code_sp[k] % len(DEFAUTS_ST)]
                else:
                    statut = 0 if clair[k] > 5 else 1
                msg = {"device_id": self.ids[k], "ts": iso, "p_ac_w": int(p[k] * 1000),
                       "v_dc": round(float(vdc[k]), 1), "temp_c": round(float(temp[k]), 1),
                       "status_code": statut}
            msgs.append(msg)
            if rng.random() < taux_doublons:
                msgs.append(msg)
        return msgs


def cle(msg):
    return msg["device_id"] if "device_id" in msg else msg["inverter"]["id"]


def ouvrir_sink(a):
    if a.sink == "stdout":
        return (lambda msg: print(json.dumps(msg, ensure_ascii=False))), (lambda: sys.stdout.flush())
    if a.sink == "fichier":
        if not a.fichier:
            sys.exit("--fichier est obligatoire avec --sink fichier")
        fh = open(a.fichier, "a", encoding="utf-8")
        return (lambda msg: fh.write(json.dumps(msg, ensure_ascii=False) + "\n")), fh.close
    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("kafka-python est requis pour --sink kafka : pip install kafka-python")
    from kafka.errors import NoBrokersAvailable
    try:
        prod = KafkaProducer(bootstrap_servers=a.bootstrap, key_serializer=str.encode,
                             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
    except NoBrokersAvailable:
        sys.exit(f"Aucun broker Kafka joignable sur {a.bootstrap} (vérifiez --bootstrap).")
    return (lambda msg: prod.send(a.topic, key=cle(msg), value=msg)), prod.flush


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--debut", default="2025-06-02T00:00:00+00:00",
                    help="instant UTC simulé du premier tick, ou 'maintenant'")
    ap.add_argument("--minutes", type=int, default=0, help="nombre de ticks (0 = sans fin)")
    ap.add_argument("--intervalle", type=float, default=1.0, help="secondes réelles entre deux ticks")
    ap.add_argument("--sites", type=int, default=3, help="nombre de sites simulés (1 à 40)")
    ap.add_argument("--onduleurs-par-site", type=int, default=20,
                    help="0 = flotte réelle du référentiel (≈ 200 par site)")
    ap.add_argument("--seed", type=int, default=2025)
    ap.add_argument("--taux-degradation", type=float, default=0.03)
    ap.add_argument("--taux-trous", type=float, default=0.003)
    ap.add_argument("--taux-doublons", type=float, default=0.005)
    ap.add_argument("--demo-panne", action="store_true",
                    help="force une panne rapide sur un onduleur (démonstration d'alerte)")
    ap.add_argument("--sink", choices=["stdout", "fichier", "kafka"], default="stdout")
    ap.add_argument("--fichier")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="onduleurs.telemetrie")
    a = ap.parse_args()
    if not 1 <= a.sites <= ref.N_SITES:
        sys.exit(f"--sites doit être compris entre 1 et {ref.N_SITES}")

    t = (datetime.now(timezone.utc).replace(second=0, microsecond=0) if a.debut == "maintenant"
         else datetime.fromisoformat(a.debut).astimezone(timezone.utc))
    rng = np.random.default_rng(a.seed)
    flotte = Flotte(a.sites, a.onduleurs_par_site, a.taux_degradation, a.demo_panne, rng)
    emit, fermer = ouvrir_sink(a)
    log(f"{flotte.n} onduleurs sur {a.sites} site(s), sink={a.sink}, début={t.isoformat()}")

    total, m = 0, 0
    try:
        while a.minutes == 0 or m < a.minutes:
            debut_tick = time.monotonic()
            for msg in flotte.tick(m, t, a.taux_trous, a.taux_doublons):
                emit(msg)
                total += 1
            m += 1
            t += timedelta(minutes=1)
            if a.intervalle > 0:
                time.sleep(max(0.0, a.intervalle - (time.monotonic() - debut_tick)))
    except KeyboardInterrupt:
        log("Arrêt demandé.")
    finally:
        fermer()
        log(f"{total} messages émis sur {m} minute(s) simulée(s).")


if __name__ == "__main__":
    main()
