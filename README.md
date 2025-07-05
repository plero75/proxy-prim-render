
# RER A – Joinville‐le‐Pont : premiers et derniers trains

Ce dépôt contient un script Python qui télécharge le GTFS Île‑de‑France Mobilités
(via un *worker‑proxy* facultatif) et calcule les premiers et derniers passages
du RER A à Joinville‑le‑Pont, dans les deux sens, pour la date de votre choix.

```bash
pip install -r requirements.txt
export IDFM_APIKEY="votre_cle_prim"
# si nécessaire :
export PROXY_WORKER="https://mon-worker.workers.dev/"

python first_last_rera_joinville.py           # aujourd’hui
python first_last_rera_joinville.py -d 2025-07-09
python first_last_rera_joinville.py --save-json sortie.json
```

## Variables d’environnement

| Variable        | Rôle                                                             |
|-----------------|------------------------------------------------------------------|
| `IDFM_APIKEY`   | Jeton « données statiques » PRIM (obligatoire)                  |
| `PROXY_WORKER`  | URL de votre worker Cloudflare servant de proxy (optionnel)     |

## Licence

MIT
