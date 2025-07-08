
# RER A – Joinville‐le‐Pont : premiers et derniers trains

Ce dépôt contient un script Python (`script.py`) qui télécharge le GTFS Île‑de‑France Mobilités et calcule les prochains passages du RER A à Joinville‑le‑Pont, ainsi que les premiers et derniers trains théoriques. Les dépendances nécessaires (**pandas**, **requests**) sont répertoriées dans `requirements.txt`.

```bash
pip install -r requirements.txt
python script.py
# export GTFS complet pour tous les arrêts ciblés
python scripts/gtfs_extract.py
# prochain trains du RER A à Joinville
python scripts/extract_rer_a_gtfs.py
```

Ces deux commandes supplémentaires utilisent les mêmes dépendances
(*pandas* et *requests*) et écrivent les fichiers JSON dans le répertoire
`static/`.

### Mise à jour des données

Les workflows GitHub fournis automatisent ces scripts&nbsp;: `update-gtfs.yml`
exécute `gtfs_extract.py` tous les lundis pour régénérer
`static/horaires_export.json`, tandis que `allstopsextract.yml` lance
`extract_rer_a_gtfs.py` chaque jour afin d'actualiser
`static/rer_a_prochains_trains_by_direction.json`.

## Licence

Ce projet est sous licence [MIT](LICENSE).
