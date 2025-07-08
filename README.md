
# RER A – Joinville‐le‐Pont : premiers et derniers trains

Ce dépôt contient un script Python (`script.py`) qui télécharge le GTFS Île‑de‑France Mobilités et calcule les prochains passages du RER A à Joinville‑le‑Pont, ainsi que les premiers et derniers trains théoriques. Les dépendances nécessaires (**pandas**, **requests**) sont répertoriées dans `requirements.txt`.

```bash
pip install -r requirements.txt
python script.py
```
 
# JavaScript

Le fichier `script.js` est minifié pour la production. Pour le modifier, éditez `script.src.js` puis générez la version compacte :

```bash
npx terser script.src.js -o script.js -c -m
```


## Licence

Ce projet est sous licence [MIT](LICENSE).
