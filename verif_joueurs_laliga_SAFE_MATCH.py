from flask import Flask, request, jsonify
import os
import time
import json
import pandas as pd
import requests
import unicodedata
from rapidfuzz import fuzz

app = Flask(__name__)

CACHE_DIR = "cache"
API_BASE = "https://data-sheets-updates.onrender.com"
MAX_CACHE_AGE_HOURS = 24
MAX_RETRIES = 3
PAUSE_AFTER = 10
PAUSE_SECONDS = 30

CLUB_SLUG_TO_ID = {
    "athletic-club-bilbao": 621,
    "atletico-madrid-madrid": 13,
    "barcelona-barcelona": 131,
    "celta-de-vigo-vigo": 940,
    "deportivo-alaves-vitoria-gasteiz": 1108,
    "elche-elche": 1531,
    "espanyol-barcelona": 670,
    "getafe-getafe-madrid": 3709,
    "girona-girona": 12321,
    "levante-valencia": 3368,
    "mallorca-palma-de-mallorca": 237,
    "osasuna-pamplona": 331,
    "rayo-vallecano-madrid": 367,
    "real-betis-sevilla": 150,
    "real-madrid-madrid": 418,
    "real-oviedo-oviedo": 2497,
    "real-sociedad-san-sebastian": 681,
    "sevilla-sevilla": 368,
    "valencia-valencia": 1049,
    "villarreal-villarreal": 1050
}

def remove_accents(text):
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def extract_first_name(full_name):
    return remove_accents(full_name.strip().split()[0]).lower()

def is_cache_fresh(filepath):
    if not os.path.exists(filepath):
        return False
    file_age_hours = (time.time() - os.path.getmtime(filepath)) / 3600
    return file_age_hours < MAX_CACHE_AGE_HOURS

def fetch_or_load_players(club_id, slug):
    cache_file = f"{CACHE_DIR}/club_{club_id}.json"
    if is_cache_fresh(cache_file):
        with open(cache_file, "r") as f:
            return json.load(f)

    for attempt in range(MAX_RETRIES):
        try:
            url = f"{API_BASE}/clubs/{club_id}/players"
            resp = requests.get(url)
            if resp.status_code == 200:
                data = [p["name"] for p in resp.json().get("players", [])]
                with open(cache_file, "w") as f:
                    json.dump(data, f)
                return data
        except Exception:
            pass
        time.sleep(1.5 + attempt)
    return []

def verifier_effectifs(df):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df = df[["player_display_name", "team_slug"]].dropna()
    results = []

    for i, (slug, club_id) in enumerate(CLUB_SLUG_TO_ID.items(), start=1):
        subset = df[df["team_slug"] == slug]
        if subset.empty:
            continue

        tm_players = fetch_or_load_players(club_id, slug)
        if not tm_players:
            continue

        time.sleep(1)

        all_transfer_firsts = [extract_first_name(p) for p in tm_players]
        all_sheet_firsts = [extract_first_name(pn) for pn in subset["player_display_name"]]

        matched_tm_names = set()
        matched_sheet_names = set()

        for _, row in subset.iterrows():
            name = row["player_display_name"]
            player_first = extract_first_name(name)
            scores = []

            for candidate in tm_players:
                score = fuzz.token_sort_ratio(remove_accents(name), remove_accents(candidate))
                candidate_first = extract_first_name(candidate)

                if candidate_first == player_first:
                    if all_transfer_firsts.count(player_first) == 1 and all_sheet_firsts.count(player_first) == 1:
                        score = min(score + 25, 100)

                scores.append((candidate, score))

            scores.sort(key=lambda x: x[1], reverse=True)
            best, score = scores[0] if scores else ("", 0)

            best = best if score >= 65 else ""
            status = "✅" if score >= 90 else "⚠️" if score >= 65 else "❌"

            if best:
                matched_tm_names.add(best)
                matched_sheet_names.add(name)

            type_value = (
                "match exact" if score >= 90 else
                "match partiel" if score >= 65 else
                "non trouvé transfermarkt (parti du club ?)"
            )

            results.append({
                "Nom du joueur dans ta liste": name,
                "Club attribué dans ta liste": slug,
                "Nom trouvé dans Transfermarkt": best,
                "Similarité (%)": round(score, 2) if best else "",
                "Match validé ?": status if best else "❌",
                "Type": type_value
            })

        for tm_name in tm_players:
            if tm_name not in matched_tm_names:
                results.append({
                    "Nom du joueur dans ta liste": "",
                    "Club attribué dans ta liste": slug,
                    "Nom trouvé dans Transfermarkt": tm_name,
                    "Similarité (%)": "",
                    "Match validé ?": "🆕",
                    "Type": "nouveau joueur à ajouter à ta data sheet"
                })

        if i == PAUSE_AFTER:
            time.sleep(PAUSE_SECONDS)

    return pd.DataFrame(results)

@app.route("/analyze", methods=["POST"])
def analyze():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    try:
        df = pd.read_csv(file, skiprows=3)
        df.columns = df.columns.str.strip()  # nettoie les espaces invisibles
        print("Colonnes détectées :", df.columns.tolist())
        required_cols = ["player_display_name", "team_slug"]
        if not all(col in df.columns for col in required_cols):
            return jsonify({"error": f"Missing required columns. Found: {df.columns.tolist()}"}), 400

        results_df = verifier_effectifs(df)
        return results_df.to_json(orient="records"), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def home():
    return "✅ API Flask déployée avec succès ! Utilise /analyze pour envoyer un CSV.", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
