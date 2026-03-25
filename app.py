from flask import Flask, render_template_string, request, jsonify
from neo4j import GraphDatabase
import requests

app = Flask(__name__)

# Connexion à la base
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "eTyDWf1CD7gvr5Qg1SgMUIBccgzbm0hTYHzNJ3hAq2M"))

# Le design de la page (HTML)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Projet Interopérabilité 2026</title>
    <style>
        body { font-family: sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .box { border: 1px solid #ccc; padding: 15px; border-radius: 5px; margin-bottom: 20px; background: #f9f9f9; }
        button { background-color: #4CAF50; color: white; padding: 10px 15px; border: none; cursor: pointer; margin: 2px; }
        button:hover { background-color: #45a049; }
        select, input[type="text"] { padding: 8px; margin-right: 5px; }
        .search-btn { background-color: #008CBA; }
        .search-btn:hover { background-color: #007099; }
        #wikidata-results { margin-top: 15px; }
        .wiki-result { 
            border: 1px solid #ddd; 
            padding: 10px; 
            margin: 5px 0; 
            border-radius: 5px; 
            background: white;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .wiki-result:hover { background: #f0f8ff; }
        .wiki-info { flex: 1; }
        .wiki-id { color: #666; font-size: 0.9em; }
        .wiki-desc { color: #333; font-style: italic; }
        .select-btn { background-color: #4CAF50; padding: 8px 15px; }
        .loading { color: #666; font-style: italic; }
        .hidden { display: none; }
    </style>
</head>
<body>
    <h1>Livrable L6.2 : Application Transverse</h1>

    {% if message %}
    <p style="color: green; font-weight: bold;">{{ message }}</p>
    {% endif %}
    {% if error %}
    <p style="color: red; font-weight: bold;">{{ error }}</p>
    {% endif %}

    <div class="box">
        <h3>Requête Transverse</h3>
        <p>Affiche le parcours complet : Client → Commande → Usine → Panne SAV</p>
        <form method="post" action="/query">
            <p>
                <label>Filtrer par modèle :</label>
                <select name="filtre_modele">
                    <option value="">Tous les modèles</option>
                    {% for m in modeles_list %}
                    <option value="{{ m }}" {{ 'selected' if filtre_modele == m else '' }}>{{ m }}</option>
                    {% endfor %}
                </select>
                <label>Filtrer par usine :</label>
                <select name="filtre_usine">
                    <option value="">Toutes les usines</option>
                    {% for u in usines_list %}
                    <option value="{{ u }}" {{ 'selected' if filtre_usine == u else '' }}>{{ u }}</option>
                    {% endfor %}
                </select>
                <label>Pannes :</label>
                <select name="filtre_panne">
                    <option value="">Tous</option>
                    <option value="avec" {{ 'selected' if filtre_panne == 'avec' else '' }}>Avec panne uniquement</option>
                    <option value="sans" {{ 'selected' if filtre_panne == 'sans' else '' }}>Sans panne uniquement</option>
                </select>
            </p>
            <button type="submit">Lancer l'analyse</button>
        </form>
    </div>

    {% if results is defined and results is not none %}
    <h3>Résultats de l'analyse ({{ results|length }} lignes) :</h3>
    <table>
        <tr>
            <th>Client (M1 Vente)</th>
            <th>Voiture VIN (M2 Prod)</th>
            <th>Modèle (M2 Prod)</th>
            <th>Usine (M2 Prod)</th>
            <th>Lien Wikidata</th>
            <th>Problème SAV (M3)</th>
            <th>Réparation (M3)</th>
        </tr>
        {% for row in results %}
        <tr>
            <td>{{ row.Client }}</td>
            <td>{{ row.Vin }}</td>
            <td>{{ row.Modele }}</td>
            <td>{{ row.Usine }}</td>
            <td>
                {% if row.Wiki %}
                    <a href="{{row.Wiki}}" target="_blank">Voir sur Wikidata</a>
                {% else %}
                    Non lié
                {% endif %}
            </td>
            <td>{{ row.Panne }}</td>
            <td>{{ row.Reparation }}</td>
        </tr>
        {% endfor %}
    </table>
    {% endif %}

    <div class="box">
        <h3>Enrichissement Wikidata</h3>
        <p>Recherchez une ville ou un terme sur Wikidata et liez-le à une usine de la base.</p>
        
        <p>
            <label>Usine à enrichir :</label>
            <select id="usine_select" name="usine_select">
                {% for u in usines_list %}
                <option value="{{ u }}">{{ u }}</option>
                {% endfor %}
            </select>
        </p>
        
        <p>
            <label>Rechercher sur Wikidata :</label>
            <input type="text" id="search_term" placeholder="Ex: Lyon, Paris, Toulouse..." style="width: 250px;">
            <button type="button" class="search-btn" onclick="searchWikidata()">Rechercher</button>
        </p>
        
        <div id="wikidata-results"></div>
    </div>

    {% if liens_existants %}
    <div class="box">
        <h3>Liens externes déjà enregistrés</h3>
        <table>
            <tr><th>Type</th><th>Identifiant</th><th>Base</th><th>URL</th></tr>
            {% for lien in liens_existants %}
            <tr>
                <td>{{ lien.Type }}</td>
                <td>{{ lien.Identifiant }}</td>
                <td>{{ lien.Propriete }}</td>
                <td><a href="{{ lien.URL }}" target="_blank">{{ lien.URL }}</a></td>
            </tr>
            {% endfor %}
        </table>
    </div>
    {% endif %}

    <script>
        function searchWikidata() {
            const searchTerm = document.getElementById('search_term').value.trim();
            const resultsDiv = document.getElementById('wikidata-results');
            
            if (!searchTerm) {
                resultsDiv.innerHTML = '<p style="color: red;">Veuillez entrer un terme de recherche.</p>';
                return;
            }
            
            resultsDiv.innerHTML = '<p class="loading">Recherche en cours sur Wikidata...</p>';
            
            fetch('/search_wikidata?q=' + encodeURIComponent(searchTerm))
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        resultsDiv.innerHTML = '<p style="color: red;">Erreur: ' + data.error + '</p>';
                        return;
                    }
                    
                    if (data.results.length === 0) {
                        resultsDiv.innerHTML = '<p>Aucun résultat trouvé pour "' + searchTerm + '".</p>';
                        return;
                    }
                    
                    let html = '<h4>Résultats Wikidata (' + data.results.length + ') :</h4>';
                    data.results.forEach(item => {
                        html += `
                            <div class="wiki-result">
                                <div class="wiki-info">
                                    <strong>${item.label}</strong> 
                                    <span class="wiki-id">(${item.id})</span><br>
                                    <span class="wiki-desc">${item.description || 'Pas de description'}</span>
                                </div>
                                <button class="select-btn" onclick="selectWikidata('${item.id}', '${item.url}', '${item.label.replace(/'/g, "\\'")}')">
                                    ✓ Sélectionner
                                </button>
                            </div>
                        `;
                    });
                    resultsDiv.innerHTML = html;
                })
                .catch(err => {
                    resultsDiv.innerHTML = '<p style="color: red;">Erreur de connexion: ' + err + '</p>';
                });
        }
        
        function selectWikidata(wikidataId, wikidataUrl, label) {
            const usine = document.getElementById('usine_select').value;
            const resultsDiv = document.getElementById('wikidata-results');
            
            resultsDiv.innerHTML = '<p class="loading">Enregistrement du lien...</p>';
            
            fetch('/link_wikidata', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    usine: usine,
                    wikidata_id: wikidataId,
                    wikidata_url: wikidataUrl,
                    label: label
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    resultsDiv.innerHTML = '<p style="color: green; font-weight: bold;">✓ ' + data.message + '</p>';
                    // Recharger la page après 1.5 secondes pour voir le lien dans le tableau
                    setTimeout(() => location.reload(), 1500);
                } else {
                    resultsDiv.innerHTML = '<p style="color: red;">Erreur: ' + data.error + '</p>';
                }
            })
            .catch(err => {
                resultsDiv.innerHTML = '<p style="color: red;">Erreur: ' + err + '</p>';
            });
        }
        
        // Permettre la recherche avec Entrée
        document.getElementById('search_term').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                searchWikidata();
            }
        });
    </script>

</body>
</html>
"""

def get_usines_list():
    with driver.session() as session:
        result = session.run("MATCH (u:Usine) RETURN DISTINCT u.localisation AS loc ORDER BY loc")
        return [r["loc"] for r in result if r["loc"]]

def get_modeles_list():
    with driver.session() as session:
        result = session.run("MATCH (v:Vehicule) RETURN DISTINCT v.modele AS m ORDER BY m")
        return [r["m"] for r in result if r["m"]]

def get_liens_existants():
    liens = []
    with driver.session() as session:
        result = session.run("""
            MATCH (u:Usine)
            WHERE u.wikidata_url IS NOT NULL OR u.dbpedia_url IS NOT NULL
            RETURN 'Usine' AS Type, u.localisation AS Identifiant,
                   u.wikidata_url AS wikidata, u.dbpedia_url AS dbpedia
        """)
        for r in result:
            if r["wikidata"]:
                liens.append({"Type": "Usine", "Identifiant": r["Identifiant"], "Propriete": "wikidata_url", "URL": r["wikidata"]})
            if r["dbpedia"]:
                liens.append({"Type": "Usine", "Identifiant": r["Identifiant"], "Propriete": "dbpedia_url", "URL": r["dbpedia"]})
        result = session.run("""
            MATCH (v:Vehicule)
            WHERE v.wikidata_url IS NOT NULL OR v.dbpedia_url IS NOT NULL
            RETURN 'Vehicule' AS Type, v.numero_de_serie AS Identifiant,
                   v.wikidata_url AS wikidata, v.dbpedia_url AS dbpedia
        """)
        for r in result:
            if r["wikidata"]:
                liens.append({"Type": "Vehicule", "Identifiant": r["Identifiant"], "Propriete": "wikidata_url", "URL": r["wikidata"]})
            if r["dbpedia"]:
                liens.append({"Type": "Vehicule", "Identifiant": r["Identifiant"], "Propriete": "dbpedia_url", "URL": r["dbpedia"]})
    return liens


@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE,
                                  usines_list=get_usines_list(),
                                  modeles_list=get_modeles_list(),
                                  liens_existants=get_liens_existants())


@app.route('/query', methods=['POST'])
def run_query():
    filtre_modele = request.form.get('filtre_modele', '')
    filtre_usine = request.form.get('filtre_usine', '')
    filtre_panne = request.form.get('filtre_panne', '')

    cypher_query = """
    MATCH (c:Client)-[:EMET]->(d:DemandeClient)-[:DONNE_LIEU_A]->(o:OffreCommerciale)-[:ABOUTIT_A]->(bc:BonDeCommande)
    MATCH (bc)-[:DECLENCHE]->(of:OrdreDeFabrication)-[:PERMET_DE_FABRIQUER]->(v:Vehicule)-[:ASSEMBLE_DANS]->(u:Usine)
    WHERE ($filtre_modele = '' OR v.modele = $filtre_modele)
      AND ($filtre_usine = '' OR u.localisation = $filtre_usine)
    OPTIONAL MATCH (ds:DemandeSAV)-[:CONCERNE]->(v)
    OPTIONAL MATCH (v)-[:DONNE_LIEU_A]->(diag:Diagnostic)-[:DECLENCHE]->(rep:Reparation)
    RETURN 
        c.nom AS Client, 
        v.numero_de_serie AS Vin,
        v.modele AS Modele,
        u.localisation AS Usine, 
        u.wikidata_url AS Wiki,
        COALESCE(ds.description, 'Aucun incident') AS Panne,
        COALESCE(rep.type_intervention, 'Aucune') AS Reparation
    ORDER BY c.nom, v.numero_de_serie
    """

    try:
        with driver.session() as session:
            result = session.run(cypher_query, filtre_modele=filtre_modele, filtre_usine=filtre_usine)
            data = [record.data() for record in result]

        if filtre_panne == 'avec':
            data = [r for r in data if r['Panne'] != 'Aucun incident']
        elif filtre_panne == 'sans':
            data = [r for r in data if r['Panne'] == 'Aucun incident']

        return render_template_string(HTML_TEMPLATE,
                                      results=data,
                                      usines_list=get_usines_list(),
                                      modeles_list=get_modeles_list(),
                                      liens_existants=get_liens_existants(),
                                      filtre_modele=filtre_modele,
                                      filtre_usine=filtre_usine,
                                      filtre_panne=filtre_panne)
    except Exception as e:
        return render_template_string(HTML_TEMPLATE,
                                      error=f"Erreur de requête: {str(e)}",
                                      usines_list=get_usines_list(),
                                      modeles_list=get_modeles_list(),
                                      liens_existants=get_liens_existants())


@app.route('/search_wikidata')
def search_wikidata():
    """Recherche sur l'API Wikidata et retourne les résultats"""
    query = request.args.get('q', '').strip()

    if not query:
        return jsonify({"error": "Terme de recherche vide", "results": []})

    try:
        # Appel à l'API Wikidata (wbsearchentities)
        url = "https://www.wikidata.org/w/api.php"
        params = {
            "action": "wbsearchentities",
            "search": query,
            "language": "fr",
            "uselang": "fr",
            "type": "item",
            "limit": 10,
            "format": "json"
        }

        # User-Agent requis par Wikidata pour éviter le 403
        headers = {
            "User-Agent": "ProjetInteroperabilite/1.0 (Projet universitaire; contact@exemple.fr)"
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        results = []
        for item in data.get("search", []):
            results.append({
                "id": item.get("id"),
                "label": item.get("label", "Sans nom"),
                "description": item.get("description", ""),
                "url": f"https://www.wikidata.org/wiki/{item.get('id')}"
            })

        return jsonify({"results": results})

    except requests.exceptions.Timeout:
        return jsonify({"error": "Timeout - Wikidata met trop de temps à répondre", "results": []})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"Erreur de connexion à Wikidata: {str(e)}", "results": []})
    except Exception as e:
        return jsonify({"error": f"Erreur inattendue: {str(e)}", "results": []})


@app.route('/link_wikidata', methods=['POST'])
def link_wikidata():
    """Enregistre le lien Wikidata pour une usine"""
    try:
        data = request.get_json()
        usine = data.get('usine')
        wikidata_url = data.get('wikidata_url')
        wikidata_id = data.get('wikidata_id')
        label = data.get('label')

        if not usine or not wikidata_url:
            return jsonify({"success": False, "error": "Données manquantes"})

        with driver.session() as session:
            result = session.run("""
                MATCH (u:Usine {localisation: $usine})
                SET u.wikidata_url = $url, u.wikidata_id = $id, u.wikidata_label = $label
                RETURN u.localisation AS nom
            """, usine=usine, url=wikidata_url, id=wikidata_id, label=label)

            record = result.single()

        if record:
            return jsonify({
                "success": True,
                "message": f"Lien Wikidata ajouté pour l'usine « {usine} » → {label} ({wikidata_id})"
            })
        else:
            return jsonify({"success": False, "error": f"Usine « {usine} » introuvable"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)