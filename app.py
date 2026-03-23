from flask import Flask, render_template_string, request
from neo4j import GraphDatabase

app = Flask(__name__)

# Connexion à la base (Même identifiants que ton script d'injection)
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "eTyDWf1CD7gvr5Qg1SgMUIBccgzbm0hTYHzNJ3hAq2M"))

# Le design de la page (HTML)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Projet Interopérabilité 2026</title>
    <style>
        body { font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .box { border: 1px solid #ccc; padding: 15px; border-radius: 5px; margin-bottom: 20px; background: #f9f9f9; }
        button { background-color: #4CAF50; color: white; padding: 10px 15px; border: none; cursor: pointer; }
        button:hover { background-color: #45a049; }
        select, input[type="text"] { padding: 8px; margin-right: 5px; }
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
                    <a href="{{row.Wiki}}" target="_blank">Voir sur wikidata</a>
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

    {% if message %}
    <p style="color: green; font-weight: bold;">{{ message }}</p>
    {% endif %}

    <div class="box">
        <h3>Livrable L7.2 : Enrichissement Wikidata / DBpedia</h3>
        <p>Lier un noeud de la base E à sa page Wikidata ou DBpedia (sans dupliquer l'information).</p>
        <form method="post" action="/link">
            <p>
                <label>Type de noeud :</label>
                <select name="type_noeud">
                    <option value="Usine">Usine (par ville)</option>
                    <option value="Vehicule">Véhicule (par VIN)</option>
                </select>
            </p>
            <p>
                <input type="text" name="identifiant" placeholder="Ville ou VIN (ex: Lyon)" required>
                <select name="base_externe">
                    <option value="wikidata">Wikidata</option>
                    <option value="dbpedia">DBpedia</option>
                </select>
                <input type="text" name="url_externe" placeholder="ID ou URL (ex: Q456)" required>
                <button type="submit" style="background-color: #008CBA;">Créer le lien</button>
            </p>
        </form>
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
    return render_template_string(HTML_TEMPLATE, usines_list=get_usines_list(), modeles_list=get_modeles_list(), liens_existants=get_liens_existants())

@app.route('/query', methods=['POST'])
def run_query():
    filtre_modele = request.form.get('filtre_modele', '')
    filtre_usine = request.form.get('filtre_usine', '')
    filtre_panne = request.form.get('filtre_panne', '')

    cypher_query = """
    MATCH (c:Client)-[:EMET]->(d:DemandeClient)-[:DONNE_LIEU_A]->(o:OffreCommerciale)-[:ABOUTIT_A]->(bc:BonDeCommande)
    MATCH (bc)-[:DECLENCHE]->(of:OrdreDeFabrication)-[:PERMET_DE_FABRIQUER]->(v:Vehicule)-[:ASSEMBLE_DANS]->(u:Usine)
    OPTIONAL MATCH (ds:DemandeSAV)-[:CONCERNE]->(v)
    OPTIONAL MATCH (v)-[:DONNE_LIEU_A]->(diag:Diagnostic)-[:DECLENCHE]->(rep:Reparation)
    WITH c, v, u, ds, rep
    WHERE ($filtre_modele = '' OR v.modele = $filtre_modele)
      AND ($filtre_usine = '' OR u.localisation = $filtre_usine)
    RETURN 
        c.nom AS Client, 
        v.numero_de_serie AS Vin,
        v.modele AS Modele,
        u.localisation AS Usine, 
        u.wikidata_url AS Wiki,
        COALESCE(ds.description, "Aucun incident") AS Panne,
        COALESCE(rep.type_intervention, "Aucune") AS Reparation
    ORDER BY c.nom, v.numero_de_serie
    """
    with driver.session() as session:
        result = session.run(cypher_query, filtre_modele=filtre_modele, filtre_usine=filtre_usine)
        data = [record.data() for record in result]

    if filtre_panne == 'avec':
        data = [r for r in data if r['Panne'] != 'Aucun incident']
    elif filtre_panne == 'sans':
        data = [r for r in data if r['Panne'] == 'Aucun incident']

    return render_template_string(HTML_TEMPLATE,
                                  results=data, usines_list=get_usines_list(), modeles_list=get_modeles_list(),
                                  liens_existants=get_liens_existants(),
                                  filtre_modele=filtre_modele, filtre_usine=filtre_usine, filtre_panne=filtre_panne)

@app.route('/link', methods=['POST'])
def add_link():
    type_noeud = request.form['type_noeud']
    identifiant = request.form['identifiant'].strip()
    base_externe = request.form['base_externe']
    url_externe = request.form['url_externe'].strip()

    if base_externe == 'wikidata':
        if not url_externe.startswith('http'):
            url_externe = f"https://www.wikidata.org/wiki/{url_externe}"
        prop = 'wikidata_url'
    else:
        if not url_externe.startswith('http'):
            url_externe = f"http://dbpedia.org/resource/{url_externe}"
        prop = 'dbpedia_url'

    if type_noeud == 'Usine':
        cypher = f"MATCH (u:Usine {{localisation: $identifiant}}) SET u.{prop} = $url RETURN u.localisation AS nom"
    else:
        cypher = f"MATCH (v:Vehicule {{numero_de_serie: $identifiant}}) SET v.{prop} = $url RETURN v.numero_de_serie AS nom"

    with driver.session() as session:
        result = session.run(cypher, identifiant=identifiant, url=url_externe)
        record = result.single()

    if record:
        return render_template_string(HTML_TEMPLATE,
                                      message=f"Lien {base_externe} ajouté pour {type_noeud} « {identifiant} » !",
                                      usines_list=get_usines_list(), modeles_list=get_modeles_list(), liens_existants=get_liens_existants())
    else:
        return render_template_string(HTML_TEMPLATE,
                                      error=f"Noeud {type_noeud} « {identifiant} » introuvable dans la base E.",
                                      usines_list=get_usines_list(), modeles_list=get_modeles_list(), liens_existants=get_liens_existants())

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)