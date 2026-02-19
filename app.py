from flask import Flask, render_template_string, request
from neo4j import GraphDatabase

app = Flask(__name__)

# Connexion à la base (Même identifiants que ton script d'injection)
driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password123"))

# Le design de la page (HTML simple)
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
    </style>
</head>
<body>
    <h1>Livrable L6.2 : Application Transverse</h1>

    <div class="box">
        <h3>Requête Transverse</h3>
        <p>Affiche le parcours complet : Client -> Commande -> Usine -> Panne SAV</p>
        <form method="post" action="/query">
            <button type="submit">Lancer l'analyse</button>
        </form>
    </div>

    {% if results %}
    <h3>Résultats de l'analyse :</h3>
    <table>
        <tr>
            <th>Client</th>
            <th>Voiture (VIN)</th>
            <th>Usine</th>
			<th>Lien Wikidata</th>
            <th>Problème SAV</th>
            <th>Coût Facture</th>
        </tr>
        {% for row in results %}
        <tr>
            <td>{{ row.Client }}</td>
            <td>{{ row.Vin }}</td>
            <td>{{ row.Usine }}</td>
			<td>
				{% if row.Wiki %}
					<a href="{{row.wiki}}" target="_blank">Voir sur wikidata</a>
				{% else %}
					Non lié
				{% endif %}
			</td>
            <td>{{ row.Panne }}</td>
            <td>{{ row.Facture }} €</td>
        </tr>
        {% endfor %}
    </table>
    {% endif %}
    
    {% if message %}
    <p style="color: green; font-weight: bold;">{{ message }}</p>
    {% endif %}

    <div class="box">
        <h3>Livrable L7.2 : Enrichissement Wikidata</h3>
        <p>Lier une ville de la base à sa page Wikidata officielle.</p>
        <form method="post" action="/link">
            <input type="text" name="ville" placeholder="Ville (ex: Lyon)" required>
            <input type="text" name="wiki_id" placeholder="ID Wikidata (ex: Q456)" required>
            <button type="submit" style="background-color: #008CBA;">Créer le lien</button>
        </form>
    </div>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route('/query', methods=['POST'])
def run_query():
    # On ajoute "u.wikidata_url" dans le RETURN
    cypher_query = """
    MATCH (c:Client)-[:SOUMET]->(d)-[:GENERE]->(o)-[:VALIDE]->(cmd:Commande)
    MATCH (cmd)-[:DECLENCHE]->(of:OrdreFab)-[:LOCALISE_A]->(u:Usine)
    MATCH (of)-[:PRODUIT]->(v:Vehicule)
    OPTIONAL MATCH (v)-[:SUBIT]->(sav:DossierSAV)-[:GENERE_FACT]->(f:FactureSAV)
    RETURN 
        c.nom AS Client, 
        v.vin AS Vin, 
        u.ville AS Usine, 
        u.wikidata_url AS Wiki,  // <--- ON RÉCUPÈRE LE LIEN ICI
        COALESCE(sav.description, "Aucun incident") AS Panne,
        COALESCE(f.total, "0.00") AS Facture
    """
    with driver.session() as session:
        result = session.run(cypher_query)
        data = [record.data() for record in result]
    
    return render_template_string(HTML_TEMPLATE, results=data)

@app.route('/link', methods=['POST'])
def add_link():
    ville = request.form['ville']
    wiki_id = request.form['wiki_id']
    
    # Ajoute une propriété 'wikidata_url' au nœud Usine correspondant
    cypher_query = """
    MATCH (u:Usine {ville: $ville})
    SET u.wikidata_url = 'https://www.wikidata.org/wiki/' + $wiki_id
    RETURN u
    """
    with driver.session() as session:
        session.run(cypher_query, ville=ville, wiki_id=wiki_id)
        
    return render_template_string(HTML_TEMPLATE, message=f"Lien Wikidata ajouté pour l'usine de {ville} !")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
