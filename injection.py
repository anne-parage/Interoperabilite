import json
import pandas as pd
import xml.etree.ElementTree as ET
from neo4j import GraphDatabase
import jsonschema
from jsonschema import validate
from sqlalchemy import create_engine

class DataInjector:
    def __init__(self, uri, user, password, mysql_cfg):
        self.db_url = (
            f"mysql+pymysql://{mysql_cfg['user']}:{mysql_cfg['password']}"
            f"@{mysql_cfg['host']}/{mysql_cfg['database']}"
        )
        self.engine = create_engine(self.db_url)

        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            summary = result.consume()
            if summary.counters.nodes_created == 0 and summary.counters.relationships_created == 0:
                if "MATCH" in query:
                    print(f"  ⚠ MATCH échoué (0 nœud/relation créé) — params: {parameters}")
            return summary

    def load_and_validate_json(self, data_file, schema_file):
        """Charge un fichier JSON et le valide immédiatement avec son schéma"""
        try:

            with open(data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            with open(schema_file, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            validate(instance=data, schema=schema)
            print(f"✓ {data_file} : Chargé et validé avec succès")
            return data

        except FileNotFoundError as e:
            print(f"✗ Fichier introuvable ({e.filename})")
        except json.JSONDecodeError as e:
            print(f"✗ Erreur de format JSON dans {data_file} ou {schema_file}")
        except jsonschema.exceptions.ValidationError as e:
            print(f"✗ Erreur de validation pour {data_file} : {e.message}")
        except Exception as e:
            print(f"✗ Erreur inattendue sur {data_file} : {e}")

        return None

    # --- M1 : VENTE (MySQL) ---
    def inject_m1_vente(self):
        print("Injection M1: Vente...")

        try:
            clients = pd.read_sql("SELECT * FROM clients", self.engine)
            demandes = pd.read_sql("SELECT * FROM demandes", self.engine)
            offres = pd.read_sql("SELECT * FROM offres", self.engine)
            commandes = pd.read_sql("SELECT * FROM commandes", self.engine)

            for _, c in clients.iterrows():
                self.query("""
                MERGE (cli:Client {IdClient: $id})
                SET cli.nom = $nom, cli.coordonnees = $coord
                """, {"id": str(c['IdClient']), "nom": str(c['nom']), "coord": str(c['coordonnees'])})

            for _, d in demandes.iterrows():
                self.query("""
                MERGE (c:Client {IdClient: $id_client})
                MERGE (dc:DemandeClient {IdDemande: $id_demande})
                SET dc.Date_Demande = $date, dc.carac_souhaitees = $carac
                MERGE (c)-[:EMET]->(dc)
                """, {"id_client": str(d['IdClient']), "id_demande": str(d['IdDemande']),
                      "date": str(d['Date_Demande']),
                      "carac": str(d['carac_souhaitees']) if 'carac_souhaitees' in d.index else str(d['caracteristiques_souhaitees'])})

            for _, o in offres.iterrows():
                self.query("""
                MERGE (dc:DemandeClient {IdDemande: $id_demande})
                MERGE (off:OffreCommerciale {IdOffre: $id_offre})
                SET off.prix = $prix, off.delai_propose = $delai, off.statut = $statut
                MERGE (dc)-[:DONNE_LIEU_A]->(off)
                """, {"id_demande": str(o['IdDemande']), "id_offre": str(o['IdOffre']),
                      "prix": float(o['prix']), "delai": str(o['delai_propose']), "statut": str(o['statut'])})

            for _, cmd in commandes.iterrows():
                self.query("""
                MERGE (off:OffreCommerciale {IdOffre: $id_offre})
                MERGE (bc:BonDeCommande {IdCommande: $id_cmd})
                SET bc.Date_commande = $date, bc.Conditions_de_vente = $cond
                MERGE (off)-[:ABOUTIT_A]->(bc)
                """, {"id_offre": str(cmd['IdOffre']), "id_cmd": str(cmd['IdCommande']),
                      "date": str(cmd['Date_commande']), "cond": str(cmd['Conditions_de_vente'])})

            print("✓ Injection M1 (MySQL) terminée.")

        except Exception as e:
            print(f"✗ Erreur lors de l'extraction MySQL: {e}")

    # --- M2 : PRODUCTION (JSON avec validation schéma) ---
    def inject_m2_production(self):
        print("Injection M2: Production avec validation des schémas...")

        # Définir les mappings fichiers de données -> schémas
        data_files = {
            'usines': ('M2_Production/usines.json', 'M2_Production/usines.schema.json'),
            'ordres_fabrication': ('M2_Production/ordres_fabrication.json', 'M2_Production/ordres_fabrication.schema.json'),
            'vehicules': ('M2_Production/vehicules.json', 'M2_Production/vehicules.schema.json'),
            'rapports_qualite': ('M2_Production/rapports_qualite.json', 'M2_Production/rapports_qualite.schema.json'),
            'livraisons': ('M2_Production/livraisons.json', 'M2_Production/livraisons.schema.json'),
            'entreprises_livraison': ('M2_Production/entreprises_livraison.json', 'M2_Production/entreprises_livraison.schema.json')
        }

        # Charger et valider toutes les données
        validated_data = {}
        print("=== VALIDATION DES SCHÉMAS ===")

        for key, (data_file, schema_file) in data_files.items():
            data = self.load_and_validate_json(data_file, schema_file)
            if data is None:
                return False # Arrêt si un fichier est invalide
            validated_data[key] = data

        print("=== DÉBUT DE L'INJECTION ===")

        # Injection des données validées
        print("\n Injection des usines...")
        for u in validated_data['usines']:
            self.query("""
            MERGE (us:Usine {IdUsine: $id})
            SET us.localisation = $loc
            """,
                      {"id": int(u['IdUsine']), "loc": u['localisation']})

        print(" Injection des ordres de fabrication...")
        for o in validated_data['ordres_fabrication']:
            self.query("""
            MERGE (bc:BonDeCommande {IdCommande: $id_cmd})
            MERGE (of:OrdreDeFabrication {IdOF: $id_of})
            SET of.Date_Planification = $dp, of.Date_lancement = $dl
            MERGE (bc)-[:DECLENCHE]->(of)
            """, {"id_cmd": str(o['IdCommande']), "id_of": str(o['IdOF']), "dp": o['Date_Planification'], "dl": o['Date_lancement']})

        print(" Injection des véhicules...")
        for v in validated_data['vehicules']:
            self.query("""
            MERGE (of:OrdreDeFabrication {IdOF: $id_of})
            MERGE (us:Usine {IdUsine: $id_usine})
            MERGE (veh:Vehicule {numero_de_serie: $vin})
            SET veh.modele = $mod
            MERGE (of)-[:PERMET_DE_FABRIQUER]->(veh)
            MERGE (veh)-[:ASSEMBLE_DANS]->(us)
            """, {"id_of": str(v['IdOF']), "id_usine": int(v['IdUsine']), "vin": str(v['numero_de_serie']), "mod": v['modele']})

        print(" Injection des rapports qualité...")
        for r in validated_data['rapports_qualite']:
            self.query("""
            MERGE (us:Usine {IdUsine: $id_usine})
            MERGE (rq:RapportQualite {IdRapport: $id})
            SET rq.Statut_conformite = $statut, rq.Date_controle = $date
            MERGE (us)-[:FAIT_L_OBJET_DE]->(rq)
            """, {"id_usine": int(r['IdUsine']), "id": str(r['IdRapport']), "statut": r['Statut_conformite'], "date": r['Date_controle']})

        print(" Injection des livraisons...")
        for l in validated_data['livraisons']:
            self.query("""
            MERGE (rq:RapportQualite {IdRapport: $id_rapport})
            MERGE (liv:Livraison {IdLivraison: $id})
            SET liv.Date_prevue = $dp, liv.Date_effective = $de,
                liv.lieu_livraison = $lieu, liv.Statut_livraison = $statut
            MERGE (rq)-[:CONFORMITE_VALIDEE]->(liv)
            """, {"id_rapport": str(l['IdRapport']), "id": str(l['IdLivraison']), "dp": l['Date_prevue'],
                  "de": str(l.get('Date_effective') or ''), "lieu": l['lieu_livraison'], "statut": l['Statut_livraison']})

        print(" Injection des entreprises de livraison...")
        for e in validated_data['entreprises_livraison']:
            self.query("""
            MERGE (liv:Livraison {IdLivraison: $id_liv})
            MERGE (ent:EntrepriseDeLivraison {nom: $nom})
            MERGE (liv)-[:LIVREE_PAR]->(ent)
            """, {"id_liv": str(e['IdLivraison']), "nom": e['nom']})

        print(" Injection M2 terminée avec succès !")
        return True

    # --- M3 : SAV (XML) ---
    def inject_m3_sav(self):
        print("Injection M3: SAV...")
        # 1. Demandes SAV
        tree_d = ET.parse('M3_SAV/dossiers_sav.xml')
        for d in tree_d.findall('.//demande_sav'):
            id_client = d.find('IdClient').text.strip()
            vin = d.find('numero_de_serie').text.strip()
            id_sav = d.get('IdSAV')
            date = d.find('Date_signalement').text.strip()
            desc = d.find('description').text.strip()
            self.query("""
            MERGE (c:Client {IdClient: $id_client})
            MERGE (ds:DemandeSAV {IdSAV: $id_sav})
            SET ds.Date_signalement = $date, ds.description = $desc, ds.numero_de_serie = $vin
            MERGE (c)-[:SIGNALE]->(ds)
            """, {"id_client": id_client, "vin": vin, "id_sav": id_sav, "date": date, "desc": desc})
            self.query("""
            MERGE (ds:DemandeSAV {IdSAV: $id_sav})
            MERGE (v:Vehicule {numero_de_serie: $vin})
            MERGE (ds)-[:CONCERNE]->(v)
            """, {"id_sav": id_sav, "vin": vin})

        # 2. Diagnostics et Réparations
        tree_i = ET.parse('M3_SAV/factures_sav.xml')
        for diag in tree_i.findall('.//diagnostic'):
            rep = diag.find('reparation')
            vin = diag.find('numero_de_serie').text.strip()
            id_diag = diag.get('IdDiagnostic')
            resultat = diag.find('resultat').text.strip()
            date_diag = diag.find('Date_diagnostic').text.strip()
            id_rep = rep.get('IdReparation')
            type_inter = rep.find('type_intervention').text.strip()
            date_rep = rep.find('Date_intervention').text.strip()

            # D'abord essayer de relier au véhicule existant
            summary = self.query("""
            MATCH (v:Vehicule {numero_de_serie: $vin})
            CREATE (v)-[:DONNE_LIEU_A]->(d:Diagnostic {IdDiagnostic: $id_diag, resultat: $res, Date_diagnostic: $date_diag})
            CREATE (d)-[:DECLENCHE]->(:Reparation {IdReparation: $id_rep, type_intervention: $type, Date_intervention: $date_rep})
            """, {"vin": vin, "id_diag": id_diag, "res": resultat, "date_diag": date_diag,
                  "id_rep": id_rep, "type": type_inter, "date_rep": date_rep})

            # Si le véhicule n'existe pas dans M2, relier à la DemandeSAV correspondante
            if summary.counters.nodes_created == 0:
                self.query("""
                MATCH (ds:DemandeSAV {numero_de_serie: $vin})
                CREATE (ds)-[:DONNE_LIEU_A]->(d:Diagnostic {IdDiagnostic: $id_diag, resultat: $res, Date_diagnostic: $date_diag})
                CREATE (d)-[:DECLENCHE]->(:Reparation {IdReparation: $id_rep, type_intervention: $type, Date_intervention: $date_rep})
                """, {"vin": vin, "id_diag": id_diag, "res": resultat, "date_diag": date_diag,
                      "id_rep": id_rep, "type": type_inter, "date_rep": date_rep})

if __name__ == "__main__":
    mysql_config = {
        'host': 'localhost',
        "user": "root",
        "password": "root",
        "database": "Vente"}

    injector = DataInjector("bolt://127.0.0.1:7687", "neo4j", "eTyDWf1CD7gvr5Qg1SgMUIBccgzbm0hTYHzNJ3hAq2M", mysql_config)

    with injector.driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    print("Base Neo4j nettoyée.")
    injector.inject_m1_vente()
    injector.inject_m2_production()
    injector.inject_m3_sav()
    injector.close()
    print("Injection complète terminée avec succès !")