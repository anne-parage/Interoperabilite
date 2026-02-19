import json
import pandas as pd
import xml.etree.ElementTree as ET
from neo4j import GraphDatabase
import jsonschema
from jsonschema import validate

class DataInjector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self, query, parameters=None):
        with self.driver.session() as session:
            session.run(query, parameters)

    def validate_json_data(self, data, schema_path):
        """Valide les données JSON contre leur schéma"""
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Le schéma définit un array, donc on valide le tableau complet
            validate(instance=data, schema=schema)
            print(f"✓ Validation réussie pour {schema_path}")
            return True

        except jsonschema.exceptions.ValidationError as e:
            print(f"✗ Erreur de validation pour {schema_path}:")
            print(f"  - {e.message}")
            if e.absolute_path:
                print(f"  - Chemin d'erreur: {' -> '.join(map(str, e.absolute_path))}")
            return False
        except Exception as e:
            print(f"✗ Erreur lors de la validation de {schema_path}: {e}")
            return False

    def load_and_validate_json(self, data_file, schema_file):
        """Charge et valide un fichier JSON avec son schéma"""
        try:
            # Charger les données
            with open(data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Valider avec le schéma
            if self.validate_json_data(data, schema_file):
                print(f"✓ Fichier {data_file} chargé et validé avec succès")
                return data
            else:
                print(f"✗ Échec de validation pour {data_file} - Arrêt du traitement")
                return None

        except FileNotFoundError as e:
            print(f"✗ Fichier introuvable: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"✗ Erreur de format JSON dans {data_file}: {e}")
            return None
        except Exception as e:
            print(f"✗ Erreur lors du chargement de {data_file}: {e}")
            return None

    # --- M1 : VENTE (CSV) ---
    def inject_m1_vente(self):
        print("Injection M1: Vente...")
        clients = pd.read_csv('M1_Vente/clients.csv')
        demandes = pd.read_csv('M1_Vente/demandes.csv')
        offres = pd.read_csv('M1_Vente/offres.csv')
        commandes = pd.read_csv('M1_Vente/commandes.csv')

        for _, c in clients.iterrows():
            self.query("CREATE (:Client {IdClient: $id, nom: $nom, coordonnees: $coord})",
                       {"id": c['IdClient'], "nom": c['nom'], "coord": c['coordonnees']})

        for _, d in demandes.iterrows():
            self.query("""
            MATCH (c:Client {IdClient: $id_client})
            CREATE (c)-[:EMET]->(:DemandeClient {IdDemande: $id_demande, Date_Demande: $date, caracteristiques_souhaitees: $carac})
            """, {"id_client": d['IdClient'], "id_demande": d['IdDemande'], "date": d['Date_Demande'], "carac": d['caracteristiques_souhaitees']})

        for _, o in offres.iterrows():
            self.query("""
            MATCH (d:DemandeClient {IdDemande: $id_demande})
            CREATE (d)-[:DONNE_LIEU_A]->(:OffreCommerciale {IdOffre: $id_offre, prix: $prix, delai_propose: $delai, statut: $statut})
            """, {"id_demande": o['IdDemande'], "id_offre": o['IdOffre'], "prix": o['prix'], "delai": o['delai_propose'], "statut": o['statut']})

        for _, cmd in commandes.iterrows():
            self.query("""
            MATCH (o:OffreCommerciale {IdOffre: $id_offre})
            CREATE (o)-[:ABOUTIT_A]->(:BonDeCommande {IdCommande: $id_cmd, Date_commande: $date, Conditions_de_vente: $cond})
            """, {"id_offre": cmd['IdOffre'], "id_cmd": cmd['IdCommande'], "date": cmd['Date_commande'], "cond": cmd['Conditions_de_vente']})

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
            print(f"\n Validation de {key}...")
            data = self.load_and_validate_json(data_file, schema_file)
            if data is None:
                print(f" ARRÊT: Impossible de valider {data_file}")
                return False
            validated_data[key] = data

        print("=== DÉBUT DE L'INJECTION ===")

        # Injection des données validées
        print("\n Injection des usines...")
        for u in validated_data['usines']:
            self.query("CREATE (:Usine {IdUsine: $id, localisation: $loc})",
                      {"id": u['IdUsine'], "loc": u['localisation']})

        print(" Injection des ordres de fabrication...")
        for o in validated_data['ordres_fabrication']:
            self.query("""
            MATCH (bc:BonDeCommande {IdCommande: $id_cmd})
            CREATE (bc)-[:DECLENCHE]->(:OrdreDeFabrication {IdOF: $id_of, Date_Planification: $dp, Date_lancement: $dl})
            """, {"id_cmd": o['IdCommande'], "id_of": o['IdOF'], "dp": o['Date_Planification'], "dl": o['Date_lancement']})

        print(" Injection des véhicules...")
        for v in validated_data['vehicules']:
            self.query("""
            MATCH (of:OrdreDeFabrication {IdOF: $id_of}), (u:Usine {IdUsine: $id_usine})
            CREATE (of)-[:PERMET_DE_FABRIQUER]->(veh:Vehicule {numero_de_serie: $vin, modele: $mod})
            CREATE (veh)-[:ASSEMBLE_DANS]->(u)
            """, {"id_of": v['IdOF'], "id_usine": v['IdUsine'], "vin": v['numero_de_serie'], "mod": v['modele']})

        print(" Injection des rapports qualité...")
        for r in validated_data['rapports_qualite']:
            self.query("""
            MATCH (u:Usine {IdUsine: $id_usine})
            CREATE (u)-[:FAIT_L_OBJET_DE]->(:RapportQualite {IdRapport: $id, Statut_conformite: $statut, Date_controle: $date})
            """, {"id_usine": r['IdUsine'], "id": r['IdRapport'], "statut": r['Statut_conformite'], "date": r['Date_controle']})

        print(" Injection des livraisons...")
        for l in validated_data['livraisons']:
            self.query("""
            MATCH (rq:RapportQualite {IdRapport: $id_rapport})
            CREATE (rq)-[:CONFORMITE_VALIDEE]->(:Livraison {
                IdLivraison: $id, Date_prevue: $dp, Date_effective: $de, 
                lieu_livraison: $lieu, Statut_livraison: $statut
            })
            """, {"id_rapport": l['IdRapport'], "id": l['IdLivraison'], "dp": l['Date_prevue'], 
                  "de": l.get('Date_effective'), "lieu": l['lieu_livraison'], "statut": l['Statut_livraison']})

        print(" Injection des entreprises de livraison...")
        for e in validated_data['entreprises_livraison']:
            self.query("""
            MATCH (liv:Livraison {IdLivraison: $id_liv})
            CREATE (liv)-[:CONFORMITE_VALIDEE]->(:EntrepriseDeLivraison {nom: $nom})
            """, {"id_liv": e['IdLivraison'], "nom": e['nom']})

        print(" Injection M2 terminée avec succès !")
        return True

    # --- M3 : SAV (XML adapté au Diagramme 2) ---
    def inject_m3_sav(self):
        print("Injection M3: SAV...")
        # 1. Demandes SAV
        tree_d = ET.parse('M3_SAV/dossiers_sav.xml')
        for d in tree_d.findall('demande_sav'):
            self.query("""
            MATCH (c:Client {IdClient: $id_client}), (v:Vehicule {numero_de_serie: $vin})
            CREATE (c)-[:SIGNALE]->(ds:DemandeSAV {IdSAV: $id_sav, Date_signalement: $date, description: $desc})
            CREATE (ds)-[:CONCERNE]->(v)
            """, {"id_client": d.find('IdClient').text, "vin": d.find('numero_de_serie').text, 
                  "id_sav": d.get('IdSAV'), "date": d.find('Date_signalement').text, "desc": d.find('description').text})

        # 2. Diagnostics et Réparations
        tree_i = ET.parse('M3_SAV/factures_sav.xml')
        for diag in tree_i.findall('diagnostic'):
            rep = diag.find('reparation')
            self.query("""
            MATCH (v:Vehicule {numero_de_serie: $vin})
            CREATE (v)-[:DONNE_LIEU_A]->(d:Diagnostic {IdDiagnostic: $id_diag, resultat: $res, Date_diagnostic: $date_diag})
            CREATE (d)-[:DECLENCHE]->(:Reparation {IdReparation: $id_rep, type_intervention: $type, Date_intervention: $date_rep})
            """, {
                "vin": diag.find('numero_de_serie').text, "id_diag": diag.get('IdDiagnostic'),
                "res": diag.find('resultat').text, "date_diag": diag.find('Date_diagnostic').text,
                "id_rep": rep.get('IdReparation'), "type": rep.find('type_intervention').text, "date_rep": rep.find('Date_intervention').text
            })

if __name__ == "__main__":
    injector = DataInjector("bolt://127.0.0.1:7687", "neo4j", "eTyDWf1CD7gvr5Qg1SgMUIBccgzbm0hTYHzNJ3hAq2M")
    injector.query("MATCH (n) DETACH DELETE n") # Nettoyage
    injector.inject_m1_vente()
    injector.inject_m2_production()
    injector.inject_m3_sav()
    injector.close()
    print("Injection complète terminée avec succès !")