#!/usr/bin/env python3
"""
Générateur de données pour tous les fichiers du projet.
Modifiez N_BASE pour changer le volume de données généré.
Les fichiers existants seront remplacés.
"""

import json
import csv
import random
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import date, timedelta
import os

# ============================================================
#  PARAMÈTRE PRINCIPAL — changez cette valeur
# ============================================================
N_BASE = 1000          # nombre de clients / demandes / offres / commandes
#  Les autres entités s'adaptent automatiquement :
#  - livraisons       ≈ N_BASE
#  - ordres_fabrication ≈ N_BASE
#  - véhicules        ≈ N_BASE
#  - rapports qualité ≈ N_BASE * 1
#  - dossiers SAV     ≈ N_BASE * 0.5  (avec et sans pannes)
#  - factures SAV     ≈ dossiers SAV
#  - entreprises livraison ≈ N_BASE
# ============================================================

BASE_DIR = os.path.dirname(__file__)
VENTE_DIR = os.path.join(BASE_DIR, "M1_Vente")
PROD_DIR = os.path.join(BASE_DIR, "M2_Production")
SAV_DIR = os.path.join(BASE_DIR, "M3_SAV")


def ensure_dir(path):
    if not os.path.isdir(path):
        raise FileNotFoundError(f"Répertoire introuvable : {path}")


for directory in (VENTE_DIR, PROD_DIR, SAV_DIR):
    ensure_dir(directory)


def path_vente(name):
    return os.path.join(VENTE_DIR, name)


def path_prod(name):
    return os.path.join(PROD_DIR, name)


def path_sav(name):
    return os.path.join(SAV_DIR, name)

random.seed(42)

# ─── Utilitaires ────────────────────────────────────────────

def rand_date(start="2026-01-01", end="2026-12-31"):
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    return (s + timedelta(days=random.randint(0, (e - s).days))).isoformat()

def add_days(d_str, n):
    return (date.fromisoformat(d_str) + timedelta(days=n)).isoformat()

def prettify_xml(elem):
    rough = ET.tostring(elem, encoding="unicode")
    return minidom.parseString(rough).toprettyxml(indent="    ", encoding=None)

# ─── Listes de référence ────────────────────────────────────

PRENOMS = ["Jean","Alice","Bob","Claire","Damien","Sophie","Thomas","Marie","Lucas","Emma",
           "Hugo","Lea","Nathan","Camille","Julien","Manon","Antoine","Chloe","Maxime","Laura",
           "Nicolas","Julie","Alexandre","Sarah","Vincent","Pauline","Pierre","Charlotte","Louis",
           "Anais","Romain","Oceane","Florian","Mathilde","Kevin","Lucie","Clement","Marine",
           "Theo","Elise","Quentin","Margaux","Sebastien","Ines","Guillaume","Amandine","Adrien",
           "Juliette","Raphael","Laure","Benoit","Elodie","Mickael","Stephanie","Cyril","Aurore",
           "Arnaud","Melanie","Gregory","Vanessa","Franck","Isabelle","Laurent","Nathalie","Pascal",
           "Sandrine","Eric","Veronique","Christophe","Catherine","David","Patricia","Philippe",
           "Brigitte","Michel","Sylvie","Alain","Martine","Bernard","Monique","Patrick","Annie",
           "Daniel","Josette","Claude","Denise","Marcel","Yvette","Henri","Colette","Jacques","Suzanne"]

NOMS = ["Dupont","Martin","Smith","Morel","Roux","Laurent","Bernard","Petit","Girard","Lefevre",
        "Moreau","Fournier","Simon","Mercier","Duval","Blanc","Robin","Lemaire","Chevalier","Garnier",
        "Garcia","Perrin","Bonnet","Dubois","Lambert","Fontaine","Rousseau","Clement","Dufour","Henry",
        "Renard","Moulin","Picard","Andre","Roche","Brunet","Colin","Marchand","Dumont","Legrand",
        "Denis","Rey","Maillard","Brun","Caron","Leroy","Gauthier","Vidal","Arnaud","Giraud",
        "David","Bertrand","Thomas","Robert","Richard","Leclerc","Schmitt","Martinez","Hernandez",
        "Lopez","Gonzalez","Perez","Sanchez","Torres","Ramirez","Flores","Rivera","Diaz","Morales",
        "Muller","Weber","Meyer","Wagner","Becker","Hoffmann","Koch","Richter","Bauer","Wolf"]

DOMAINES = ["gmail.com","yahoo.fr","outlook.fr","free.fr","orange.fr","bbox.fr","web.fr","email.com","sfr.fr","laposte.net"]

VILLES = ["Paris","Lyon","Marseille","Bordeaux","Toulouse","Nantes","Strasbourg","Lille","Rennes",
          "Montpellier","Nice","Grenoble","Rouen","Toulon","Saint-Etienne","Dijon","Angers",
          "Le Mans","Reims","Brest","Clermont-Ferrand","Caen","Metz","Nancy","Avignon"]

TYPES_VOITURE = ["Citadine","Berline","SUV","Break","Coupé","Monospace"]
COULEURS = ["Rouge","Bleu","Noire","Blanche","Verte","Grise","Argent","Beige"]

CONDITIONS_VENTE = ["Paiement comptant","Paiement 3x","Credit","Leasing","Cheque"]

MODELES = ["Modele A","Modele B","Modele C","Modele D","Modele E"]

ENTREPRISES_LIV = ["EcoTransit","TransExpress","SpeedLog","DeliveryPlus","LogiRapide"]

STATUTS_LIV = ["Livré","En transit","En attente","Retardé"]
STATUTS_LIV_WEIGHTS = [0.70, 0.15, 0.08, 0.07]  # majorité livrée

USINES = [
    {"IdUsine": 1, "localisation": "Paris"},
    {"IdUsine": 2, "localisation": "Lyon"},
    {"IdUsine": 3, "localisation": "Marseille"},
    {"IdUsine": 4, "localisation": "Sochaux"},
    {"IdUsine": 5, "localisation": "Toulouse"},
    {"IdUsine": 6, "localisation": "Bordeaux"},
    {"IdUsine": 7, "localisation": "Nantes"},
    {"IdUsine": 8, "localisation": "Strasbourg"},
    {"IdUsine": 9, "localisation": "Lille"},
    {"IdUsine": 10, "localisation": "Rennes"},
]

# ─── Pannes et descriptions SAV ─────────────────────────────

PANNES = [
    # Moteur
    "Voyant moteur allume","Bruit moteur suspect","Injecteur encrasse","Vanne EGR encrassee",
    "Turbo defaillant","Probleme injection","Courroie de distribution usee","Fuite huile moteur",
    "Surchauffe moteur","Demarreur grince",
    # Freinage
    "Freins qui grincent","Pédale de frein molle","Disques de frein uses","Plaquettes usees",
    "Fuite liquide de frein",
    # Electrique
    "Batterie decharge repetee","Capteur ABS defaillant","Alternateur defaillant",
    "Court-circuit tableau de bord","Probleme demarrage electrique",
    # Suspension / direction
    "Amortisseurs uses","Bras de suspension fissure","Volant qui vibre","Direction dure",
    "Rotule de direction usee",
    # Divers
    "Climatisation en panne","Fuite liquide de refroidissement","Boite de vitesses qui saute",
    "Embrayage qui glisse","Pneu crevé repetition",
]

DIAGNOSTICS = [
    "Bras de suspension fissure","Bobine allumage defaillante","Turbo jeu axial excessif",
    "Faisceau electrique oxyde","Joint de carter use","Capteur pression rail HS",
    "Disques voiles","Etrier de frein grippe","Alternateur hors service","Batterie HS",
    "Vanne EGR colmatee","Sonde lambda defaillante","Courroie accessoire craquelée",
    "Pompe a eau fuyante","Thermostat bloque ouvert","Calculateur moteur defaillant",
    "Roulement de roue use","Biellette de direction tordue","Amortisseur perce","Embrayage use",
    "Injecteur colmate","Turbo usure palier","Filtre a particules sature","Pression pneu anormale",
    "Ressort de suspension casse",
]

REPARATIONS = [
    "Changement plaquettes et disques","Rectification disques de frein","Remplacement capteur PMH",
    "Remplacement joint carter","Remplacement alternateur","Remplacement batterie",
    "Nettoyage injecteurs","Remplacement courroie distribution","Remplacement pompe a eau",
    "Purge et remplacement liquide refroidissement","Remplacement bobine allumage",
    "Remplacement vanne EGR","Remplacement sonde lambda","Remplacement filtre a particules",
    "Remplacement amortisseurs","Remplacement rotule de direction","Remplacement biellette",
    "Remplacement embrayage complet","Revision boite de vitesses","Remplacement turbo",
    "Remplacement thermostat","Remplacement pompe a carburant","Remplacement capteur ABS",
    "Equilibrage et remplacement pneumatiques","Reprogrammation calculateur",
]

# ═══════════════════════════════════════════════════════════════
#  GÉNÉRATION
# ═══════════════════════════════════════════════════════════════

print(f"=== Génération de données (N_BASE={N_BASE}) ===\n")

# ─── 1. CLIENTS (CSV) ───────────────────────────────────────
clients = []
used_names = set()
for i in range(1, N_BASE + 1):
    while True:
        p = random.choice(PRENOMS)
        n = random.choice(NOMS)
        full = f"{p} {n}"
        if full not in used_names:
            used_names.add(full)
            break
    slug = f"{p.lower()}.{n.lower()}"
    dom = random.choice(DOMAINES)
    clients.append({
        "IdClient": f"C{i:03d}",
        "nom": full,
        "coordonnees": f"{slug}@{dom}"
    })

with open(path_vente("clients.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["IdClient","nom","coordonnees"])
    w.writeheader()
    w.writerows(clients)
print(f"✓ clients.csv          → {len(clients)} lignes")

# ─── 2. DEMANDES (CSV) ──────────────────────────────────────
demandes = []
for i in range(1, N_BASE + 1):
    client = random.choice(clients)
    typ = random.choice(TYPES_VOITURE)
    couleur = random.choice(COULEURS)
    demandes.append({
        "IdDemande": f"D{i+100:03d}",
        "IdClient": client["IdClient"],
        "Date_Demande": rand_date("2026-01-01","2026-09-30"),
        "caracteristiques_souhaitees": f"{typ} {couleur}"
    })

with open(path_vente("demandes.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["IdDemande","IdClient","Date_Demande","caracteristiques_souhaitees"])
    w.writeheader()
    w.writerows(demandes)
print(f"✓ demandes.csv         → {len(demandes)} lignes")

# ─── 3. OFFRES (CSV) ────────────────────────────────────────
delais = ["15 jours","20 jours","30 jours","35 jours","40 jours","45 jours"]
statuts_offre = ["Accepte","Refuse","En attente"]
statuts_offre_w = [0.75, 0.15, 0.10]

offres = []
for i, dem in enumerate(demandes, 1):
    statut = random.choices(statuts_offre, weights=statuts_offre_w)[0]
    offres.append({
        "IdOffre": f"O{i+200:03d}",
        "IdDemande": dem["IdDemande"],
        "prix": random.randint(10000, 75000),
        "delai_propose": random.choice(delais),
        "statut": statut
    })

with open(path_vente("offres.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["IdOffre","IdDemande","prix","delai_propose","statut"])
    w.writeheader()
    w.writerows(offres)
print(f"✓ offres.csv           → {len(offres)} lignes")

# ─── 4. COMMANDES (CSV) — seulement offres acceptées ────────
offres_acceptees = [o for o in offres if o["statut"] == "Accepte"]
commandes = []
for i, off in enumerate(offres_acceptees, 1):
    commandes.append({
        "IdCommande": f"CMD{i+500:03d}",
        "IdOffre": off["IdOffre"],
        "Date_commande": rand_date("2026-01-01","2026-09-30"),
        "Conditions_de_vente": random.choice(CONDITIONS_VENTE)
    })

with open(path_vente("commandes.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["IdCommande","IdOffre","Date_commande","Conditions_de_vente"])
    w.writeheader()
    w.writerows(commandes)
print(f"✓ commandes.csv        → {len(commandes)} lignes")

# ─── 5. USINES (JSON) ───────────────────────────────────────
with open(path_prod("usines.json"), "w", encoding="utf-8") as f:
    json.dump(USINES, f, ensure_ascii=False, indent=2)
print(f"✓ usines.json          → {len(USINES)} usines (inchangé)")

# ─── 6. ORDRES DE FABRICATION (JSON) ────────────────────────
ordres = []
for i, cmd in enumerate(commandes, 1):
    plan = rand_date("2026-01-01","2026-10-01")
    launch = add_days(plan, random.randint(3, 10))
    ordres.append({
        "IdOF": f"OF{i:03d}",
        "IdCommande": cmd["IdCommande"],
        "Date_Planification": plan,
        "Date_lancement": launch
    })

with open(path_prod("ordres_fabrication.json"), "w", encoding="utf-8") as f:
    json.dump(ordres, f, ensure_ascii=False, indent=2)
print(f"✓ ordres_fabrication.json → {len(ordres)} ordres")

# ─── 7. VÉHICULES (JSON) ────────────────────────────────────
def rand_vin():
    return "VIN" + "".join([str(random.randint(0,9)) for _ in range(9)])

vehicules = []
used_vins = set()
for of in ordres:
    while True:
        vin = rand_vin()
        if vin not in used_vins:
            used_vins.add(vin)
            break
    usine = random.choice(USINES)
    vehicules.append({
        "numero_de_serie": vin,
        "IdOF": of["IdOF"],
        "IdUsine": usine["IdUsine"],
        "modele": random.choice(MODELES)
    })

with open(path_prod("vehicules.json"), "w", encoding="utf-8") as f:
    json.dump(vehicules, f, ensure_ascii=False, indent=2)
print(f"✓ vehicules.json       → {len(vehicules)} véhicules")

# ─── 8. RAPPORTS QUALITÉ (JSON) ─────────────────────────────
# ~10 % de non-conformités
n_rapports = max(len(vehicules), N_BASE)
rapports = []
for i in range(1, n_rapports + 1):
    usine = random.choice(USINES)
    conforme = random.choices(["Conforme","Non Conforme"], weights=[0.90, 0.10])[0]
    rapports.append({
        "IdRapport": f"RQ{i:03d}",
        "IdUsine": usine["IdUsine"],
        "Statut_conformite": conforme,
        "Date_controle": rand_date("2026-01-01","2026-12-31")
    })

with open(path_prod("rapports_qualite.json"), "w", encoding="utf-8") as f:
    json.dump(rapports, f, ensure_ascii=False, indent=2)
nc = sum(1 for r in rapports if r["Statut_conformite"] == "Non Conforme")
print(f"✓ rapports_qualite.json → {len(rapports)} rapports ({nc} non-conformes)")

# ─── 9. LIVRAISONS (JSON) ───────────────────────────────────
# On associe chaque commande à une livraison
livraisons_data = []
for i, cmd in enumerate(commandes, 1):
    rapport = random.choice(rapports)
    date_prev = rand_date("2026-02-01","2026-12-15")
    statut = random.choices(STATUTS_LIV, weights=STATUTS_LIV_WEIGHTS)[0]
    if statut == "Livré":
        date_eff = add_days(date_prev, random.randint(0, 5))
    elif statut == "Retardé":
        date_eff = add_days(date_prev, random.randint(6, 20))
    else:
        date_eff = None

    ville = random.choice(VILLES)
    livraisons_data.append({
        "IdLivraison": f"LIV{i:03d}",
        "IdRapport": rapport["IdRapport"],
        "Date_prevue": date_prev,
        "Date_effective": date_eff,
        "lieu_livraison": ville,
        "Statut_livraison": statut
    })

with open(path_prod("livraisons.json"), "w", encoding="utf-8") as f:
    json.dump(livraisons_data, f, ensure_ascii=False, indent=2)
print(f"✓ livraisons.json      → {len(livraisons_data)} livraisons")

# ─── 10. ENTREPRISES LIVRAISON (JSON) ───────────────────────
ent_livraisons = []
for liv in livraisons_data:
    ent_livraisons.append({
        "nom": random.choice(ENTREPRISES_LIV),
        "IdLivraison": liv["IdLivraison"]
    })

with open(path_prod("entreprises_livraison.json"), "w", encoding="utf-8") as f:
    json.dump(ent_livraisons, f, ensure_ascii=False, indent=2)
print(f"✓ entreprises_livraison.json → {len(ent_livraisons)} entrées")

# ─── 11. DOSSIERS SAV (XML) — avec et sans pannes ───────────
# ~50 % des véhicules ont un dossier SAV (pannes réelles)
# ~20 % ont un SAV "revision" (sans vraie panne)
n_sav_pannes = int(len(vehicules) * 0.50)
n_sav_revision = int(len(vehicules) * 0.20)

vins_all = [v["numero_de_serie"] for v in vehicules]
clients_ids = [c["IdClient"] for c in clients]

vins_pannes = random.sample(vins_all, min(n_sav_pannes, len(vins_all)))
remaining_vins = [v for v in vins_all if v not in vins_pannes]
vins_revision = random.sample(remaining_vins, min(n_sav_revision, len(remaining_vins)))

root_sav = ET.Element("demandes")
sav_counter = 901
sav_vins_used = []  # pour lier avec factures

# Dossiers AVEC pannes
for vin in vins_pannes:
    d = ET.SubElement(root_sav, "demande_sav", IdSAV=f"SAV_{sav_counter}")
    ET.SubElement(d, "IdClient").text = random.choice(clients_ids)
    ET.SubElement(d, "numero_de_serie").text = vin
    ET.SubElement(d, "Date_signalement").text = rand_date("2026-06-01","2026-12-31")
    ET.SubElement(d, "description").text = random.choice(PANNES)
    sav_vins_used.append((f"SAV_{sav_counter}", vin, "panne"))
    sav_counter += 1

# Dossiers SANS panne (révision, entretien)
descriptions_revision = [
    "Revision periodique 30 000 km","Revision annuelle","Controle technique pre-passage",
    "Changement huile moteur de routine","Verification freins de routine",
    "Remplacement filtres periodique","Controle eclairage et pneumatiques",
    "Verification niveaux generale","Entretien climatisation","Controle pre-livraison",
]
for vin in vins_revision:
    d = ET.SubElement(root_sav, "demande_sav", IdSAV=f"SAV_{sav_counter}")
    ET.SubElement(d, "IdClient").text = random.choice(clients_ids)
    ET.SubElement(d, "numero_de_serie").text = vin
    ET.SubElement(d, "Date_signalement").text = rand_date("2026-06-01","2026-12-31")
    ET.SubElement(d, "description").text = random.choice(descriptions_revision)
    sav_vins_used.append((f"SAV_{sav_counter}", vin, "revision"))
    sav_counter += 1

xml_sav_str = prettify_xml(root_sav)
with open(path_sav("dossiers_sav.xml"), "w", encoding="utf-8") as f:
    f.write(xml_sav_str)
print(f"✓ dossiers_sav.xml     → {len(sav_vins_used)} dossiers ({n_sav_pannes} pannes + {n_sav_revision} révisions)")

# ─── 12. FACTURES SAV (XML) ─────────────────────────────────
# Chaque dossier SAV génère un diagnostic + réparation + facture
root_fac = ET.Element("interventions")
for idx, (sav_id, vin, nature) in enumerate(sav_vins_used, 1):
    diag_elem = ET.SubElement(root_fac, "diagnostic", IdDiagnostic=f"DIAG_{idx}")
    ET.SubElement(diag_elem, "numero_de_serie").text = vin

    if nature == "panne":
        resultat = random.choice(DIAGNOSTICS)
        type_interv = random.choice(REPARATIONS)
        date_diag = rand_date("2026-06-01","2026-12-20")
        date_rep = add_days(date_diag, random.randint(1, 7))
        montant = random.randint(150, 3500)
    else:
        resultat = "Aucune anomalie detectee"
        type_interv = "Entretien standard"
        date_diag = rand_date("2026-06-01","2026-12-20")
        date_rep = add_days(date_diag, 1)
        montant = random.randint(80, 350)

    ET.SubElement(diag_elem, "resultat").text = resultat
    ET.SubElement(diag_elem, "Date_diagnostic").text = date_diag

    rep_elem = ET.SubElement(diag_elem, "reparation", IdReparation=f"REP_{idx}")
    ET.SubElement(rep_elem, "type_intervention").text = type_interv
    ET.SubElement(rep_elem, "Date_intervention").text = date_rep

    fac_elem = ET.SubElement(rep_elem, "facture", IdFacture=f"FAC_{idx}")
    ET.SubElement(fac_elem, "montant_HT").text = str(montant)
    ET.SubElement(fac_elem, "TVA").text = "20"
    ET.SubElement(fac_elem, "montant_TTC").text = str(round(montant * 1.20, 2))
    ET.SubElement(fac_elem, "Date_facture").text = add_days(date_rep, 1)
    ET.SubElement(fac_elem, "statut_paiement").text = random.choices(
        ["Payé","En attente","Impayé"], weights=[0.75, 0.18, 0.07])[0]

xml_fac_str = prettify_xml(root_fac)
with open(path_sav("factures_sav.xml"), "w", encoding="utf-8") as f:
    f.write(xml_fac_str)
print(f"✓ factures_sav.xml     → {len(sav_vins_used)} interventions facturées")

# ─── Résumé ─────────────────────────────────────────────────
print("\n=== Résumé final ===")
print(f"  Clients              : {len(clients)}")
print(f"  Demandes             : {len(demandes)}")
print(f"  Offres               : {len(offres)}")
print(f"  Commandes (accept.)  : {len(commandes)}")
print(f"  Ordres fabrication   : {len(ordres)}")
print(f"  Véhicules            : {len(vehicules)}")
print(f"  Rapports qualité     : {len(rapports)}  ({nc} non-conformes)")
print(f"  Livraisons           : {len(livraisons_data)}")
print(f"  Entreprises livraison: {len(ent_livraisons)}")
print(f"  Dossiers SAV         : {len(sav_vins_used)}  ({n_sav_pannes} pannes / {n_sav_revision} révisions)")
print(f"  Interventions/Fact.  : {len(sav_vins_used)}")
print("\nFichiers générés :")
print(f"  Vente      → {VENTE_DIR}")
print(f"  Production → {PROD_DIR}")
print(f"  SAV        → {SAV_DIR}")
