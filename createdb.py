import sqlite3
import csv

# Chemin vers le fichier CSV
csv_file = "data.csv"

# Chemin vers la base de données SQLite
db_file = "dvf.db"

# Connexion à la base de données SQLite
conn = sqlite3.connect(db_file)
cursor = conn.cursor()


# Création de la table dans la base de données SQLite
cursor.execute('''CREATE TABLE IF NOT EXISTS dvf (
                    id_mutation TEXT,
                    date_mutation TEXT,
                    numero_disposition TEXT,
                    nature_mutation TEXT,
                    valeur_fonciere INTEGER,
                    adresse_numero TEXT,
                    adresse_suffixe TEXT,
                    adresse_nom_voie TEXT,
                    adresse_code_voie TEXT,
                    code_postal TEXT,
                    code_commune TEXT,
                    nom_commune TEXT,
                    code_departement TEXT,
                    ancien_code_commune TEXT,
                    ancien_nom_commune TEXT,
                    id_parcelle TEXT,
                    ancien_id_parcelle TEXT,
                    numero_volume TEXT,
                    lot1_numero TEXT,
                    lot1_surface_carrez TEXT,
                    lot2_numero TEXT,
                    lot2_surface_carrez TEXT,
                    lot3_numero TEXT,
                    lot3_surface_carrez TEXT,
                    lot4_numero TEXT,
                    lot4_surface_carrez TEXT,
                    lot5_numero TEXT,
                    lot5_surface_carrez TEXT,
                    nombre_lots INTEGER,
                    code_type_local TEXT,
                    type_local TEXT,
                    surface_reelle_bati INTEGER,
                    nombre_pieces_principales INTEGER,
                    code_nature_culture TEXT,
                    nature_culture TEXT,
                    code_nature_culture_speciale TEXT,
                    nature_culture_speciale TEXT,
                    surface_terrain INTEGER,
                    longitude REAL,
                    latitude REAL,
                    CONSTRAINT unique_row UNIQUE (id_mutation, date_mutation, numero_disposition)
                )''')

# Lecture du fichier CSV et insertion des données dans la base de données SQLite
with open(csv_file, newline='', encoding='utf-8') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    next(csvreader)  # Skip header row
    for row in csvreader:
        cursor.execute('''INSERT OR IGNORE INTO dvf VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', row)

# Commit des changements et fermeture de la connexion
conn.commit()
conn.close()

print("La base de données SQLite a été créée avec succès à partir du fichier CSV, en excluant les valeurs en double.")
