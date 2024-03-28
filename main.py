from fastapi import FastAPI, HTTPException
from sqlmodel import Field, SQLModel, create_engine, Session, select
from databases import DatabaseURL, Database
import csv
from pathlib import Path

# Définition du modèle SQLModel pour les données DVF
class Dvf(SQLModel, table=True):
    id_mutation: str = Field(primary_key=True)
    date_mutation: str
    numero_disposition: str
    nature_mutation: str
    valeur_fonciere: float
    adresse_numero: str
    adresse_suffixe: str
    adresse_nom_voie: str
    adresse_code_voie: str
    code_postal: str
    code_commune: str
    nom_commune: str
    code_departement: str
    ancien_code_commune: str
    ancien_nom_commune: str
    id_parcelle: str
    ancien_id_parcelle: str
    numero_volume: str
    lot1_numero: str
    lot1_surface_carrez: float
    lot2_numero: str
    lot2_surface_carrez: float
    lot3_numero: str
    lot3_surface_carrez: float
    lot4_numero: str
    lot4_surface_carrez: float
    lot5_numero: str
    lot5_surface_carrez: float
    nombre_lots: int
    code_type_local: str
    type_local: str
    surface_reelle_bati: float
    nombre_pieces_principales: int
    code_nature_culture: str
    nature_culture: str
    code_nature_culture_speciale: str
    nature_culture_speciale: str
    surface_terrain: float
    longitude: float
    latitude: float

# Configuration de la base de données
DATABASE_URL = "sqlite:///./dvf.db"
database = Database(DATABASE_URL)

# Création de l'API FastAPI
app = FastAPI()

# Fonction pour se connecter à la base de données
@app.on_event("startup")
async def startup():
    await database.connect()
    await create_db_if_not_exists()

# Fonction pour se déconnecter de la base de données
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# Fonction pour créer la base de données et insérer les données du CSV si elle n'existe pas déjà
async def create_db_if_not_exists():
    db_path = Path("dvf.db")
    if not db_path.exists():
        async with database.transaction():
            await database.execute("CREATE TABLE IF NOT EXISTS dvf (id_mutation TEXT PRIMARY KEY, date_mutation TEXT, numero_disposition TEXT, nature_mutation TEXT, valeur_fonciere REAL, adresse_numero TEXT, adresse_suffixe TEXT, adresse_nom_voie TEXT, adresse_code_voie TEXT, code_postal TEXT, code_commune TEXT, nom_commune TEXT, code_departement TEXT, ancien_code_commune TEXT, ancien_nom_commune TEXT, id_parcelle TEXT, ancien_id_parcelle TEXT, numero_volume TEXT, lot1_numero TEXT, lot1_surface_carrez REAL, lot2_numero TEXT, lot2_surface_carrez REAL, lot3_numero TEXT, lot3_surface_carrez REAL, lot4_numero TEXT, lot4_surface_carrez REAL, lot5_numero TEXT, lot5_surface_carrez REAL, nombre_lots INTEGER, code_type_local TEXT, type_local TEXT, surface_reelle_bati REAL, nombre_pieces_principales INTEGER, code_nature_culture TEXT, nature_culture TEXT, code_nature_culture_speciale TEXT, nature_culture_speciale TEXT, surface_terrain REAL, longitude REAL, latitude REAL)")
            await insert_data_from_csv()

# Fonction pour insérer les données du CSV dans la base de données
# Fonction pour insérer les données du CSV dans la base de données
async def insert_data_from_csv():
    with open('donnees_dvf.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        async with database.transaction():
            for row in reader:
                values = {k: v for k, v in row.items() if k in Dvf.__annotations__}
                query = Dvf.__table__.insert().values(**values)
                await database.execute(query=query)


# Endpoint pour récupérer les prix moyens au mètre carré par ville
@app.get("/prix-moyen-m2/")
async def lire_prix_moyen_m2_par_ville():
    query = select(Dvf.nom_commune, (Dvf.valeur_fonciere / Dvf.surface_reelle_bati).label("prix_moyen_m2")).group_by(Dvf.nom_commune)
    result = await database.fetch_all(query)
    prix_moyen_m2_par_ville = {row["nom_commune"]: row["prix_moyen_m2"] for row in result}
    return prix_moyen_m2_par_ville

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
