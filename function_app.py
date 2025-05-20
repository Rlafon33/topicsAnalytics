import os
import json
import logging
import time
from datetime import datetime
from io import BytesIO, StringIO

import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from datetime import date


# Chargement des variables d'environnement
load_dotenv()

# --- Constants & Configuration ---
ENV_BLOB_CONN = "BLOB_CONNECTION_STRING"
ENV_BEARER = "BEARER_TOKEN"
API_BASE = "https://bpi.api.datagalaxy.com/v2"
VERSION_ID = os.getenv("VERSION_ID", "7333a87f-1f0f-4cc7-8d81-fcc2b283d433")

# --- Environment Helper ---

def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        logging.warning(f"Env var '{key}' is not set.")
        return ""
    return val

BLOB_CONN_STR = get_env(ENV_BLOB_CONN)
BEARER_TOKEN = get_env(ENV_BEARER)

# --- Blob Storage Utilities ---

def get_blob_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(BLOB_CONN_STR)


def read_csv_from_blob(container: str, blob_name: str,
                       sep: str = ';', encoding: str = 'cp1252') -> pd.DataFrame:
    client = get_blob_client().get_container_client(container)
    data = client.get_blob_client(blob_name).download_blob().readall()
    logging.info(f"Loaded blob {blob_name} from container {container}.")
    return pd.read_csv(BytesIO(data), sep=sep, encoding=encoding)


def write_csv_to_blob(
    container: str,
    df: pd.DataFrame,
    sep: str = ';',
    encoding: str = 'cp1252',
    filename: str = None
) -> str:
    """
    Enregistre le DataFrame `df` au format CSV dans le container Blob donné.
    Si `filename` est fourni, l’utilise comme nom de fichier, sinon génère
    un nom par défaut basé sur la date UTC du jour (YYYYMMDD_TopicsEnrichis.csv).
    """
    client = get_blob_client().get_container_client(container)

    # Détermination du nom de fichier
    if filename is None:
        filename = f"{datetime.utcnow():%Y%m%d}_TopicsEnrichis.csv"

    # Sérialisation du DataFrame en CSV dans un buffer mémoire
    buf = StringIO()
    df.to_csv(buf, index=False, sep=sep, encoding=encoding)

    # Upload du buffer vers Azure Blob
    client.upload_blob(
        name=filename,
        data=buf.getvalue().encode(encoding),
        overwrite=True
    )
    logging.info(f"Uploaded blob {filename} to container {container}.")
    return filename

# --- API Helper with Retry ---

def api_get_with_retry(url: str, params: dict, headers: dict,
                       retries: int = 3, wait: float = 1.0) -> dict:
    """
    Effectue un GET avec retry si échec.
    """
    for attempt in range(1, retries + 1):
        logging.info(f"API GET call: URL={url}, params={params}, attempt={attempt}")
        try:
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logging.warning(f"API call failed (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(wait)
            else:
                logging.error(f"API call to {url} failed after {retries} attempts.")
                raise

# --- Topic Fetching ---

def fetch_all_topics() -> pd.DataFrame:
    
    url = f"{API_BASE}/structures"
    params = {
        'versionId': VERSION_ID,
        'parentId': '59f37279-3a9e-4025-956c-088a0c8f217d:d4e9eb56-184d-4dae-9a6c-c4be5644c398',
        'Limit': 2500,
        'maxDepth': 0,
        'includeAttributes': 'true',
        'includeLinks': 'true'
    }
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}
    results = []

    while url:
        logging.info(f"Fetching topics from {url}")
        data = api_get_with_retry(url, params, headers)
        results.extend(data.get('results', []))
        url = data.get('next_page')
        params.clear()

    logging.info(f"Fetched {len(results)} topics in total.")
    return pd.json_normalize(results)

# --- Counting & Extraction Functions ---

def count_data_fields(topic_id: str) -> int:
    url = f"{API_BASE}/fields"
    params = {'parentId': topic_id, 'versionId': VERSION_ID, 'type': 'Field', 'includeLinks': 'true'}
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}
    total = 0

    while url:
        logging.debug(f"Counting data fields for topic {topic_id} at {url}")
        data = api_get_with_retry(url, params, headers)
        for field in data.get('results', []):
            attrs = field.get('attributes', {})
            if attrs.get('Donnee Locale'): continue
            if 'data' in field.get('path', '').lower().split('\\'):
                total += 1
        url = data.get('next_page')
        params.clear()

    return total


def count_glossary_alignments(topic_id: str) -> int:
    url = f"{API_BASE}/fields"
    params = {'parentId': topic_id, 'versionId': VERSION_ID, 'type': 'Field', 'includeLinks': 'true'}
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}
    count = 0

    while url:
        logging.debug(f"Counting glossary alignments for topic {topic_id}")
        data = api_get_with_retry(url, params, headers)
        for field in data.get('results', []):
            if field.get('attributes', {}).get('Donnee Locale'): continue
            for links in field.get('links', {}).values():
                for link in links:
                    if 'BusinessTerm' in link.get('typePath', ''):
                        count += 1
        url = data.get('next_page')
        params.clear()

    return count


def extract_usage_parent_id_from_topic(row: pd.Series) -> str:
    raw = row.get('links.IsUsedBy')
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if isinstance(raw, list) and raw:
        return raw[0].get('id')
    return None


def count_usage_glossary_alignments(usage_id: str) -> int:
    if not usage_id:
        return 0
    url = f"{API_BASE}/usages"
    params = {'parentId': usage_id, 'versionId': VERSION_ID,
              'includeLinks': 'true', 'includeAttributes': 'true'}
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}
    count = 0

    while url:
        logging.debug(f"Counting usage alignments for usage {usage_id}")
        data = api_get_with_retry(url, params, headers)
        for usage in data.get('results', []):
            attrs = usage.get('attributes', {})
            if attrs.get('Donnee Locale'): continue
            for links in usage.get('links', {}).values():
                for link in links:
                    if 'BusinessTerm' in link.get('typePath', ''):
                        count += 1
        url = data.get('next_page')
        params.clear()

    return count


def has_entity_field(topic_id: str) -> bool:
    url = f"{API_BASE}/fields"
    params = {'parentId': topic_id, 'versionId': VERSION_ID, 'includeLinks': 'true'}
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}

    while url:
        logging.debug(f"Checking entity field for topic {topic_id}")
        data = api_get_with_retry(url, params, headers)
        for field in data.get('results', []):
            attrs = field.get('attributes', {})
            if attrs.get('Donnee Locale') or attrs.get('status') == 'Obsolete':
                continue
            if 'payload\\entity' in field.get('path', '').lower():
                return True
        url = data.get('next_page')
        params.clear()

    return False


def get_portee(path: str) -> str:
    parts = path.split('\\')
    if parts:
        segs = parts[-1].split('_')
        if len(segs) > 2:
            return segs[2]
    return ''

# --- Data Enrichment ---

def add_referential_topics(df_topics: pd.DataFrame, df_ref: pd.DataFrame) -> pd.DataFrame:
    # Calcul du code application
    def extract_code_application(tn: str) -> str:
        if tn.startswith('prd_kif_'):
            parts = tn.split('_')
            if len(parts) > 3 and parts[3].isalpha() and len(parts[3]) == 3:
                return parts[3].upper()
            else:
                return "KIF"
        else:
            return tn.split('_')[1].upper()
    
    df_topics['Code application'] = df_topics['technicalName'].apply(extract_code_application)
    df_topics['Code producteur'] = df_topics['technicalName'].apply(lambda tn: tn.split('_')[1].upper())
    logging.info("Applied custom logic for Code application and Code producteur.")

    # Attribution du type de topic
    df_topics['attributes.Type Topic'] = df_topics['technicalName'].apply(
        lambda tn: 'Reprise' if '_reprise_' in tn else ('Technique' if '_technical_' in tn else 'Public')
    )
    logging.info("Set Type Topic based on naming rules.")

    # Jointure référentiel
    df_ref_sel = df_ref[['trigramme', 'nom', 'train', 'agileTeam (valeur corrigée)']]
    df = df_topics.merge(
        df_ref_sel,
        left_on='Code application', right_on='trigramme', how='left'
    )
    df.rename(columns={'nom': 'Application', 'agileTeam (valeur corrigée)': 'équipe'}, inplace=True)
    logging.info("Merged with reference dataframe.")
    return df.drop(columns=['trigramme'])

# --- Compute and Finalize ---

def compute_alignment_counts(row: pd.Series) -> pd.Series:
    try:
        total = count_data_fields(row['id'])
        direct = count_glossary_alignments(row['id'])
        usage = count_usage_glossary_alignments(
            extract_usage_parent_id_from_topic(row)
        )
        return pd.Series({
            'NombreChampsAAligner': total,
            'NombreChampsAlignesDirectement': direct,
            'NombreChampsAlignesUsage': usage
        })
    except Exception as e:
        logging.warning(f"Alignment count failed for {row['id']}: {e}")
        return pd.Series({
            'NombreChampsAAligner': 0,
            'NombreChampsAlignesDirectement': 0,
            'NombreChampsAlignesUsage': 0
        })

def load_latest_histo_paths(container_name: str, prefix: str = "histo/") -> set:
    """
    Récupère le set des Path du dernier fichier historique dans le container donné,
    en ne listant que les blobs sous le préfixe `prefix`.
    """
    container = get_blob_client().get_container_client(container_name)
    # ne lister que les blobs commençant par "histo/"
    blobs = sorted(
        container.list_blobs(name_starts_with=prefix),
        key=lambda b: b.name
    )
    if not blobs:
        return set()

    latest_blob_name = blobs[-1].name  # ex : "histo/202505_histoTopics.csv"
    data = container.get_blob_client(latest_blob_name).download_blob().readall()
    df_old = pd.read_csv(BytesIO(data), sep=';', encoding='cp1252')
    return set(df_old['Path'])

def generate_final_output_df(df: pd.DataFrame, old_paths: set = None) -> pd.DataFrame:
    df_counts = df.apply(compute_alignment_counts, axis=1)
    df = pd.concat([df, df_counts], axis=1)

    # Flag entité vs fonctionnel
    df['Flag topic entité ?'] = df['id'].apply(
        lambda tid: 'Topic Entité' if has_entity_field(tid) else 'Topic Fonctionnel'
    )

    # Assemblage du dataframe de sortie
    df_out = pd.DataFrame({
        'Train': df['train'],
        'Application': df['Application'],
        'Code application': df['Code application'],
        'Code producteur': df['Code producteur'],
        'Equipe': df.get('équipe', 'Nextgen'),
        'Path': df['path'],
        'Nom du topic': df['name'],
        'Type': df['attributes.Type Topic'],
        'Flag topic entité ?': df['Flag topic entité ?'],
        'Portée': df['path'].apply(get_portee),
        'Status du topic': df['attributes.status'],
        'Date de création du topic': df['attributes.creationTime'],
        'Date de dernière modification du topic': df['attributes.lastModificationTime'],
        'Description': df['attributes.description'],
        '% de documentation': df['attributes.% de documentation'],
        'Nombre de données de la payload à aligner': df['NombreChampsAAligner'],
        'Nombre de données alignés au glossaire': df[['NombreChampsAlignesDirectement', 'NombreChampsAlignesUsage']].max(axis=1)
    })

    df_out['% lineage glossaire'] = (
        df_out['Nombre de données alignés au glossaire']
        / df_out['Nombre de données de la payload à aligner'] * 100
    ).fillna(0)

    bins = [0, 0.000001, 80, 100, float('inf')]
    labels = [
        '0-Lineage fonctionnel & aligné au glossaire inexistant',
        '1-Lineage fonctionnel & aligné au glossaire partiel :<80%',
        '2-Lineage fonctionnel & aligné au glossaire à compléter :<100%',
        '3-Lineage fonctionnel & aligné au glossaire complet'
    ]
    df_out['Classe de pourcentage'] = pd.cut(
        df_out['% lineage glossaire'], bins=bins, labels=labels, right=False
    )
    if old_paths is not None:
        df_out['Nouveau'] = df_out['Path'].isin(old_paths).map({True: 'Non', False: 'Oui'})
        logging.info(f"Flag 'Nouveau' ajouté : {df_out['Nouveau'].value_counts().to_dict()}")
    else:
        df_out['Nouveau'] = 'Oui'  # par défaut si pas d'historique

    logging.info("Generated final output dataframe.")
    return df_out

# --- Main Analytics Function ---

def topicsAnalytics():
    logging.info("Starting topics analytics process.")
    source, ref_blob, target = "sources", "ref/ref_application.csv", "analytics"
    # Chargement du référentiel
    try:
        df_ref = read_csv_from_blob(source, ref_blob)
    except Exception as e:
        logging.error(f"Reference load error: {e}")
        return "Reference load failed"
    try:
        logging.info("Starting load params.")
        df_obeya = read_csv_from_blob("sources", "params/Obeya.csv", sep=';', encoding='utf-8')
        obeya_dates = df_obeya.iloc[:, 0].astype(str).tolist()
        today_str = datetime.today().strftime('%Y%m%d')
        logging.info(f"Date du jour : {today_str}, Dates Obeya : {obeya_dates}")
    except Exception as e:
        logging.warning(f"Erreur lecture Obeya.csv : {str(e)}")
        obeya_dates = []
    
    # Fetch topics
    try:
        df_topics = fetch_all_topics()
     #   df_topics = df_topics.head(30)
    except Exception as e:
        logging.error(f"Fetching topics failed: {e}")
        return "Fetching topics failed"

    # Enrichissement et filtrage
    df_enriched = add_referential_topics(df_topics, df_ref)
    df_filtered = df_enriched[df_enriched['technicalName'].str.endswith('ini')]
    logging.info(f"Filtered to {len(df_filtered)} topics ending with 'ini'.")

    # Génération de la sortie
    try:
        old_paths = load_latest_histo_paths("sources", prefix="histo/")
        df_output = generate_final_output_df(df_filtered, old_paths=old_paths)
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        return "Processing failed"

    # Écriture dans Blob
    try:
        filename = write_csv_to_blob(target, df_output)
        msg = f"Saved CSV to '{target}' as {filename}"
        logging.info(msg)
    except Exception as e:
        logging.error(f"Upload error: {e}")
        return "Upload failed"
    
    if today_str in obeya_dates:
        try:
            df_histo = pd.DataFrame(df_output["Path"])
            histo_filename = datetime.today().strftime('%Y%m') + "_histoTopics.csv"
            write_csv_to_blob("sources/histo", df_histo, sep=';', encoding='cp1252', filename=histo_filename)
            logging.info(f"Fichier historique généré : sources/histo/{histo_filename}")
        except Exception as e:
            logging.warning(f"Erreur lors de la génération du fichier histo : {str(e)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    result = topicsAnalytics()
    print(result)
