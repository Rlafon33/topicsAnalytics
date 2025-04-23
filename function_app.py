import os
import json
import logging
from datetime import datetime
from io import BytesIO, StringIO

import azure.functions as func
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient


# --- Constants & Configuration ---
ENV_BLOB_CONN = "BLOB_CONNECTION_STRING"
ENV_BEARER     = "BEARER_TOKEN"
API_BASE       = "https://bpi.api.datagalaxy.com/v2"
VERSION_ID     = os.environ.get("VERSION_ID", "7333a87f-1f0f-4cc7-8d81-fcc2b283d433")

# --- Environment Helper ---
def get_env(key: str) -> str:
    """Retrieve an environment variable or throw if missing."""
    val = os.environ.get(key)
    if not val:
      logging.warning(f"Env var '{key}' is not set at import time.")
      return ""
    return val

# Load settings
BLOB_CONN_STR = get_env(ENV_BLOB_CONN)
BEARER_TOKEN  = get_env(ENV_BEARER)

# --- Blob Storage Utilities ---
def get_blob_client() -> BlobServiceClient:
    """Instantiate BlobServiceClient from connection string."""
    return BlobServiceClient.from_connection_string(BLOB_CONN_STR)


def read_csv_from_blob(container: str, blob_name: str,
                       sep: str = ';', encoding: str = 'cp1252') -> pd.DataFrame:
    """Download CSV from blob and return pandas DataFrame."""
    client = get_blob_client().get_container_client(container)
    data = client.get_blob_client(blob_name).download_blob().readall()
    return pd.read_csv(BytesIO(data), sep=sep, encoding=encoding)


def write_csv_to_blob(container: str, df: pd.DataFrame,
                      sep: str = ';', encoding: str = 'cp1252') -> str:
    """Upload DataFrame as CSV to blob; return blob name."""
    client = get_blob_client().get_container_client(container)
    filename = f"{datetime.utcnow():%Y%m%d}_TopicsEnrichis.csv"
    buf = StringIO()
    df.to_csv(buf, index=False, sep=sep, encoding=encoding)
    client.upload_blob(name=filename, data=buf.getvalue().encode(encoding), overwrite=True)
    return filename

# --- DataGalaxy API Helpers ---
def fetch_all_topics() -> pd.DataFrame:
    """Retrieve all topics, handling pagination."""
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
    all_results = []

    while url:
        logging.info(f"Fetching topics: {url}")
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        all_results.extend(data.get('results', []))
        url = data.get('next_page')
        params.clear()

    return pd.json_normalize(all_results)


def add_referential_topics(df_topics: pd.DataFrame, df_ref: pd.DataFrame) -> pd.DataFrame:
    """Merge topics with application reference data."""
    df_topics['Code application'] = (
        df_topics['technicalName'].str.split('_').str[1].str.upper()
    )
    df_ref_sel = df_ref[['trigramme', 'nom', 'train', 'agileTeam (valeur corrigée)']]
    df = df_topics.merge(
        df_ref_sel,
        left_on='Code application', right_on='trigramme', how='left'
    )
    df.rename(columns={'nom': 'Application', 'agileTeam (valeur corrigée)': 'équipe'}, inplace=True)
    return df.drop(columns=['trigramme'])


def count_data_fields(topic_id: str) -> int:
    """Count non-local 'data' fields for a topic, with pagination."""
    url = f"{API_BASE}/fields"
    params = {'parentId': topic_id, 'versionId': VERSION_ID,
              'type': 'Field', 'includeLinks': 'true'}
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}
    total = 0

    while url:
        logging.info(f"Counting data fields: {url}")
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        for f in data.get('results', []):
            attrs = f.get('attributes', {})
            if attrs.get('Donnee Locale'): continue
            if 'data' in f.get('path', '').lower().split('\\'):
                total += 1
        url = data.get('next_page')
        params.clear()

    return total


def count_glossary_alignments(topic_id: str) -> int:
    """Count direct glossary alignments in topic fields."""
    url = f"{API_BASE}/fields"
    params = {'parentId': topic_id, 'versionId': VERSION_ID,
              'type': 'Field', 'includeLinks': 'true'}
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}
    count = 0
    key = 'BusinessTerm'

    while url:
        logging.info(f"Counting glossary alignments: {url}")
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        for f in data.get('results', []):
            if f.get('attributes', {}).get('Donnee Locale'): continue
            for links in f.get('links', {}).values():
                for link in links:
                    if key in link.get('typePath', ''):
                        count += 1
        url = data.get('next_page')
        params.clear()

    return count


def extract_usage_parent_id_from_topic(row: pd.Series) -> str:
    """Get first usage ID from topic's IsUsedBy links."""
    raw = row.get('links.IsUsedBy')
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if isinstance(raw, list) and raw:
        return raw[0].get('id')
    return None


def count_usage_glossary_alignments(usage_id: str) -> int:
    """Count glossary alignments in usage elements."""
    if not usage_id:
        return 0
    url = f"{API_BASE}/usages"
    params = {'parentId': usage_id, 'versionId': VERSION_ID,
              'includeLinks': 'true', 'includeAttributes': 'true'}
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}
    count = 0
    key = 'BusinessTerm'

    while url:
        logging.info(f"Counting usage alignments: {url}")
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        for u in data.get('results', []):
            if u.get('attributes', {}).get('Donnee Locale'): continue
            for links in u.get('links', {}).values():
                for link in links:
                    if key in link.get('typePath', ''):
                        count += 1
        url = data.get('next_page')
        params.clear()

    return count


def compute_alignment_counts(row: pd.Series) -> pd.Series:
    """Return Series(count_fields, direct_align, usage_align)."""
    topic_id = row['id']
    total = count_data_fields(topic_id)
    direct = count_glossary_alignments(topic_id)
    usage_parent = extract_usage_parent_id_from_topic(row)
    usage = count_usage_glossary_alignments(usage_parent)
    return pd.Series({
        'NombreChampsAAligner': total,
        'NombreChampsAlignesDirectement': direct,
        'NombreChampsAlignesUsage': usage
    })


def has_entity_field(topic_id: str) -> bool:
    """Check if any field path contains '\\payload\\entity'."""
    url = f"{API_BASE}/fields"
    params = {'parentId': topic_id, 'versionId': VERSION_ID,
              'includeLinks': 'true'}
    headers = {'Authorization': f"Bearer {BEARER_TOKEN}"}

    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        for f in resp.json().get('results', []):
            attrs = f.get('attributes', {})
            if attrs.get('Donnee Locale') or attrs.get('status') == 'Obsolète': continue
            if 'payload\\entity' in f.get('path', '').lower():
                return True
        url = resp.json().get('next_page')
        params.clear()

    return False


def get_portee(path: str) -> str:
    """Extract third token after '_' in last segment of path."""
    parts = path.split('\\')
    if parts:
        segs = parts[-1].split('_')
        if len(segs) > 2:
            return segs[2]
    return ''


def generate_final_output_df(df: pd.DataFrame) -> pd.DataFrame:
    """Build final DataFrame with enrichments and metrics."""
    # Apply alignment counts and merge back
    df_counts = df.apply(compute_alignment_counts, axis=1)
    df = pd.concat([df, df_counts], axis=1)

    # Flag topics
    df['Flag topic entité ?'] = df['id'].apply(
        lambda tid: 'Topic Entité' if has_entity_field(tid) else 'Topic Fonctionnel'
    )

    # Construct output
    df_out = pd.DataFrame({
        'Train': df['train'],
        'Application': df['Application'],
        'Code application': df['Code application'],
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
        'Nombre de données alignés au glossaire': df[['NombreChampsAlignesDirectement',
                                                     'NombreChampsAlignesUsage']].max(axis=1)
    })

    # Percent lineage glossaire
    df_out['% lineage glossaire'] = (
        df_out['Nombre de données alignés au glossaire']
        / df_out['Nombre de données de la payload à aligner']
        * 100
    ).fillna(0)

    # Class labels 
    bins = [0, 0.000001, 80, 100, float('inf')]
    labels = [
        '0-Lineage fonctionnel & aligné au MOM inexistant',
        '1-Lineage fonctionnel & aligné au MOM partiel :<80%',
        '2-Lineage fonctionnel & aligné au MOM à compléter :<100%',
        '3-Lineage fonctionnel & aligné au MOM complet'
    ]
    df_out['Classe de pourcentage'] = pd.cut(
        df_out['% lineage glossaire'], bins=bins, labels=labels, right=False
    )

    return df_out

def topicsAnalytics(req: func.HttpRequest) -> func.HttpResponse:
    """Trigger HTTP to build and upload enriched topics CSV."""
    logging.info("Starting topic analytics.")

    source    = "sources"
    ref_blob  = "ref/ref_application.csv"
    target    = "analytics"

    # Read reference
    try:
        df_ref = read_csv_from_blob(source, ref_blob)
        logging.info("Reference data loaded.")
    except Exception as err:
        logging.error(f"Reference load error: {err}")
        return func.HttpResponse("Reference load failed", status_code=500)

    # Fetch & process
    try:
        df_topics   = fetch_all_topics()
        df_enriched = add_referential_topics(df_topics, df_ref)
        df_filtered = df_enriched[df_enriched['technicalName'].str.endswith('ini')].head(30)
        df_final    = generate_final_output_df(df_filtered)
    except Exception as err:
        logging.error(f"Processing error: {err}")
        return func.HttpResponse("Processing failed", status_code=500)

    # Upload result
    try:
        name = write_csv_to_blob(target, df_final)
        msg  = f"Saved CSV to '{target}' as {name}"
        logging.info(msg)
        return func.HttpResponse(msg, status_code=200)
    except Exception as err:
        logging.error(f"Upload error: {err}")
        return func.HttpResponse("Upload failed", status_code=500)

