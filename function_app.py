import logging
import azure.functions as func
import pandas as pd
import requests
from io import StringIO, BytesIO
from datetime import datetime
import os
from azure.storage.blob import BlobServiceClient
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Token bearer pour les appels à l'API DataGalaxy
BEARER_TOKEN = (
    "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJxTlc3X1VuZEQzOTJpamIyRUZodGYtbHVNbUg5bU9PajJpamFfYzk3dXRFIn0.eyJleHAiOjE3NzQ2OTA3MzYsImlhdCI6MTc0MzE1NDczNiwianRpIjoiZjFkYzIwNmYtNjYzOS00YzM1LWFjMjYtYWJkYjMzYzE2ZDc3IiwiaXNzIjoiaHR0cHM6Ly9icGkuZGF0YWdhbGF4eS5jb20vYXV0aC9yZWFsbXMvNGEwY2YyYWYtZjk5ZS00ZWI0LTk4NGUtOWQ0ZDI4NTI1Y2QxIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6IjM3OTI1NzQ1LWJhOGYtNGFiNC04NGQ1LWE5YjQ3MmVjMTIyYiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImRnLXRva2VucyIsInNpZCI6ImM0ZmQzYzMwLTBlMWItNDNmMC04NDhmLWNkZWM0ODU5NzQ4NCIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cHM6Ly9lbGRpY29qZmNoYmVjYWFtYWhlYmJkaWFha2ZsY2NvZy5jaHJvbWl1bWFwcC5vcmciLCJodHRwczovL2JwcHBsZG5pcG5wcGttb25nbmtpaGxpb2FvanBtaW5lLmNocm9taXVtYXBwLm9yZyIsImh0dHBzOi8vYWVhYWVnaWxnZ2dmZ2FpbWpuYWVsam1mZ2FlZmNuZmUuY2hyb21pdW1hcHAub3JnIiwiaHR0cHM6Ly9icGkuZGF0YWdhbGF4eS5jb20iXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRlZmF1bHQtcm9sZXMtNGEwY2YyYWYtZjk5ZS00ZWI0LTk4NGUtOWQ0ZDI4NTI1Y2QxIiwib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib2ZmbGluZV9hY2Nlc3Mgb3BlbmlkIGVtYWlsIHByb2ZpbGUgZGdfcGxhdGZvcm0iLCJkZ191c2VyX2lkIjoiNWQwYzQ0NDktMDUxMy00MDRjLWIzNTEtM2RkMDVlYWNhY2UyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsIm5hbWUiOiJyZW1pX2xhZm9uX3JlYWRfcHJkIFVzZXIiLCJkZ19jbGllbnRfaWQiOiI0YTBjZjJhZi1mOTllLTRlYjQtOTg0ZS05ZDRkMjg1MjVjZDEiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiI1ZDBjNDQ0OS0wNTEzLTQwNGMtYjM1MS0zZGQwNWVhY2FjZTJAZGF0YWdhbGF4eS5kYXQiLCJnaXZlbl9uYW1lIjoicmVtaV9sYWZvbl9yZWFkX3ByZCIsImZhbWlseV9uYW1lIjoiVXNlciIsImVtYWlsIjoiNWQwYzQ0NDktMDUxMy00MDRjLWIzNTEtM2RkMDVlYWNhY2UyQGRhdGFnYWxheHkuZGF0In0.cWY99O4bm2Qmx7SkfzF2OXEXew3dVA1oSwLB4ziJi22bah5QTsVY0aeDUSu_RwTJ4-wIxCbqGdiNcGyKQ14aMH8gfEZcKpcFC2kza51EYen2HR5sbbpljRGT6zHwlZQg2cITaUYdfVduNePagrX5B3EP5J4Rmkb9pHO5m8fOpybNn3-KUrusivRtzVNEYXEEYDUBAvtNzd1AvRLQWQtucNWMArdhPS40E7nxp-HceGtnWbzbmeZiG9kHY2B9nspvcYlWYWGK4l4nbcZPN4fD9zAJFdGLW27dzgoC64baA2-7T83GjRYWW7cicinn8aQq90Agkc9KV2AU903B54Nzeg")

# --- Fonctions Blob --- #
def get_blob_service_client() -> BlobServiceClient:
    connection_string = os.environ.get("BLOB_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("La variable d'environnement 'BLOB_CONNECTION_STRING' n'est pas configurée.")
    return BlobServiceClient.from_connection_string(connection_string)

def read_csv_from_blob(container_name: str, blob_name: str, sep=';', encoding='cp1252') -> pd.DataFrame:
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    blob_bytes = blob_client.download_blob().readall()
    return pd.read_csv(BytesIO(blob_bytes), sep=sep, encoding=encoding)

def write_csv_to_blob(container_name: str, df: pd.DataFrame, sep=';', encoding='cp1252') -> str:
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)
    filename = datetime.now().strftime("%Y%m%d") + "_TopicsEnrichis.csv"
    blob_name = filename
    output = StringIO()
    df.to_csv(output, index=False, sep=sep, encoding=encoding)
    csv_bytes = output.getvalue().encode(encoding)
    container_client.upload_blob(name=blob_name, data=csv_bytes, overwrite=True)
    return blob_name

# --- Fonctions API Kafka et calculs divers (inchangées) --- #
def fetch_all_topics_from_api() -> pd.DataFrame:
    api_url = (
        "https://bpi.api.datagalaxy.com/v2/structures?"
        "versionId=7333a87f-1f0f-4cc7-8d81-fcc2b283d433&"
        "parentId=59f37279-3a9e-4025-956c-088a0c8f217d:d4e9eb56-184d-4dae-9a6c-c4be5644c398&"
        "Limit=2500&maxDepth=0&includeAttributes=true&includeLinks=true"
    )
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    topics = []
    while api_url:
        logging.info(f"Fetching topics from API: {api_url}")
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        topics.extend(data.get("results", []))
        api_url = data.get("next_page")
    return pd.json_normalize(topics)

def add_referential_topics(df_topics: pd.DataFrame, df_ref: pd.DataFrame) -> pd.DataFrame:
    df_topics["Code application"] = df_topics["technicalName"].str.split('_').str[1].str.upper()
    df_ref_filtered = df_ref[['trigramme', 'nom', 'train', 'agileTeam (valeur corrigée)']].copy()
    df_merged = df_topics.merge(
        df_ref_filtered,
        how='left',
        left_on='Code application',
        right_on='trigramme'
    )
    df_merged.rename(
        columns={
            'nom': 'Application',
            'agileTeam (valeur corrigée)': 'équipe'
        },
        inplace=True
    )
    df_merged.drop(columns=['trigramme'], inplace=True)
    return df_merged

def count_data_fields(topic_id: str) -> int:
    base_url = "https://bpi.api.datagalaxy.com/v2/fields"
    params = {
        "parentId": topic_id,
        "versionId": "7333a87f-1f0f-4cc7-8d81-fcc2b283d433",
        "type": "Field",
        "includeLinks": "true"
    }
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    count = 0
    logging.info(f"Calling Field API: {base_url} with params: {params}")
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    for field in data.get("results", []):
        if field.get("attributes", {}).get("Donnee Locale") == True:
            continue
        components = [comp.lower() for comp in field.get("path", "").split("\\") if comp]
        if "data" in components:
            count += 1
    next_page = data.get("next_page")
    while next_page:
        logging.info(f"Calling Field API (next page): {next_page}")
        response = requests.get(next_page, headers=headers)
        response.raise_for_status()
        data = response.json()
        for field in data.get("results", []):
            if field.get("attributes", {}).get("Donnee Locale") == True:
                continue
            components = [comp.lower() for comp in field.get("path", "").split("\\") if comp]
            if "data" in components:
                count += 1
        next_page = data.get("next_page")
    return count

def count_glossary_alignments(topic_id: str) -> int:
    base_url = "https://bpi.api.datagalaxy.com/v2/fields"
    params = {
        "parentId": topic_id,
        "versionId": "7333a87f-1f0f-4cc7-8d81-fcc2b283d433",
        "type": "Field",
        "includeLinks": "true"
    }
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    count = 0
    expected_fragment = "BusinessTerm"  # Alignement glossaire contient ce fragment dans le typePath
    logging.info(f"Calling Field API for Glossary Alignments: {base_url} with params: {params}")
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    for field in data.get("results", []):
        if field.get("attributes", {}).get("Donnee Locale") == True:
            continue
        for link_list in field.get("links", {}).values():
            for link in link_list:
                if expected_fragment in link.get("typePath", ""):
                    count += 1
    next_page = data.get("next_page")
    while next_page:
        logging.info(f"Calling Field API for Glossary Alignments (next page): {next_page}")
        response = requests.get(next_page, headers=headers)
        response.raise_for_status()
        data = response.json()
        for field in data.get("results", []):
            if field.get("attributes", {}).get("Donnee Locale") == True:
                continue
            for link_list in field.get("links", {}).values():
                for link in link_list:
                    if expected_fragment in link.get("typePath", ""):
                        count += 1
        next_page = data.get("next_page")
    return count

def get_direct_glossary_field_ids(topic_id: str) -> set:
    base_url = "https://bpi.api.datagalaxy.com/v2/fields"
    params = {
        "parentId": topic_id,
        "versionId": "7333a87f-1f0f-4cc7-8d81-fcc2b283d433",
        "type": "Field",
        "includeLinks": "true"
    }
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    ids = set()
    logging.info(f"Calling Field API for Direct Glossary IDs: {base_url} with params: {params}")
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    glossary_typePath = "\\Universe\\Universe\\Universe\\Concept\\BusinessTerm"
    for field in data.get("results", []):
        if field.get("attributes", {}).get("Donnee Locale") == True:
            continue
        for link_list in field.get("links", {}).values():
            for link in link_list:
                if link.get("typePath") == glossary_typePath:
                    ids.add(field.get("id"))
                    break
    next_page = data.get("next_page")
    while next_page:
        logging.info(f"Calling Field API (next page) for Direct Glossary IDs: {next_page}")
        response = requests.get(next_page, headers=headers)
        response.raise_for_status()
        data = response.json()
        for field in data.get("results", []):
            if field.get("attributes", {}).get("Donnee Locale") == True:
                continue
            for link_list in field.get("links", {}).values():
                for link in link_list:
                    if link.get("typePath") == glossary_typePath:
                        ids.add(field.get("id"))
                        break
        next_page = data.get("next_page")
    return ids

def extract_usage_parent_id_from_topic(topic_row: pd.Series) -> str:
    links = topic_row.get("links.IsUsedBy")
    if links:
        if isinstance(links, str):
            try:
                links = json.loads(links)
            except Exception as e:
                logging.error(f"Erreur de parsing JSON dans links.IsUsedBy: {e}")
                return None
        if isinstance(links, list) and len(links) > 0:
            return links[0].get("id")
    return None

def count_usage_glossary_alignments(usage_parent_id: str) -> int:
    base_url = "https://bpi.api.datagalaxy.com/v2/usages"
    params = {
        "parentId": usage_parent_id,
        "versionId": "7333a87f-1f0f-4cc7-8d81-fcc2b283d433",
        "includeAttributes": "true",
        "includeLinks": "true"
    }
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    count = 0
    expected_fragment = "BusinessTerm"  # On recherche ce fragment dans le typePath
    logging.info(f"Calling Usage API for Glossary Alignments: {base_url} with params: {params}")
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    for usage in data.get("results", []):
        if usage.get("attributes", {}).get("Donnee Locale") == True:
            continue
        for link_list in usage.get("links", {}).values():
            for link in link_list:
                if expected_fragment in link.get("typePath", ""):
                    count += 1
    next_page = data.get("next_page")
    while next_page:
        logging.info(f"Calling Usage API for Glossary Alignments (next page): {next_page}")
        response = requests.get(next_page, headers=headers)
        response.raise_for_status()
        data = response.json()
        for usage in data.get("results", []):
            if usage.get("attributes", {}).get("Donnee Locale") == True:
                continue
            for link_list in usage.get("links", {}).values():
                for link in link_list:
                    if expected_fragment in link.get("typePath", ""):
                        count += 1
        next_page = data.get("next_page")
    return count

def get_usage_glossary_field_ids(usage_parent_id: str) -> set:
    base_url = "https://bpi.api.datagalaxy.com/v2/usages"
    params = {
        "parentId": usage_parent_id,
        "versionId": "7333a87f-1f0f-4cc7-8d81-fcc2b283d433",
        "includeAttributes": "true",
        "includeLinks": "true"
    }
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    ids = set()
    logging.info(f"Calling Usage API for Glossary IDs: {base_url} with params: {params}")
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    glossary_typePath = "\\Universe\\Universe\\Universe\\Concept\\BusinessTerm"
    for usage in data.get("results", []):
        if usage.get("attributes", {}).get("Donnee Locale") == True:
            continue
        for link_list in usage.get("links", {}).values():
            for link in link_list:
                if link.get("typePath") == glossary_typePath:
                    ids.add(usage.get("id"))
                    break
    next_page = data.get("next_page")
    while next_page:
        logging.info(f"Calling Usage API for Glossary IDs (next page): {next_page}")
        response = requests.get(next_page, headers=headers)
        response.raise_for_status()
        data = response.json()
        for usage in data.get("results", []):
            if usage.get("attributes", {}).get("Donnee Locale") == True:
                continue
            for link_list in usage.get("links", {}).values():
                for link in link_list:
                    if link.get("typePath") == glossary_typePath:
                        ids.add(usage.get("id"))
                        break
        next_page = data.get("next_page")
    return ids

def compute_alignment_counts(row: pd.Series) -> pd.Series:
    topic_id = row.get("id")
    total_fields = count_data_fields(topic_id)  # NombreChampsAAligner
    direct_count = count_glossary_alignments(topic_id)
    direct_ids = get_direct_glossary_field_ids(topic_id)
    usage_parent_id = extract_usage_parent_id_from_topic(row)
    if usage_parent_id is None:
        logging.warning(f"Aucun usage parent trouvé pour le topic {topic_id}.")
        usage_count = 0
        usage_ids = set()
    else:
        usage_count = count_usage_glossary_alignments(usage_parent_id)
        usage_ids = get_usage_glossary_field_ids(usage_parent_id)
    common_count = len(direct_ids.intersection(usage_ids))
    aligned_fields = direct_count + usage_count - common_count  # NombreChampsAlignes
    return pd.Series([total_fields, aligned_fields])

def generate_final_output_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construit le DataFrame final selon le contrat d'interface.
    Les colonnes non renseignées sont laissées vides.
    """
    df_output = pd.DataFrame()
    df_output["Train"] = df["train"]
    df_output["Application"] = df["Application"]
    df_output["Code applicatio"] = df["Code application"]
    df_output["Equipe"] = df.get("équipe", "Nextgen")
    df_output["Nom du topic"] = df["name"]
    df_output["Type"] = df["attributes.Type Topic"]
    df_output["Flag topic entité ?"] = df["NombreChampsAAligner"].apply(
        lambda x: "Topics Entités" if x > 0 else "Topics Fonctionnels"
    )
    df_output["Date de création du topic"] = df["attributes.creationTime"]
    df_output["Date de dernière modification du topic"] = df["attributes.lastModificationTime"]
    df_output["Path"] = df["path"]
    df_output["Description"] = df["attributes.description"]
    df_output["Nombre de données de la payload"] = df["NombreChampsAAligner"]
    df_output["Nombre de données technique de la payload"] = df["NombreChampsAAligner"]
    df_output["Nombre de données à usage local de la payload"] = ""  # Non renseigné dans le nouveau contrat
    df_output["Nombre de données à aligner fonctionnellement"] = ""  # Non renseigné
    df_output["Status du topic"] = df["attributes.status"]
    df_output["Nombre de données avec lineage technique"] = ""  # Non renseigné
    df_output["Nombre de données avec lineage fonctionnel"] = ""  # Non renseigné
    df_output["Nombre de données alignés au glossaire"] = df["NombreChampsAlignes"]
    df_output["% lineage technique"] = ""  # Non renseigné
    df_output["% lineage fonctionnel"] = ""  # Non renseigné
    df_output["% lineage glossaire"] = df.apply(
        lambda row: (row["NombreChampsAlignes"] / row["NombreChampsAAligner"] * 100) if row["NombreChampsAAligner"] != 0 else 0,
        axis=1
    )
    df_output["Classe de pourcentage"] = ""  # Non renseigné
    return df_output

@app.route(route="topicsAnalytics", methods=["GET"])
def topicsAnalytics(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Déclenchement de la fonction pour générer le CSV des topics enrichis.")
    
    source_container = "sources"
    target_container = "analytics"
    ref_blob_name = "ref/ref_application.csv"
    
    try:
        df_ref = read_csv_from_blob(source_container, ref_blob_name, sep=';', encoding='cp1252')
        logging.info("Lecture du CSV de référence réussie.")
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du CSV de référence : {str(e)}")
        return func.HttpResponse("Erreur lors de la lecture du CSV de référence", status_code=500)
    
    try:
        df_topics = fetch_all_topics_from_api()
        logging.info("Récupération des topics via l'API DataGalaxy réussie.")
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des topics : {str(e)}")
        return func.HttpResponse("Erreur lors de la récupération des topics", status_code=500)
    
    try:
        df_enriched = add_referential_topics(df_topics, df_ref)
        logging.info("Enrichissement des topics avec le référentiel réussi.")
    except Exception as e:
        logging.error(f"Erreur lors de l'enrichissement des topics : {str(e)}")
        return func.HttpResponse("Erreur lors de l'enrichissement des topics", status_code=500)
    
    # Filtrer pour ne conserver que les topics dont le technicalName se termine par "ini"
    df_filtered = df_enriched[df_enriched["technicalName"].str.endswith("ini")].copy()
    # Limiter le nombre de topics (ici 30 pour les tests)
    # df_filtered = df_filtered.head(30)
    
    # Calculer pour chaque topic les indicateurs d'alignement
    df_filtered[["NombreChampsAAligner", "NombreChampsAlignes"]] = df_filtered.apply(compute_alignment_counts, axis=1)
    
    # Générer le DataFrame final en respectant le mapping cible/source
    df_final = generate_final_output_df(df_filtered)
    
    try:
        output_blob_name = write_csv_to_blob(target_container, df_final, sep=';', encoding='cp1252')
        message = f"Fichier CSV final sauvegardé dans '{target_container}' sous le nom : {output_blob_name}"
        logging.info(message)
        return func.HttpResponse(message, status_code=200)
    except Exception as e:
        logging.error(f"Erreur lors de l'écriture du CSV final : {str(e)}")
        return func.HttpResponse("Erreur lors de l'écriture du CSV final", status_code=500)
