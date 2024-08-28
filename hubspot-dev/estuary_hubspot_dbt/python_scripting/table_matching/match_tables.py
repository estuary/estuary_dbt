from google.cloud import bigquery
from fuzzywuzzy import process
from dotenv import load_dotenv
import os
import csv
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Get environment variables
PROJECT_ID = os.getenv('PROJECT_ID')
FIVETRAN_DATASET = os.getenv('FIVETRAN_DATASET')
ESTUARY_DATASET = os.getenv('ESTUARY_DATASET')


# Initialize BigQuery client
client = bigquery.Client()

def load_table_names(project_id, dataset_id):
    ''' Retrieves the names of all tables in a given project and dataset.

        Args:
            project_id (str): The ID of the project.
            dataset_id (str): The ID of the dataset.

        Returns:
            set: A set containing the names of all tables in the specified project and dataset.
    '''

    query = f"""
    SELECT table_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    """
    query_job = client.query(query)  # Make an API request.
    results = query_job.result()  # Wait for the job to complete.

    table_names = [row.table_name for row in results]
    return set(table_names)

def get_different_sets(estuary_table_names, fivetran_table_names, threshold=0):
    """
    Compares two sets of table names and returns the sets of tables that are different or similar.
    Parameters:
    - estuary_table_names (set): Set of table names from Estuary.
    - fivetran_table_names (set): Set of table names from Fivetran.
    - threshold (int, optional): The minimum similarity_score for a table name to be considered similar. Defaults to 0.

    Returns:
    - estuary_only (set): Set of table names that exist only in Estuary (using exact matching).
    - fivetran_only (set): Set of table names that exist only in Fivetran (using exact matching).
    - exact_matches (set): Set of table names that exist in both Estuary and Fivetran (using exact matching).
    - similar (list): List of tuples containing the table names from Estuary, the best matching table name from Fivetran, and the similarity_score.
    - fuzzy_matching_df (DataFrame): DataFrame containing the table name mappings with similarity_scores, sorted in descending order of similarity_score.
    """

    # Create sets for the three conditions
    estuary_only = estuary_table_names - fivetran_table_names
    fivetran_only = fivetran_table_names - estuary_table_names
    exact_matches = estuary_table_names.intersection(fivetran_table_names)

    # Create a set for the table names with a fuzzy matching score above the threshold
    similar = []
    for est_table in estuary_table_names:
        best_match, score = process.extractOne(est_table, fivetran_table_names)
        if score >= threshold:
            similar.append((est_table, best_match, score))
    fuzzy_matching_df = pd.DataFrame(similar, columns=['fivetran_column', 'estuary_column', 'similarity_score']).sort_values('similarity_score', ascending=False)
    return estuary_only, fivetran_only, exact_matches, fuzzy_matching_df

def write_to_output(estuary_only, fivetran_only, exact_matches, fuzzy_matching_df):
    """
    Write the given data to CSV and JSON files.
    Parameters:
    - estuary_only (list): A list of table names that exist only in the Estuary database.
    - fivetran_only (list): A list of table names that exist only in the Fivetran database.
    - exact_matches (list): A list of table names that have exact matches in both databases.
    - similar (list): A list of tuples containing the Estuary table name, Fivetran table name, and similarity_score.
    - fuzzy_matching_df (pandas.DataFrame): A DataFrame containing the mapping between Estuary and Fivetran tables.
    Returns:
    None
    """

    csv_output_dir = "output/tables_matching/csv"
    json_output_dir = "output/tables_matching/json"

    # Write estuary_only to CSV
    with open(os.path.join(csv_output_dir, 'estuary_only.csv'), mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Table Name'])
        for table in estuary_only:
            writer.writerow([table])

    # Write fivetran_only to CSV
    with open(os.path.join(csv_output_dir, 'fivetran_only.csv'), mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Table Name'])
        for table in fivetran_only:
            writer.writerow([table])

    # Write exact_matches to CSV
    with open(os.path.join(csv_output_dir, 'exact_matches.csv'), mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Table Name'])
        for table in exact_matches:
            writer.writerow([table])

    # Write fuzzy_matching_df to CSV
    # Filter fuzzy_matching_df based on similarity_score
    filtered_fuzzy_df = fuzzy_matching_df[fuzzy_matching_df['similarity_score'] >= 80]

    # Write filtered_fuzzy_df to JSON
    filtered_fuzzy_df.to_json(os.path.join(json_output_dir, 'fuzzy_matches.json'), orient='records')
