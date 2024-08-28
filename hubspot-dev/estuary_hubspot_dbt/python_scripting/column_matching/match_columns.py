from google.cloud import bigquery
import pandas as pd
from fuzzywuzzy import process
from dotenv import load_dotenv
import os
import json

# Load environment variables from .env file
load_dotenv()

# Get environment variables
PROJECT_ID = os.getenv('PROJECT_ID')
FIVETRAN_DATASET = os.getenv('FIVETRAN_DATASET')
ESTUARY_DATASET = os.getenv('ESTUARY_DATASET')

# Initialize BigQuery client
client = bigquery.Client()

# Function to get column names from a BigQuery table
def get_column_names(project_id, dataset_id, table_id):
    ''' Retrieves the column names from a specified table in a BigQuery dataset.

        Args:
            project_id (str): The ID of the BigQuery project.
            dataset_id (str): The ID of the BigQuery dataset.
            table_id (str): The ID of the table.

        Returns:
            list: A list of column names from the specified table, excluding the 'id' column and any columns containing 'fivetran' in their name.
        '''

    query = f"""
    SELECT column_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_id}' AND column_name != 'id' AND NOT column_name LIKE '%fivetran%'
    """
    query_job = client.query(query)  # Make an API request.
    results = query_job.result()  # Wait for the job to complete.

    columns = [row.column_name for row in results]
    return columns

# Function to create a mapping table using fuzzy matching
def create_mapping_table(cols_1, cols_2, threshold=0):
    """
    Creates a mapping table between two sets of columns based on fuzzy matching.

    Args:
        cols_1 (list): List of columns from the first set.
        cols_2 (list): List of columns from the second set.
        threshold (int, optional): Similarity threshold for matching. Defaults to 50.

    Returns:
        pandas.DataFrame: Mapping table with columns 'fivetran_column', 'estuary_column', and 'similarity_score'.
    """


    mapping = []
    for col1 in cols_1:
        # Get the best match for each column in cols_1 from cols_2
        best_match, score = process.extractOne(col1, cols_2)
        # Filter out matches below the threshold
        if score >= threshold:
            mapping.append([col1, best_match, score])
        else:
            mapping.append([col1, None, score])

    # Create a DataFrame from the mapping
    mapping_df = pd.DataFrame(mapping, columns=['fivetran_column', 'estuary_column', 'similarity_score'])
    return mapping_df

# key is estuary table name, value is fivetran table name
table_mapping = {
    "contact_lists": "contact_list",
    "contacts": "contact",
    "deal_pipelines": "deal_pipeline",
    "deals": "deal",
    "email_events": "email_event",
    "engagements_calls": "engagement_call",
    "engagements_emails": "engagement_email",
    "engagements_meetings": "engagement_meeting",
    "engagements_notes": "engagement_note",
    "engagements_tasks": "engagement_task",
    "engagements": "engagement",
    "events": "event",
    "forms": "form",
    "marketing_emails": "marketing_email",
    "owners": "owner",
    "ticket_pipelines": "ticket_pipeline",
    "users": "users"
}

if __name__ == "__main__":

    # loop through the hubspot table pairs; key is estuary table name, value is fivetran table name
    for table_name in table_mapping.items():
    # Get column names for both tables from different datasets
        fivetran_tables = get_column_names(PROJECT_ID, FIVETRAN_DATASET, table_name[1])
        estuary_tables = get_column_names(PROJECT_ID, ESTUARY_DATASET, table_name[0])

        # Generate the mapping table
        mapping_df = create_mapping_table(fivetran_tables, estuary_tables)

        # Order the mapping DataFrame by similarity_score
        mapping_df = mapping_df.sort_values('similarity_score', ascending=False)

        output_dir = "output/column_fuzzy_matching"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Write the mapping DataFrame to a CSV file
        mapping_df.to_csv(os.path.join(output_dir, table_name[0]+'.csv'), index=False)

        # Write the mapping DataFrame to a JSON mapping file
        filtered_mapping_df = mapping_df[mapping_df['similarity_score'] >= 80]
        json_mapping = dict(zip(filtered_mapping_df['fivetran_column'], filtered_mapping_df['estuary_column']))
        output_dir = "output/column_fuzzy_matching/json_map"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        with open(os.path.join(output_dir, table_name[0]+'.json'), 'w') as json_file:
            json.dump(json_mapping, json_file)

        # Display the mapping table
        print(mapping_df)
