from table_matching.match_tables import load_table_names, get_different_sets, write_to_output
from column_matching.match_columns import create_mapping_table, get_column_names, table_mapping
from dotenv import load_dotenv
import os
import json

# Load environment variables from .env file
load_dotenv()

# Get environment variables
PROJECT_ID = os.getenv('PROJECT_ID')
FIVETRAN_DATASET = os.getenv('FIVETRAN_DATASET')
ESTUARY_DATASET = os.getenv('ESTUARY_DATASET')

def setup():
    # Create directories if they don't already exist
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, 'output')
    os.makedirs(os.path.join(output_dir, 'columns_matching', 'csv'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'columns_matching', 'json'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'tables_matching', 'csv'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'tables_matching', 'json'), exist_ok=True)

    os.makedirs(os.path.join(current_dir, 'input'), exist_ok=True)

    return

def update_table_matching():
    estuary_table_names = load_table_names(project_id=PROJECT_ID, dataset_id=ESTUARY_DATASET)
    fivetran_table_names = load_table_names(project_id=PROJECT_ID, dataset_id=FIVETRAN_DATASET)

    estuary_only, fivetran_only, exact_matches, fuzzy_matching_df = get_different_sets(
        estuary_table_names=estuary_table_names, fivetran_table_names=fivetran_table_names
    )

    write_to_output(estuary_only, fivetran_only, exact_matches, fuzzy_matching_df)

def update_column_matching():

    csv_output_dir = "output/columns_matching/csv"
    json_output_dir = "output/columns_matching/json"
    # loop through the hubspot table pairs; key is estuary table name, value is fivetran table name
    for table_name in table_mapping.items():

        # Get column names for both tables from different datasets
        fivetran_tables = get_column_names(PROJECT_ID, FIVETRAN_DATASET, table_name[1])
        estuary_tables = get_column_names(PROJECT_ID, ESTUARY_DATASET, table_name[0])

        # Generate the mapping table
        mapping_df = create_mapping_table(fivetran_tables, estuary_tables)

        # Order the mapping DataFrame by similarity_score
        mapping_df = mapping_df.sort_values('similarity_score', ascending=False)

        # Write the mapping DataFrame to a CSV file
        mapping_df.to_csv(os.path.join(csv_output_dir, table_name[0]+'.csv'), index=False)

        # Write the mapping DataFrame to a JSON mapping file
        filtered_mapping_df = mapping_df[mapping_df['similarity_score'] >= 80]
        json_mapping = dict(zip(filtered_mapping_df['fivetran_column'], filtered_mapping_df['estuary_column']))
        with open(os.path.join(json_output_dir, table_name[0]+'.json'), 'w') as json_file:
            json.dump(json_mapping, json_file)


if __name__ == "__main__":
    setup()
    update_table_matching()
    update_column_matching()
