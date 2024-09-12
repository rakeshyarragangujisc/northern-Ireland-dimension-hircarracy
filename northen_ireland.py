import pandas as pd

# File paths
ward_to_lgd_path = 'Lookup-Table-Ward2014-to-DEA2014-to-LGD2014.ods'
dz_to_lgd_path = 'geography-data-zone-and-super-data-zone-lookups.xlsx'

# Load the ward lookup file (ODS format)
ward_lookup = pd.read_excel(ward_to_lgd_path, engine='odf')

# Clean and rename columns from the ward lookup file
ward_lookup_cleaned = ward_lookup.dropna(how='all', axis=1)  # Drop empty columns
ward_lookup_cleaned.columns = ['WARD2014', 'WARD2014NAME', 'DEA2014', 'DEA2014Name', 'LGD2014', 'LGD2014NAME'] 

ward_lookup_cleaned = ward_lookup_cleaned.dropna()  # Remove any remaining NaN rows

# Load the specific sheet 'DZ2021_Admin_geog_lookup' from the Excel file
dz_data = pd.read_excel(dz_to_lgd_path, sheet_name='DZ2021_Admin_geog_lookup')

# Extract unique values for each geography level
dz_data = dz_data[['DZ2021_code', 'DZ2021_name', 'SDZ2021_code', 'SDZ2021_name', 'DEA2014_code', 'DEA2014_name',
                   'LGD2014_code', 'LGD2014_name', 'SETTLEMENT2015_code', 'SETTLEMENT2015_name']].drop_duplicates()

ward_data = ward_lookup_cleaned[['WARD2014', 'WARD2014NAME', 'DEA2014', 'LGD2014']].drop_duplicates()

# Create a hierarchy and parent-child relationships
lgd_list = dz_data[['LGD2014_code', 'LGD2014_name']].drop_duplicates().sort_values('LGD2014_code')
lgd_list['Parent_Code'] = 'N92000002'  # Parent is Northern Ireland
lgd_list['geography_grouping_id'] = 'LGD'

settlements_list = dz_data[['SETTLEMENT2015_code', 'SETTLEMENT2015_name', 'LGD2014_code']].drop_duplicates().sort_values('SETTLEMENT2015_code')
settlements_list['Parent_Code'] = settlements_list['LGD2014_code'] 
settlements_list['geography_grouping_id'] = 'Settlement'

dea_list = dz_data[['DEA2014_code', 'DEA2014_name', 'LGD2014_code']].drop_duplicates().sort_values('DEA2014_code')
dea_list['Parent_Code'] = dea_list['LGD2014_code'] 
dea_list['geography_grouping_id'] = 'DEA'

ward_list = ward_data[['WARD2014', 'WARD2014NAME', 'DEA2014', 'LGD2014']].drop_duplicates().sort_values('WARD2014')
ward_list['Parent_Code'] = ward_list['DEA2014'] 
ward_list['geography_grouping_id'] = 'Ward'

sdz_list = dz_data[['SDZ2021_code', 'SDZ2021_name', 'DEA2014_code']].drop_duplicates().sort_values('SDZ2021_code')
sdz_list['Parent_Code'] = sdz_list['DEA2014_code'] 
sdz_list['geography_grouping_id'] = 'SDZ'

dz_list = dz_data[['DZ2021_code', 'DZ2021_name', 'SDZ2021_code']].drop_duplicates().sort_values('DZ2021_code')
dz_list['Parent_Code'] = dz_list['SDZ2021_code'] 
dz_list['geography_grouping_id'] = 'DZ'

# Parent geography mapping dictionary to lookup the parent geography group ID
parent_geography_grouping_id_map = {
    'N92000002': 'northern ireland',
    'LGD': 'LGD',
    'Settlement': 'LGD',
    'DEA': 'LGD',
    'Ward': 'DEA',
    'SDZ': 'DEA',
    'DZ': 'SDZ'
}

# Function to lookup parent geography grouping ID based on parent code
def get_parent_geography_grouping_id(row):
    if row['parent_code'] == 'N92000002':
        return 'northern ireland'
    if row['geography_grouping_id'] in parent_geography_grouping_id_map:
        return parent_geography_grouping_id_map[row['geography_grouping_id']]
    return None

# Merging and renaming final lists
lgd_final = lgd_list[['LGD2014_code', 'LGD2014_name', 'Parent_Code', 'geography_grouping_id']].rename(
    columns={'LGD2014_code': 'code', 'LGD2014_name': 'name', 'Parent_Code': 'parent_code'})

settlements_final = settlements_list[['SETTLEMENT2015_code', 'SETTLEMENT2015_name', 'Parent_Code', 'geography_grouping_id']].rename(
    columns={'SETTLEMENT2015_code': 'code', 'SETTLEMENT2015_name': 'name', 'Parent_Code': 'parent_code'})

dea_final = dea_list[['DEA2014_code', 'DEA2014_name', 'Parent_Code', 'geography_grouping_id']].rename(
    columns={'DEA2014_code': 'code', 'DEA2014_name': 'name', 'Parent_Code': 'parent_code'})

ward_final = ward_list[['WARD2014', 'WARD2014NAME', 'Parent_Code', 'geography_grouping_id']].rename(
    columns={'WARD2014': 'code', 'WARD2014NAME': 'name', 'Parent_Code': 'parent_code'})

sdz_final = sdz_list[['SDZ2021_code', 'SDZ2021_name', 'Parent_Code', 'geography_grouping_id']].rename(
    columns={'SDZ2021_code': 'code', 'SDZ2021_name': 'name', 'Parent_Code': 'parent_code'})

dz_final = dz_list[['DZ2021_code', 'DZ2021_name', 'Parent_Code', 'geography_grouping_id']].rename(
    columns={'DZ2021_code': 'code', 'DZ2021_name': 'name', 'Parent_Code': 'parent_code'})

# Concatenate all lists into a single DataFrame
final_hierarchy = pd.concat([lgd_final, settlements_final, dea_final, ward_final, sdz_final, dz_final])

# Add the parent_geography_grouping_id column
final_hierarchy['parent_geography_grouping_id'] = final_hierarchy.apply(get_parent_geography_grouping_id, axis=1)

# Ensure the correct column order and export to Excel
final_hierarchy = final_hierarchy[['code', 'parent_code', 'name', 'geography_grouping_id', 'parent_geography_grouping_id']]
final_hierarchy.to_excel('Northern_Ireland_Census_Hierarchy.xlsx', index=False)

print("Hierarchy structure with parent codes and parent geography grouping ID successfully written to 'Northern_Ireland_Census_Hierarchy.xlsx'")
