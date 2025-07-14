import pandas as pd

# Read the enhanced enrichment CSV
df = pd.read_csv('enhanced_enrichment.csv')

print(f"Total rows: {len(df)}")
print(f"Total columns: {len(df.columns)}")

# Check for employee columns
employee_cols = [col for col in df.columns if col.startswith('cs_employee_')]
print(f"\nEmployee columns found: {len(employee_cols)}")

# Check specific fields the user mentioned
target_fields = [
    'cs_employee_edu_1_institution_name',
    'cs_employee_edu_1_institution_full_address', 
    'cs_employee_edu_1_institution_country_iso2',
    'cs_employee_edu_1_institution_country_iso3',
    'cs_employee_edu_1_institution_regions',
    'cs_employee_edu_1_institution_city',
    'cs_employee_projected_total_salary_updated_at',
    'cs_employee_language_2_order',
    'cs_employee_projected_total_salary_period',
    'cs_employee_projected_base_salary_median',
    'cs_employee_active_experience_department',
    'cs_employee_active_experience_management_level',
    'cs_employee_is_decision_maker',
    'cs_employee_inferred_skills',
    'cs_employee_historical_skills',
    'cs_employee_total_experience_duration_months',
    'cs_employee_last_graduation_date',
    'cs_employee_education_degrees',
    'cs_employee_connections_count',
    'cs_employee_active_experience_company_id'
]

print("\nChecking specific target fields:")
for field in target_fields:
    if field in df.columns:
        non_empty = df[field].notna().sum()
        print(f"✅ {field}: {non_empty}/{len(df)} rows have data")
    else:
        print(f"❌ {field}: NOT FOUND in CSV")

# Check first few rows for employee data
print("\n\nFirst 3 rows - checking if employee data is populated:")
for i in range(min(3, len(df))):
    row = df.iloc[i]
    print(f"\nRow {i+1}: {row.get('recipient_email', 'N/A')}")
    
    # Check if any employee fields have data
    employee_data_count = sum(1 for col in employee_cols if pd.notna(row.get(col)) and str(row.get(col)).strip() != '')
    print(f"  Employee fields with data: {employee_data_count}/{len(employee_cols)}")
    
    # Show some sample employee data
    sample_fields = ['cs_employee_full_name', 'cs_employee_id', 'cs_employee_connections_count', 'cs_employee_edu_1_institution_name']
    for field in sample_fields:
        if field in df.columns:
            value = row.get(field)
            if pd.notna(value) and str(value).strip() != '':
                print(f"  {field}: {value}")
            else:
                print(f"  {field}: EMPTY") 