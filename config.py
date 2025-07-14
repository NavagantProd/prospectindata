"""
Configuration settings for CoreSignal enrichment pipeline
"""
import os
from typing import Dict, Any

# CoreSignal API Configuration
CORESIGNAL_BASE_URL = "https://api.coresignal.com/cdapi/v2"
CORESIGNAL_API_KEY = os.getenv("CORESIGNAL_API_KEY", "")

# API Endpoints
ENDPOINTS = {
    "company_search": "/company_base/search/filter",
    "company_collect": "/company_base/collect",
    "member_search": "/member/search/filter",
    "member_collect": "/member/collect"
}

# Rate limiting and retry settings
RATE_LIMIT_DELAY = 1.0  # seconds between requests
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# File paths
DEFAULT_INPUT_FILE = "leads.csv"
DEFAULT_OUTPUT_FILE = "leads_enriched.csv"
DEFAULT_LOG_FILE = "pipeline.log"

# Column mappings for input CSV
INPUT_COLUMN_MAPPING = {
    'name': 'contact_full_name',
    'email': 'contact_email', 
    'company_name': 'contact_firm_name',  # <-- updated to match actual CSV
    'company_website': 'cs_company_website'
}

# Output schema - all enrichment fields
OUTPUT_SCHEMA = [
    # Core contact info
    "name", "email", "company_name", "company_website",
    
    # Company enrichment fields
    "company_id", "company_display_name", "company_canonical_url", 
    "company_website_url", "company_industry", "company_size", "company_type",
    "company_founded", "company_employees", "company_description", 
    "company_headquarters", "company_specialties", "company_logo_url",
    
    # Employee enrichment fields  
    "employee_id", "employee_full_name", "employee_first_name", "employee_last_name",
    "employee_title", "employee_headline", "employee_url", "employee_canonical_url",
    "employee_location", "employee_country", "employee_industry", 
    "employee_connections_count", "employee_summary", "employee_experience",
    "employee_education", "employee_created", "employee_last_updated",
    
    # Enrichment metadata
    "enrichment_status", "enrichment_error", "company_match_score", 
    "employee_match_score", "api_calls_made",
    # --- HIGH-VALUE COMPANY FIELDS (from curl) ---
    "company_name_exact",
    "company_website_domain_only",
    "company_industry_exact",
    "company_size_range",
    "company_employees_count",
    "company_founded_year",
    "company_is_public",
    "company_ipo_date",
    "company_stock_ticker_ticker",
    "company_revenue_annual_source_1_annual_revenue",
    "company_revenue_annual_range_source_4_annual_revenue_range_from",
    "company_hq_country_iso2",
    "company_hq_region",
    "company_hq_city",
    "company_hq_state",
    "company_locations_full_location_address",
    "company_income_statements_net_income",
    "company_income_statements_ebitda_margin",
    "company_total_website_visits_monthly",
    "company_visits_breakdown_by_country_country",
    "company_bounce_rate",
    "company_pages_per_visit",
    "company_employees_count_change_yearly_percentage",
    "company_total_website_visits_change_monthly_percentage",
    "company_active_job_postings_count_change_monthly_percentage",
    "company_followers_count_professional_network",
    "company_followers_count_twitter",
    # Company updates (flattened, up to 5)
    "company_updates_1_date", "company_updates_1_description", "company_updates_1_reactions_count", "company_updates_1_comments_count",
    "company_updates_2_date", "company_updates_2_description", "company_updates_2_reactions_count", "company_updates_2_comments_count",
    "company_updates_3_date", "company_updates_3_description", "company_updates_3_reactions_count", "company_updates_3_comments_count",
    "company_updates_4_date", "company_updates_4_description", "company_updates_4_reactions_count", "company_updates_4_comments_count",
    "company_updates_5_date", "company_updates_5_description", "company_updates_5_reactions_count", "company_updates_5_comments_count",
    "company_last_funding_round_name",
    "company_last_funding_round_announced_date",
    "company_last_funding_round_amount_raised",
    # Funding rounds (flattened, up to 5)
    "company_funding_rounds_1_name",
    "company_funding_rounds_2_name",
    "company_funding_rounds_3_name",
    "company_funding_rounds_4_name",
    "company_funding_rounds_5_name",
    "company_parent_company_information_parent_company_name",
    "company_acquired_by_summary_acquirer_name",
    # Technologies used (flattened, up to 5)
    "company_technologies_used_1_technology",
    "company_technologies_used_2_technology",
    "company_technologies_used_3_technology",
    "company_technologies_used_4_technology",
    "company_technologies_used_5_technology",
    "company_num_news_articles",
    # --- HIGH-VALUE EMPLOYEE FIELDS (from curl) ---
    "employee_full_name_exact",
    "employee_headline",
    "employee_location_full_exact",
    "employee_location_country_iso2",
    "employee_connections_count",
    "employee_followers_count",
    "employee_primary_professional_email_exact",
    "employee_active_experience_title",
    "employee_active_experience_company_id",
    "employee_total_experience_duration_months",
    # Experience (flattened, up to 5)
    "employee_experience_1_position_title_exact",
    "employee_experience_1_duration_months",
    "employee_experience_1_company_name_exact",
    "employee_experience_2_position_title_exact",
    "employee_experience_2_duration_months",
    "employee_experience_2_company_name_exact",
    "employee_experience_3_position_title_exact",
    "employee_experience_3_duration_months",
    "employee_experience_3_company_name_exact",
    "employee_experience_4_position_title_exact",
    "employee_experience_4_duration_months",
    "employee_experience_4_company_name_exact",
    "employee_experience_5_position_title_exact",
    "employee_experience_5_duration_months",
    "employee_experience_5_company_name_exact",
    # Education (flattened, up to 3)
    "employee_education_1_institution_name_exact",
    "employee_education_1_degree_exact",
    "employee_education_2_institution_name_exact",
    "employee_education_2_degree_exact",
    "employee_education_3_institution_name_exact",
    "employee_education_3_degree_exact",
    "employee_last_graduation_date",
    "employee_inferred_skills",
    "employee_historical_skills",
    "employee_recommendations_count",
    "employee_experience_change_last_identified_at",
    "employee_experience_recently_started_date_from",
    "employee_experience_recently_closed_date_to",
    # --- ADDITIONAL HIGH-VALUE FIELDS FROM MULTI-SOURCE JSON ---
    # Social/contact URLs
    "company_facebook_urls", "company_twitter_urls", "company_instagram_urls", "company_youtube_urls", "company_github_urls", "company_reddit_urls", "company_discord_urls", "company_pinterest_urls", "company_tiktok_urls", "company_crunchbase_url",
    # Company logo (raw and URL)
    "company_logo_raw", "company_logo_url",
    # NLP/text fields
    "company_description_enriched", "company_description_metadata_raw",
    # Codes and categories
    "company_sic_codes", "company_naics_codes", "company_categories_and_keywords",
    # Status/type/ownership
    "company_type", "company_status_value", "company_status_comment", "company_ownership_status",
    # Headquarters and location
    "company_hq_country", "company_hq_country_iso3", "company_hq_full_address", "company_hq_street", "company_hq_zipcode",
    # Locations (flattened up to 5)
    "company_locations_1_address", "company_locations_1_is_primary", "company_locations_2_address", "company_locations_2_is_primary", "company_locations_3_address", "company_locations_3_is_primary", "company_locations_4_address", "company_locations_4_is_primary", "company_locations_5_address", "company_locations_5_is_primary",
    # Company updates (flattened up to 5, already present but ensure all text fields)
    # Technologies used (flattened up to 5, already present but ensure all text fields)
    # Funding rounds (flattened up to 5, already present but ensure all text fields)
    # Competitors (flattened up to 5)
    "company_competitors_1_website", "company_competitors_1_similarity_score", "company_competitors_1_total_website_visits_monthly", "company_competitors_1_category", "company_competitors_1_rank_category",
    "company_competitors_2_website", "company_competitors_2_similarity_score", "company_competitors_2_total_website_visits_monthly", "company_competitors_2_category", "company_competitors_2_rank_category",
    "company_competitors_3_website", "company_competitors_3_similarity_score", "company_competitors_3_total_website_visits_monthly", "company_competitors_3_category", "company_competitors_3_rank_category",
    "company_competitors_4_website", "company_competitors_4_similarity_score", "company_competitors_4_total_website_visits_monthly", "company_competitors_4_category", "company_competitors_4_rank_category",
    "company_competitors_5_website", "company_competitors_5_similarity_score", "company_competitors_5_total_website_visits_monthly", "company_competitors_5_category", "company_competitors_5_rank_category",
    # Company phone numbers/emails (flattened up to 3)
    "company_phone_number_1", "company_phone_number_2", "company_phone_number_3",
    "company_email_1", "company_email_2", "company_email_3",
    # Visits breakdown by country (flattened up to 5)
    "company_visits_country_1_name", "company_visits_country_1_percentage", "company_visits_country_1_monthly_change",
    "company_visits_country_2_name", "company_visits_country_2_percentage", "company_visits_country_2_monthly_change",
    "company_visits_country_3_name", "company_visits_country_3_percentage", "company_visits_country_3_monthly_change",
    "company_visits_country_4_name", "company_visits_country_4_percentage", "company_visits_country_4_monthly_change",
    "company_visits_country_5_name", "company_visits_country_5_percentage", "company_visits_country_5_monthly_change",
    # Visits breakdown by gender/age
    "company_visits_male_percentage", "company_visits_female_percentage",
    "company_visits_age_18_24_percentage", "company_visits_age_25_34_percentage", "company_visits_age_35_44_percentage", "company_visits_age_45_54_percentage", "company_visits_age_55_64_percentage", "company_visits_age_65_plus_percentage",
    # Website analytics
    "company_bounce_rate", "company_pages_per_visit", "company_average_visit_duration_seconds",
    # Similarly ranked websites (flattened up to 5)
    "company_similar_website_1", "company_similar_website_2", "company_similar_website_3", "company_similar_website_4", "company_similar_website_5",
    # LinkedIn followers by month (flattened up to 5)
    "company_linkedin_followers_1_count", "company_linkedin_followers_1_date",
    "company_linkedin_followers_2_count", "company_linkedin_followers_2_date",
    "company_linkedin_followers_3_count", "company_linkedin_followers_3_date",
    "company_linkedin_followers_4_count", "company_linkedin_followers_4_date",
    "company_linkedin_followers_5_count", "company_linkedin_followers_5_date",
    # Employees count by month (flattened up to 5)
    "company_employees_1_count", "company_employees_1_date",
    "company_employees_2_count", "company_employees_2_date",
    "company_employees_3_count", "company_employees_3_date",
    "company_employees_4_count", "company_employees_4_date",
    "company_employees_5_count", "company_employees_5_date",
    # Employees count by seniority (current)
    "company_employees_count_owner", "company_employees_count_founder", "company_employees_count_clevel", "company_employees_count_partner", "company_employees_count_vp", "company_employees_count_head", "company_employees_count_director", "company_employees_count_manager", "company_employees_count_senior", "company_employees_count_intern", "company_employees_count_specialist", "company_employees_count_other_management",
    # Key executives (flattened up to 5)
    "company_key_executive_1_full_name", "company_key_executive_1_position_title",
    "company_key_executive_2_full_name", "company_key_executive_2_position_title",
    "company_key_executive_3_full_name", "company_key_executive_3_position_title",
    "company_key_executive_4_full_name", "company_key_executive_4_position_title",
    "company_key_executive_5_full_name", "company_key_executive_5_position_title",
    # Top previous companies (flattened up to 5)
    "company_top_previous_company_1_name", "company_top_previous_company_1_count",
    "company_top_previous_company_2_name", "company_top_previous_company_2_count",
    "company_top_previous_company_3_name", "company_top_previous_company_3_count",
    "company_top_previous_company_4_name", "company_top_previous_company_4_count",
    "company_top_previous_company_5_name", "company_top_previous_company_5_count",
    # Top next companies (flattened up to 5)
    "company_top_next_company_1_name", "company_top_next_company_1_count",
    "company_top_next_company_2_name", "company_top_next_company_2_count",
    "company_top_next_company_3_name", "company_top_next_company_3_count",
    "company_top_next_company_4_name", "company_top_next_company_4_count",
    "company_top_next_company_5_name", "company_top_next_company_5_count",
    # Add more as needed from the JSON for full coverage
    # --- ADDITIONAL FIELDS FROM COMPANY JSONS (FULL COVERAGE) ---
    # Revenue (annual, quarterly, range, by source)
    "company_revenue_annual_source_5_annual_revenue", "company_revenue_annual_source_5_annual_revenue_currency",
    "company_revenue_annual_range_source_6_annual_revenue_range_from", "company_revenue_annual_range_source_6_annual_revenue_range_to", "company_revenue_annual_range_source_6_annual_revenue_range_currency",
    # Visits breakdown by country (flattened up to 5, with all subfields)
    "company_visits_country_1_name", "company_visits_country_1_percentage", "company_visits_country_1_monthly_change",
    "company_visits_country_2_name", "company_visits_country_2_percentage", "company_visits_country_2_monthly_change",
    "company_visits_country_3_name", "company_visits_country_3_percentage", "company_visits_country_3_monthly_change",
    "company_visits_country_4_name", "company_visits_country_4_percentage", "company_visits_country_4_monthly_change",
    "company_visits_country_5_name", "company_visits_country_5_percentage", "company_visits_country_5_monthly_change",
    # Company updates (flattened up to 5, all subfields)
    "company_updates_1_date", "company_updates_1_description", "company_updates_1_reactions_count", "company_updates_1_comments_count",
    "company_updates_2_date", "company_updates_2_description", "company_updates_2_reactions_count", "company_updates_2_comments_count",
    "company_updates_3_date", "company_updates_3_description", "company_updates_3_reactions_count", "company_updates_3_comments_count",
    "company_updates_4_date", "company_updates_4_description", "company_updates_4_reactions_count", "company_updates_4_comments_count",
    "company_updates_5_date", "company_updates_5_description", "company_updates_5_reactions_count", "company_updates_5_comments_count",
    # Funding rounds (flattened up to 5, all subfields)
    "company_funding_rounds_1_name", "company_funding_rounds_1_announced_date", "company_funding_rounds_1_lead_investors", "company_funding_rounds_1_amount_raised", "company_funding_rounds_1_amount_raised_currency", "company_funding_rounds_1_num_investors",
    "company_funding_rounds_2_name", "company_funding_rounds_2_announced_date", "company_funding_rounds_2_lead_investors", "company_funding_rounds_2_amount_raised", "company_funding_rounds_2_amount_raised_currency", "company_funding_rounds_2_num_investors",
    "company_funding_rounds_3_name", "company_funding_rounds_3_announced_date", "company_funding_rounds_3_lead_investors", "company_funding_rounds_3_amount_raised", "company_funding_rounds_3_amount_raised_currency", "company_funding_rounds_3_num_investors",
    "company_funding_rounds_4_name", "company_funding_rounds_4_announced_date", "company_funding_rounds_4_lead_investors", "company_funding_rounds_4_amount_raised", "company_funding_rounds_4_amount_raised_currency", "company_funding_rounds_4_num_investors",
    "company_funding_rounds_5_name", "company_funding_rounds_5_announced_date", "company_funding_rounds_5_lead_investors", "company_funding_rounds_5_amount_raised", "company_funding_rounds_5_amount_raised_currency", "company_funding_rounds_5_num_investors",
    # Technologies used (flattened up to 5, all subfields)
    "company_technologies_used_1_technology", "company_technologies_used_1_first_verified_at", "company_technologies_used_1_last_verified_at",
    "company_technologies_used_2_technology", "company_technologies_used_2_first_verified_at", "company_technologies_used_2_last_verified_at",
    "company_technologies_used_3_technology", "company_technologies_used_3_first_verified_at", "company_technologies_used_3_last_verified_at",
    "company_technologies_used_4_technology", "company_technologies_used_4_first_verified_at", "company_technologies_used_4_last_verified_at",
    "company_technologies_used_5_technology", "company_technologies_used_5_first_verified_at", "company_technologies_used_5_last_verified_at",
    # Followers counts (all sources)
    "company_followers_count_linkedin", "company_followers_count_twitter", "company_followers_count_owler",
    # News articles
    "company_num_news_articles",
    # Any other actionable, non-id, non-forbidden fields from the JSONs
    # ... (add as needed for full coverage)
]