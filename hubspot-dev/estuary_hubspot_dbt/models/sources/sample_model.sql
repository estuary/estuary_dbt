{{ config(
    materialized='table',
) }}

/* Example mapping dictionary.  If we deploy a dbt package, we'd likely maintain a git repo which has these values hardcoded
    We can use the match_columns module in the python_scripting directory to help automate the generation of these mappings
    with fuzzy matching, but it likely will not be perfect */
{% set mappings = {
    "id": "id",
    "properties_address": "property_address",
    "properties_annualrevenue": "property_annualrevenue",
    "properties_city": "property_city",
    "properties_closedate": "property_closedate",
    "properties_country": "property_country",
    "properties_createdate": "property_createdate",
    "properties_days_to_close": "property_days_to_close",
    "properties_description": "property_description",
    "properties_domain": "property_domain",
    "properties_facebook_company_page": "property_facebook_company_page",
    "properties_first_contact_createdate": "property_first_contact_createdate",
    "properties_first_conversion_date": "property_first_conversion_date",
    "properties_first_conversion_event_name": "property_first_conversion_event_name",
    "properties_first_deal_created_date": "property_first_deal_created_date",
    "properties_founded_year": "property_founded_year",
    "properties_hs_additional_domains": "property_hs_additional_domains",
    "properties_hs_all_assigned_business_unit_ids": "property_hs_all_assigned_business_unit_ids",
    "properties_hs_all_owner_ids": "property_hs_all_owner_ids",
    "properties_hs_analytics_first_timestamp": "property_hs_analytics_first_timestamp",
    "properties_hs_analytics_first_touch_converting_campaign": "property_hs_analytics_first_touch_converting_campaign",
    "properties_hs_analytics_first_visit_timestamp": "property_hs_analytics_first_visit_timestamp",
    "properties_hs_analytics_last_timestamp": "property_hs_analytics_last_timestamp",
    "properties_hs_analytics_last_touch_converting_campaign": "property_hs_analytics_last_touch_converting_campaign",
    "properties_hs_analytics_last_visit_timestamp": "property_hs_analytics_last_visit_timestamp",
    "properties_hs_analytics_latest_source": "property_hs_analytics_latest_source",
    "properties_hs_analytics_latest_source_data_1": "property_hs_analytics_latest_source_data_1",
    "properties_hs_analytics_latest_source_data_2": "property_hs_analytics_latest_source_data_2",
    "properties_hs_analytics_latest_source_timestamp": "property_hs_analytics_latest_source_timestamp",
    "properties_hs_analytics_num_page_views": "property_hs_analytics_num_page_views",
    "properties_hs_analytics_num_visits": "property_hs_analytics_num_visits",
    "properties_hs_analytics_source": "property_hs_analytics_source",
    "properties_hs_analytics_source_data_1": "property_hs_analytics_source_data_1",
    "properties_hs_analytics_source_data_2": "property_hs_analytics_source_data_2",
    "properties_hs_created_by_user_id": "property_hs_created_by_user_id",
    "properties_hs_date_entered_customer": "property_hs_date_entered_customer",
    "properties_hs_date_entered_lead": "property_hs_date_entered_lead",
    "properties_hs_date_entered_marketingqualifiedlead": "property_hs_date_entered_marketingqualifiedlead",
    "properties_hs_date_entered_opportunity": "property_hs_date_entered_opportunity",
    "properties_hs_date_exited_lead": "property_hs_date_exited_lead",
    "properties_hs_date_exited_marketingqualifiedlead": "property_hs_date_exited_marketingqualifiedlead",
    "properties_hs_date_exited_opportunity": "property_hs_date_exited_opportunity",
    "properties_hs_last_booked_meeting_date": "property_hs_last_booked_meeting_date",
    "properties_hs_last_logged_call_date": "property_hs_last_logged_call_date",
    "properties_hs_last_open_task_date": "property_hs_last_open_task_date",
    "properties_hs_last_sales_activity_date": "property_hs_last_sales_activity_date",
    "properties_hs_last_sales_activity_timestamp": "property_hs_last_sales_activity_timestamp",
    "properties_hs_last_sales_activity_type": "property_hs_last_sales_activity_type",
    "properties_hs_lastmodifieddate": "property_hs_lastmodifieddate",
    "properties_hs_latest_meeting_activity": "property_hs_latest_meeting_activity",
    "properties_hs_merged_object_ids": "property_hs_merged_object_ids",
    "properties_hs_num_blockers": "property_hs_num_blockers",
    "properties_hs_num_child_companies": "property_hs_num_child_companies",
    "properties_hs_num_contacts_with_buying_roles": "property_hs_num_contacts_with_buying_roles",
    "properties_hs_num_decision_makers": "property_hs_num_decision_makers",
    "properties_hs_num_open_deals": "property_hs_num_open_deals",
    "properties_hs_object_id": "property_hs_object_id",
    "properties_hs_pipeline": "property_hs_pipeline",
    "properties_hs_sales_email_last_replied": "property_hs_sales_email_last_replied",
    "properties_hs_target_account_probability": "property_hs_target_account_probability",
    "properties_hs_time_in_customer": "property_hs_time_in_customer",
    "properties_hs_time_in_lead": "property_hs_time_in_lead",
    "properties_hs_time_in_marketingqualifiedlead": "property_hs_time_in_marketingqualifiedlead",
    "properties_hs_time_in_opportunity": "property_hs_time_in_opportunity",
    "properties_hs_total_deal_value": "property_hs_total_deal_value",
    "properties_hs_updated_by_user_id": "property_hs_updated_by_user_id",
    "properties_hs_user_ids_of_all_owners": "property_hs_user_ids_of_all_owners",
    "properties_hubspot_owner_assigneddate": "property_hubspot_owner_assigneddate",
    "properties_hubspot_owner_id": "property_hubspot_owner_id",
    "properties_industry": "property_industry",
    "properties_is_public": "property_is_public",
    "properties_lifecyclestage": "property_lifecyclestage",
    "properties_linkedin_company_page": "property_linkedin_company_page",
    "properties_linkedinbio": "property_linkedinbio",
    "properties_name": "property_name",
    "properties_notes_last_contacted": "property_notes_last_contacted",
    "properties_notes_last_updated": "property_notes_last_updated",
    "properties_notes_next_activity_date": "property_notes_next_activity_date",
    "properties_num_associated_contacts": "property_num_associated_contacts",
    "properties_num_associated_deals": "property_num_associated_deals",
    "properties_num_contacted_notes": "property_num_contacted_notes",
    "properties_num_conversion_events": "property_num_conversion_events",
    "properties_num_notes": "property_num_notes",
    "properties_numberofemployees": "property_numberofemployees",
    "properties_phone": "property_phone",
    "properties_recent_conversion_date": "property_recent_conversion_date",
    "properties_recent_conversion_event_name": "property_recent_conversion_event_name",
    "properties_recent_deal_amount": "property_recent_deal_amount",
    "properties_recent_deal_close_date": "property_recent_deal_close_date",
    "properties_state": "property_state",
    "properties_timezone": "property_timezone",
    "properties_total_money_raised": "property_total_money_raised",
    "properties_total_revenue": "property_total_revenue",
    "properties_twitterhandle": "property_twitterhandle",
    "properties_type": "property_type",
    "properties_web_technologies": "property_web_technologies",
    "properties_website": "property_website",
    "properties_zip": "property_zip"
} %}

with source as (

    select * from {{ source('fivetran_hubspot_dev', 'company') }}

),

renamed as (

    /* For phase 1, we can simply loop through the the mapping and alias the fivetran name with the estuary name.
    However, this does not account for the fact that we'd need to recreate all the fivetran model named with the Estuary naming
    convention.

    For that, we'd use the match_tables module in the python_scripting directory to create another mapping, and then we'd have
        a python script that is either triggered as a pre-hook or run manually by the client to spawn the .sql files with the below code
        reformed into a macro. */

    select
        {% for estuary_column_name, fivetran_column_name in mappings.items() %}
            {% if fivetran_column_name == 'id' %}
        id
            {% else %}
        , {{ fivetran_column_name }} as {{ estuary_column_name }}
            {% endif %}
        {% endfor %}
    from source

)

select * from renamed

