{% snapshot snp_campaign_data %}
 
{{
config(
target_database='CAPSTONE_DATA_RAW',
target_schema='bronze',
unique_key='campaign_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}
 
SELECT *
FROM {{ ref('campaign') }}
 
QUALIFY ROW_NUMBER() OVER(
PARTITION BY campaign_id
ORDER BY last_modified_date DESC
)=1
 
{% endsnapshot %}