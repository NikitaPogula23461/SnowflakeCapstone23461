SELECT
 
--Surrogate key
ROW_NUMBER() OVER (ORDER BY campaign_id) AS campaignkey,
 
--CampaignID
campaign_id AS campaignid,

campaign_name,

campaign_type,
 
--Target Audience Segment
audience_segment AS target_audience_segment,
 
--Budget
budget,
 
--Duration
campaign_duration_days AS duration,
 
--ROI
expected_roi AS roi,
 
start_date,
 
end_date
 
FROM {{ ref('campaign_data_silver') }}