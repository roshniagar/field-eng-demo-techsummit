-- Top 10 critical/elevated subscribers by CLV at risk
-- Source: Lakebase Postgres streaming.synced_subscriber_position
SELECT
    subscriber_id,
    plan_type,
    tenure_months,
    risk_band,
    churn_risk_score,
    churn_reason,
    clv_at_risk_usd,
    open_ticket_count
FROM streaming.synced_subscriber_position
WHERE risk_band IN ('critical', 'elevated')
ORDER BY clv_at_risk_usd DESC
LIMIT 10;
