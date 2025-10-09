-- Average risk per customer
CREATE OR REPLACE VIEW VW_CUSTOMER_RISK_ASSIGNED AS
SELECT 
    t.customer_id,
    ROUND(t.avg_risk) AS assigned_risk_profile_id,
    rp.risk_profile AS assigned_risk_profile
FROM (
    SELECT 
        ca.customer_id,
        AVG(a.risk_profile_id) AS avg_risk
    FROM FF_CUSTOMER_ANSWERS ca
    JOIN FF_ANSWERS a  
        ON ca.question_answer_id = a.question_answer_id
    GROUP BY ca.customer_id
) t
LEFT JOIN FF_RISK_PROFILES rp
    ON rp.risk_profile_id = ROUND(t.avg_risk);


-- View to create total assets for each customer
CREATE OR REPLACE VIEW VW_CUSTOMER_TOTAL_ASSETS AS
SELECT 
    ca.customer_id,
    SUM(ca.total) AS total_assets
FROM FF_CUSTOMER_ASSETS ca
GROUP BY ca.customer_id;


-- Weighted risk average
CREATE OR REPLACE VIEW VW_CUSTOMER_WEIGHTED_RISK_ASSIGNED AS
SELECT
    t.customer_id,
    ROUND(t.weighted_avg_risk) AS assigned_risk_profile_id,
    rps.risk_profile AS assigned_risk_profile
FROM (
    SELECT
        ca.customer_id,
        SUM(
            CASE
                WHEN a.risk_profile_id = 1 THEN 3 * 1
                WHEN a.risk_profile_id = 2 THEN 2 * 2
                WHEN a.risk_profile_id = 3 THEN 1 * 3
                WHEN a.risk_profile_id = 4 THEN 2 * 4
                WHEN a.risk_profile_id = 5 THEN 3 * 5
                ELSE 0
            END
        ) /
        NULLIF(
            SUM(
                CASE
                    WHEN a.risk_profile_id = 1 THEN 3
                    WHEN a.risk_profile_id = 2 THEN 2
                    WHEN a.risk_profile_id = 3 THEN 1
                    WHEN a.risk_profile_id = 4 THEN 2
                    WHEN a.risk_profile_id = 5 THEN 3
                    ELSE 0
                END
            ), 0
        ) AS weighted_avg_risk
    FROM FF_CUSTOMER_ANSWERS ca
    JOIN FF_ANSWERS a 
        ON ca.question_answer_id = a.question_answer_id
    WHERE a.risk_profile_id IS NOT NULL
    GROUP BY ca.customer_id
) t
LEFT JOIN FF_RISK_PROFILES rps
    ON rps.risk_profile_id = ROUND(t.weighted_avg_risk);
