CREATE OR REPLACE VIEW VW_PERCENTAGE_FEE_PER_FUND AS
    SELECT FA.FUND_ID, (SUM(AC.PERCENTAGE_FEE * FA.PERCENT_OF_FUND)/100) AS FUND_FEE_PERCENT
    FROM FF_ASSET_CLASSES AC
        JOIN FF_FUND_ASSETS FA ON AC.ASSET_CLASS_ID = FA.ASSET_CLASS_ID
    GROUP BY FUND_ID ORDER BY FUND_ID;

-- new approach
ALTER TABLE FF_POTENTIAL_FUNDS
ADD MIN_REVENUE NUMBER(12,2);

ALTER TABLE FF_POTENTIAL_FUNDS
ADD MAX_REVENUE NUMBER(12,2);

ALTER TABLE FF_POTENTIAL_FUNDS
ADD MAX_REVENUE_PER_CUSTOMER NUMBER(12,2);

UPDATE FF_POTENTIAL_FUNDS SET FUND_DESCRIPTION='above 60, more than 5 mil (mostly avg and little below avg)' WHERE FUND_ID=1;
UPDATE FF_POTENTIAL_FUNDS SET FUND_DESCRIPTION='married women (mostly avg, little below avg)(they have high assets in general)' WHERE FUND_ID=2;
UPDATE FF_POTENTIAL_FUNDS SET FUND_DESCRIPTION='divorced or single, 1 to 5 mil (above avg)' WHERE FUND_ID=3;
UPDATE FF_POTENTIAL_FUNDS SET FUND_DESCRIPTION='customers that are between 30 and 55 and have 2 or more dependents (avg and above avg)' WHERE FUND_ID=4;
UPDATE FF_POTENTIAL_FUNDS SET FUND_DESCRIPTION='customers from north east region (avg and above avg)' WHERE FUND_ID=5;

CREATE OR REPLACE PROCEDURE calculate_fund_min_max_revenue (
    p_customer_ids IN CUSTOMER_ID_ARRAY,
    p_fund_id      IN NUMBER
)
AS
    v_customer_count    NUMBER := 0;
    v_fee_percent       NUMBER := NULL;
    v_min_per_customer  NUMBER := 0;
    v_max_per_customer  NUMBER := 0;
    v_max_sum_component NUMBER := 0;
    v_min_revenue       NUMBER := 0;
    v_max_revenue       NUMBER := 0;
    v_max_revenue_per_cust NUMBER := 0;
BEGIN
    -- 0) quick count, validate non-empty
    SELECT COUNT(*) INTO v_customer_count FROM TABLE(p_customer_ids);

    -- 1) fetch fee once (raise if missing)
    BEGIN
        SELECT ff.fund_fee_percent
        INTO v_fee_percent
        FROM VW_PERCENTAGE_FEE_PER_FUND ff
        WHERE ff.fund_id = p_fund_id;
    END;

    -- 2) single set-based pass: compute per-customer min_invest, max_allowed, actual_max_candidate,
    --    then aggregate MIN(min_invest), MAX(max_allowed), and SUM(actual_max_candidate)
    WITH cust_calc AS (
        SELECT
            t.column_value AS customer_id,
            NVL(a.total_assets,0) AS total_assets,
            NVL(r.assigned_risk_profile_id,0) AS risk_profile,
            -- per-customer min based on risk band
            CASE NVL(r.assigned_risk_profile_id,0)
                WHEN 1 THEN NVL(a.total_assets,0) * 0.001
                WHEN 2 THEN NVL(a.total_assets,0) * 0.005
                WHEN 3 THEN NVL(a.total_assets,0) * 0.010
                WHEN 4 THEN NVL(a.total_assets,0) * 0.015
                WHEN 5 THEN NVL(a.total_assets,0) * 0.020
                ELSE 0
            END AS per_min_invest,
            -- per-customer max allowed (cap)
            CASE NVL(r.assigned_risk_profile_id,0)
                WHEN 1 THEN NVL(a.total_assets,0) * 0.05
                WHEN 2 THEN NVL(a.total_assets,0) * 0.10
                WHEN 3 THEN NVL(a.total_assets,0) * 0.15
                WHEN 4 THEN NVL(a.total_assets,0) * 0.20
                WHEN 5 THEN NVL(a.total_assets,0) * 0.30
                ELSE 0
            END AS per_max_allowed
        FROM TABLE(p_customer_ids) t
        LEFT JOIN vw_customer_total_assets a
            ON t.column_value = a.customer_id
        LEFT JOIN vw_customer_weighted_risk_assigned r
            ON t.column_value = r.customer_id
    )
    SELECT
        NVL(MIN(per_min_invest),0),
        NVL(MAX(per_max_allowed),0)
    INTO v_min_per_customer, v_max_per_customer
    FROM cust_calc;
    
    SELECT SUM(LEAST(v_max_per_customer, total_assets))
    INTO v_max_sum_component
    FROM vw_customer_total_assets a
        JOIN TABLE(p_customer_ids) t ON a.customer_id = t.column_value;


    -- 3) compute revenues
    v_min_revenue := v_min_per_customer * v_customer_count * v_fee_percent;
    v_max_revenue := v_max_sum_component * v_fee_percent;
    v_max_revenue_per_cust := v_max_revenue/v_customer_count;

    -- 4) update single table
    UPDATE FF_POTENTIAL_FUNDS
    SET minimum_investment_required = v_min_per_customer,
        maximum_investment_allowed  = v_max_per_customer,
        min_revenue                 = v_min_revenue,
        max_revenue                 = v_max_revenue,
        max_revenue_per_customer    = v_max_revenue_per_cust
    WHERE fund_id = p_fund_id;
    COMMIT;
END;
/

-- FUND 1 - above 60, more than 5 mil (mostly avg and little below avg)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT C.CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS C 
        JOIN VW_CUSTOMER_TOTAL_ASSETS TA ON C.CUSTOMER_ID = TA.CUSTOMER_ID
    WHERE C.AGE>60 AND TA.TOTAL_ASSETS>5000000;
    calculate_fund_min_max_revenue(V_CUSTOMER_IDS, 1);
END;
/

-- FUND 2 - married women (mostly avg, little below avg)(they have high assets in general)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS
    WHERE MARITAL_STATUS='Married' AND GENDER='Female';
    calculate_fund_min_max_revenue(V_CUSTOMER_IDS, 2);
END;
/

-- FUND 3 - divorced or single, 1 to 5 mil (above avg)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT C.CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS C 
        JOIN VW_CUSTOMER_TOTAL_ASSETS TA ON C.CUSTOMER_ID = TA.CUSTOMER_ID
    WHERE TA.TOTAL_ASSETS>1000000 AND TA.TOTAL_ASSETS<5000000 AND (C.MARITAL_STATUS='Single' OR C.MARITAL_STATUS='Divorced');
    calculate_fund_min_max_revenue(V_CUSTOMER_IDS, 3);
END;
/

-- FUND 4 - customers that are between 30 and 55 and have 2 or more dependents (avg and above avg)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS
    WHERE AGE>=30 AND AGE<=55 AND NUMBER_OF_DEPENDENTS>=2;
    calculate_fund_min_max_revenue(V_CUSTOMER_IDS, 4);
END;
/

-- FUND 5 - customers from north east region (avg and above avg)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS
    WHERE STATE IN ('Massachusetts', 'Pennsylvania', 'Connecticut', 'Delaware', 'Maryland', 'Maine', 'Vermont');
    calculate_fund_min_max_revenue(V_CUSTOMER_IDS, 5);
END;
/

COMMIT;