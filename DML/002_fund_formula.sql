-- new formula

CREATE OR REPLACE TYPE CUSTOMER_ID_ARRAY AS TABLE OF NUMBER;
/

CREATE OR REPLACE PROCEDURE GENERATE_FUND_COMP (
    P_CUSTOMER_IDS IN CUSTOMER_ID_ARRAY,
    P_FUND_ID IN NUMBER
) AS
    TYPE RISK_PROFILE_REC IS RECORD (
        RISK_PROFILE_ID NUMBER,
        CUSTOMER_COUNT  NUMBER
    );
    TYPE RISK_PROFILE_TAB IS TABLE OF RISK_PROFILE_REC;

    TYPE ASSET_CLASS_REC IS RECORD (
        ASSET_CLASS_ID NUMBER,
        RISK_PROFILE_ID NUMBER,
        AVG_RETURN_10Y NUMBER,
        STD_DEV_10Y NUMBER
    );
    TYPE ASSET_CLASS_TAB IS TABLE OF ASSET_CLASS_REC;

    TYPE ALLOCATION_REC IS RECORD (
        ASSET_CLASS_ID NUMBER,
        ALLOC_PCT NUMBER
    );
    TYPE ALLOCATION_TAB IS TABLE OF ALLOCATION_REC;

    RISK_PROFILES RISK_PROFILE_TAB;
    ASSET_CLASSES ASSET_CLASS_TAB;
    ALLOCATIONS ALLOCATION_TAB;

    V_TOTAL_CUSTOMERS NUMBER := 0;
    V_MIN_ALLOC_PCT NUMBER := 5; -- Minimum allocation threshold
    V_TOTAL_ALLOC_PCT NUMBER := 0;
BEGIN
    -- 1. Get customer counts per risk profile
    SELECT ASSIGNED_RISK_PROFILE_ID, COUNT(*)
    BULK COLLECT INTO RISK_PROFILES
    FROM VW_CUSTOMER_WEIGHTED_RISK_ASSIGNED
    WHERE CUSTOMER_ID MEMBER OF P_CUSTOMER_IDS
    GROUP BY ASSIGNED_RISK_PROFILE_ID;

    -- calculates number of customers
    FOR I IN 1 .. RISK_PROFILES.COUNT LOOP
        V_TOTAL_CUSTOMERS := V_TOTAL_CUSTOMERS + RISK_PROFILES(I).CUSTOMER_COUNT;
    END LOOP;

    -- 2. Get asset classes and their performance data from FF_ASSET_CLASSES
    SELECT ASSET_CLASS_ID, RISK_PROFILE_ID, AVG_RETURN_10Y, STD_DEV_10Y
    BULK COLLECT INTO ASSET_CLASSES
    FROM FF_ASSET_CLASSES;

    -- 3. Calculate allocations
    ALLOCATIONS := ALLOCATION_TAB();
    FOR I IN 1 .. RISK_PROFILES.COUNT LOOP
        DECLARE
            V_RISK_PROFILE_ID NUMBER := RISK_PROFILES(I).RISK_PROFILE_ID;
            V_RISK_COUNT NUMBER := RISK_PROFILES(I).CUSTOMER_COUNT;
            V_PROFILE_PCT NUMBER := 0;
            V_TOTAL_SCORE NUMBER := 0;
            TYPE SCORE_REC IS RECORD (
                ASSET_CLASS_ID NUMBER,
                SCORE NUMBER
            );
            TYPE SCORE_TAB IS TABLE OF SCORE_REC;
            SCORES SCORE_TAB := SCORE_TAB();
        BEGIN
            IF V_TOTAL_CUSTOMERS > 0 THEN
                V_PROFILE_PCT := V_RISK_COUNT * 100 / V_TOTAL_CUSTOMERS;
            END IF;

            -- Get asset classes for this risk profile and calculate scores
            FOR J IN 1 .. ASSET_CLASSES.COUNT LOOP
                IF ASSET_CLASSES(J).RISK_PROFILE_ID = V_RISK_PROFILE_ID THEN
                    -- Risk-adjusted score: avg_return_10y / std_dev_10y (avoid div by zero)
                    DECLARE
                        V_SCORE NUMBER := 0;
                    BEGIN
                        IF ASSET_CLASSES(J).STD_DEV_10Y > 0 THEN
                            V_SCORE := ASSET_CLASSES(J).AVG_RETURN_10Y / ASSET_CLASSES(J).STD_DEV_10Y;
                        ELSE
                            V_SCORE := 0;
                        END IF;
                        SCORES.EXTEND;
                        SCORES(SCORES.COUNT).ASSET_CLASS_ID := ASSET_CLASSES(J).ASSET_CLASS_ID;
                        SCORES(SCORES.COUNT).SCORE := V_SCORE;
                        V_TOTAL_SCORE := V_TOTAL_SCORE + V_SCORE;
                    END;
                END IF;
            END LOOP;

            -- Allocate percentages based on scores
            FOR K IN 1 .. SCORES.COUNT LOOP
                DECLARE
                    V_ALLOC_PCT NUMBER := 0;
                BEGIN
                    IF V_TOTAL_SCORE > 0 THEN
                        V_ALLOC_PCT := V_PROFILE_PCT * (SCORES(K).SCORE / V_TOTAL_SCORE);
                    ELSE
                        V_ALLOC_PCT := V_PROFILE_PCT / SCORES.COUNT;
                    END IF;
                    -- Store allocation
                    ALLOCATIONS.EXTEND;
                    ALLOCATIONS(ALLOCATIONS.COUNT).ASSET_CLASS_ID := SCORES(K).ASSET_CLASS_ID;
                    ALLOCATIONS(ALLOCATIONS.COUNT).ALLOC_PCT := V_ALLOC_PCT;
                END;
            END LOOP;
        END;
    END LOOP;

    -- 4. Remove allocations below minimum threshold and redistribute
    DECLARE
        V_EXCESS_PCT NUMBER := 0;
        V_TOTAL_VALID_PCT NUMBER := 0;
    BEGIN
        -- Calculate excess and valid total
        FOR I IN 1 .. ALLOCATIONS.COUNT LOOP
            IF ALLOCATIONS(I).ALLOC_PCT < V_MIN_ALLOC_PCT THEN
                V_EXCESS_PCT := V_EXCESS_PCT + ALLOCATIONS(I).ALLOC_PCT;
                ALLOCATIONS(I).ALLOC_PCT := 0;
            ELSE
                V_TOTAL_VALID_PCT := V_TOTAL_VALID_PCT + ALLOCATIONS(I).ALLOC_PCT;
            END IF;
        END LOOP;
        -- Redistribute excess proportionally
        IF V_TOTAL_VALID_PCT > 0 THEN
            FOR I IN 1 .. ALLOCATIONS.COUNT LOOP
                IF ALLOCATIONS(I).ALLOC_PCT >= V_MIN_ALLOC_PCT THEN
                    ALLOCATIONS(I).ALLOC_PCT := ALLOCATIONS(I).ALLOC_PCT + (ALLOCATIONS(I).ALLOC_PCT / V_TOTAL_VALID_PCT) * V_EXCESS_PCT;
                END IF;
            END LOOP;
        END IF;
    END;

    -- 5. Precision adjustment: ensure total is exactly 100%
    V_TOTAL_ALLOC_PCT := 0;
    FOR I IN 1 .. ALLOCATIONS.COUNT LOOP
        V_TOTAL_ALLOC_PCT := V_TOTAL_ALLOC_PCT + ALLOCATIONS(I).ALLOC_PCT;
    END LOOP;
    IF V_TOTAL_ALLOC_PCT <> 100 THEN
        -- Adjust the largest allocation
        DECLARE
            V_MAX_IDX NUMBER := 1;
        BEGIN
            FOR I IN 2 .. ALLOCATIONS.COUNT LOOP
                IF ALLOCATIONS(I).ALLOC_PCT > ALLOCATIONS(V_MAX_IDX).ALLOC_PCT THEN
                    V_MAX_IDX := I;
                END IF;
            END LOOP;
            ALLOCATIONS(V_MAX_IDX).ALLOC_PCT := ALLOCATIONS(V_MAX_IDX).ALLOC_PCT + (100 - V_TOTAL_ALLOC_PCT);
        END;
    END IF;

    -- 6. Update FF_FUND_ASSETS table
    FOR I IN 1 .. ALLOCATIONS.COUNT LOOP
        UPDATE FF_FUND_ASSETS
        SET PERCENT_OF_FUND = ALLOCATIONS(I).ALLOC_PCT
        WHERE FUND_ID = P_FUND_ID AND ASSET_CLASS_ID = ALLOCATIONS(I).ASSET_CLASS_ID;
    END LOOP;
END;
/

-- anonymous block to run the procedure for different customer segments

-- FUND 1 - above 60, more than 5 mil (mostly avg and little below avg)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT C.CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS C 
        JOIN VW_CUSTOMER_TOTAL_ASSETS TA ON C.CUSTOMER_ID = TA.CUSTOMER_ID
    WHERE C.AGE>60 AND TA.TOTAL_ASSETS>5000000;
    GENERATE_FUND_COMP(V_CUSTOMER_IDS, 1);
END;
/

-- FUND 2 - married women (mostly avg, little below avg)(they have high assets in general)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS
    WHERE MARITAL_STATUS='Married' AND GENDER='Female';
    GENERATE_FUND_COMP(V_CUSTOMER_IDS, 2);
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
    GENERATE_FUND_COMP(V_CUSTOMER_IDS, 3);
END;
/

-- FUND 4 - customers that are between 30 and 55 and have 2 or more dependents (avg and above avg)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS
    WHERE AGE>=30 AND AGE<=55 AND NUMBER_OF_DEPENDENTS>=2;
    GENERATE_FUND_COMP(V_CUSTOMER_IDS, 4);
END;
/

-- FUND 5 - customers from north east region (avg and above avg)
DECLARE 
    V_CUSTOMER_IDS CUSTOMER_ID_ARRAY;
BEGIN
    SELECT CUSTOMER_ID BULK COLLECT INTO V_CUSTOMER_IDS 
    FROM FF_CUSTOMERS
    WHERE STATE IN ('Massachusetts', 'Pennsylvania', 'Connecticut', 'Delaware', 'Maryland', 'Maine', 'Vermont');
    GENERATE_FUND_COMP(V_CUSTOMER_IDS, 5);
END;
/

COMMIT;

SELECT * FROM FF_FUND_ASSETS;


--CREATE OR REPLACE TYPE CUSTOMER_ID_ARRAY AS TABLE OF NUMBER;
--/

-- factors that may affect fund - 
-- returns of each asset class

--STORE PROCEDURE TO GENERATE FUND
--CREATE OR REPLACE PROCEDURE GENERATE_FUND_COMP (
--    P_CUSTOMER_IDS IN CUSTOMER_ID_ARRAY,
--    P_FUND_ID IN NUMBER
--) AS
--    TYPE RISK_PROFILE_REC IS RECORD (
--        RISK_PROFILE_ID NUMBER,
--        CUSTOMER_COUNT  NUMBER
--    );
--    TYPE RISK_PROFILE_TAB IS TABLE OF RISK_PROFILE_REC;
--    
--    RISK_PROFILES RISK_PROFILE_TAB;
--    V_TOTAL_CUSTOMERS NUMBER := 0;
--    
--    CURSOR C_ASSET_CLASSES IS
--        SELECT ASSET_CLASS_ID, RISK_PROFILE_ID
--        FROM FF_ASSET_CLASSES;
--BEGIN
--    -- STORING COUNT OF CUSTOMERS PER RISK PROFILE ID
--    SELECT ASSIGNED_RISK_PROFILE_ID, COUNT(*)
--    BULK COLLECT INTO RISK_PROFILES
--    FROM VW_CUSTOMER_WEIGHTED_RISK_ASSIGNED
--    WHERE CUSTOMER_ID MEMBER OF P_CUSTOMER_IDS
--    GROUP BY ASSIGNED_RISK_PROFILE_ID;
--    
--    FOR I IN 1 .. RISK_PROFILES.COUNT LOOP
--        V_TOTAL_CUSTOMERS := V_TOTAL_CUSTOMERS + RISK_PROFILES(I).CUSTOMER_COUNT;
--    END LOOP;
--    
--    FOR AC IN C_ASSET_CLASSES LOOP
--        DECLARE
--            V_PERCENT NUMBER := 0;
--            V_ASSET_CLASS_COUNT NUMBER := 0;
--            V_SPLIT_PERCENT NUMBER := 0;
--            V_RISK_COUNT NUMBER := 0;
--        BEGIN
--            -- Find customer count for this risk profile
--            FOR J IN 1 .. RISK_PROFILES.COUNT LOOP
--                IF RISK_PROFILES(J).RISK_PROFILE_ID = AC.RISK_PROFILE_ID THEN
--                    V_RISK_COUNT := RISK_PROFILES(J).CUSTOMER_COUNT;
--                    EXIT;
--                END IF;
--            END LOOP;
--
--            IF V_TOTAL_CUSTOMERS > 0 THEN
--                V_PERCENT := V_RISK_COUNT * 100 / V_TOTAL_CUSTOMERS;
--
--                -- Count asset classes for this risk profile
--                SELECT COUNT(*) INTO V_ASSET_CLASS_COUNT
--                FROM FF_ASSET_CLASSES
--                WHERE RISK_PROFILE_ID = AC.RISK_PROFILE_ID;
--
--                IF V_ASSET_CLASS_COUNT > 0 THEN
--                    V_SPLIT_PERCENT := V_PERCENT / V_ASSET_CLASS_COUNT;
--                ELSE
--                    V_SPLIT_PERCENT := 0;
--                END IF;
--            END IF;
--            -- Update each asset class with its share
--            UPDATE FF_FUND_ASSETS
--            SET PERCENT_OF_FUND = V_SPLIT_PERCENT
--            WHERE FUND_ID = P_FUND_ID AND ASSET_CLASS_ID = AC.ASSET_CLASS_ID;
--        END;
--    END LOOP;
--    
--END;
--/
