-- =============================================================================
-- Seller x City Performance — Daily metrics (Pickup, OFD, FAC, Breach, Conv, ZRTO)
-- Grouped by: reporting_date, destination_city, seller_type, payment_type
-- =============================================================================
-- Tokens replaced at runtime by scraper.py (see get_query()):
--   {end_date}   → yesterday (YYYYMMDD)
--   {start_date} → 30 days before yesterday (YYYYMMDD)
--
-- destination_city is resolved by joining the shipment's destination_pincode_key
-- against the logistics_geo_hive_dim, with GURGAON/PATAUDI/GHAZIABAD/NOIDA/
-- GAUTAM BUDDHA NAGAR collapsed into a single 'NCR' bucket.
-- =============================================================================

WITH geo AS (
    SELECT
        logistics_geo_hive_dim_key,
        pincode,
        city,
        CASE WHEN city IN ('GURGAON','PATAUDI','GHAZIABAD','NOIDA','GAUTAM BUDDHA NAGAR')
            THEN 'NCR' ELSE city_combined END AS city_combined
    FROM bigfoot_external_neo.scp_ekl__logistics_geo_hive_dim
),

vti_geo AS (
    SELECT DISTINCT
        ext.vendor_tracking_id,
        geo.city_combined AS destination_city
    FROM bigfoot_external_neo.scp_fsgde_ns__externalisation_l1_scala_fact ext
    LEFT JOIN geo ON ext.destination_pincode_key = geo.logistics_geo_hive_dim_key
),

conv AS (
    SELECT
        shipment_received_at_origin_date_key,
        seller_type,
        geo.city_combined AS destination_city,
        CASE WHEN LOWER(payment_type) = 'prepaid' THEN 'prepaid' ELSE 'cod' END AS payment_type,
        COUNT(DISTINCT vendor_tracking_id) AS conv_deno,
        COUNT(DISTINCT CASE
            WHEN LOWER(ekl_shipment_type) = 'forward'
                 AND LOWER(shipment_current_status) IN ('delivered', 'delivery_update')
            THEN vendor_tracking_id
        END) AS conv_num,
        COUNT(DISTINCT CASE
            WHEN fsd_first_ofd_date_key IS NULL
                 AND first_undelivery_status IS NULL
                 AND last_undelivery_status IS NULL
                 AND shipped_lpd_date_key < rto_create_date_key
            THEN vendor_tracking_id
        END) AS EKL_RTO_num
    FROM bigfoot_external_neo.scp_fsgde_ns__externalisation_l1_scala_fact ext
    LEFT JOIN geo ON ext.destination_pincode_key = geo.logistics_geo_hive_dim_key
    WHERE seller_type NOT IN ('Non-FA','FA','WSR','MYN','MYS','FKW','FKH','MYE','MP_FBF_SELLER','MP_NON_FBF_SELLER','SF2','SF4')
    GROUP BY 1, 2, 3, 4
),

pickup AS (
    SELECT
        shipment_created_at_date_key,
        seller_type,
        geo.city_combined AS destination_city,
        CASE WHEN LOWER(payment_type) = 'prepaid' THEN 'prepaid' ELSE 'cod' END AS payment_type,
        COUNT(DISTINCT vendor_tracking_id) AS fm_created,
        COUNT(DISTINCT CASE
            WHEN shipment_received_at_origin_date_key IS NOT NULL THEN vendor_tracking_id
        END) AS fm_picked,
        COUNT(DISTINCT CASE
            WHEN shipment_received_at_origin_date_key = shipment_created_at_date_key THEN vendor_tracking_id
        END) AS fm_d0_picked
    FROM bigfoot_external_neo.scp_fsgde_ns__externalisation_l1_scala_fact ext
    LEFT JOIN geo ON ext.destination_pincode_key = geo.logistics_geo_hive_dim_key
    WHERE seller_type NOT IN ('Non-FA','FA','WSR','MYN','MYS','FKW','FKH','MYE','MP_FBF_SELLER','MP_NON_FBF_SELLER','SF2','SF4')
        AND LOWER(ekl_shipment_type) NOT IN ('rvp')
    GROUP BY 1, 2, 3, 4
),

fac AS (
    SELECT
        tasklist_created_date_key,
        seller_type,
        vti_geo.destination_city,
        CASE WHEN LOWER(payment_type) = 'prepaid' THEN 'prepaid' ELSE 'cod' END AS payment_type,
        COUNT(DISTINCT CASE
            WHEN LOWER(tasklist_type) = 'runsheet'
                 AND LOWER(attempt_type) = 'customer'
                 AND shipment_actioned_flag = 1
                 AND attempt_no = 1
            THEN CONCAT(task.vendor_tracking_id, CAST(tasklist_id AS STRING))
        END) AS First_attempt_delivered,
        COUNT(DISTINCT CASE
            WHEN LOWER(tasklist_type) = 'runsheet'
                 AND LOWER(attempt_type) = 'customer'
                 AND attempt_no = 1
            THEN CONCAT(task.vendor_tracking_id, CAST(tasklist_id AS STRING))
        END) AS fac_deno,
        COUNT(DISTINCT CASE
            WHEN LOWER(tasklist_type) = 'runsheet'
                 AND LOWER(attempt_type) = 'customer'
            THEN CONCAT(task.vendor_tracking_id, CAST(tasklist_id AS STRING))
        END) AS total_attempts,
        COUNT(DISTINCT CASE
            WHEN LOWER(tasklist_type) = 'runsheet'
                 AND LOWER(attempt_type) = 'customer'
                 AND shipment_actioned_flag = 1
            THEN CONCAT(task.vendor_tracking_id, CAST(tasklist_id AS STRING))
        END) AS total_delivered_attempts,
        COUNT(DISTINCT CASE
            WHEN attempt_Type = 'Customer'
                 AND undel_unpick_status = 'Undelivered_Attempted-Request for reschedule'
            THEN CONCAT(task.vendor_tracking_id, CAST(tasklist_id AS STRING))
        END) AS rfr_num,
        COUNT(DISTINCT CASE
            WHEN attempt_Type = 'Customer'
            THEN CONCAT(task.vendor_tracking_id, CAST(tasklist_id AS STRING))
        END) AS rfr_deno
    FROM bigfoot_external_neo.scp_fsgde_ns__lastmile_tasklist_base_fact task
    LEFT JOIN vti_geo ON task.vendor_tracking_id = vti_geo.vendor_tracking_id
    WHERE seller_type NOT IN ('Non-FA','FA','WSR','MYN','MYS','FKW','FKH','MYE','MP_FBF_SELLER','MP_NON_FBF_SELLER','SF2','SF4','SF6','SF7')
        AND LOWER(facility_type) NOT IN ('large')
    GROUP BY 1, 2, 3, 4
),

ofd AS (
    SELECT
        fsd_first_dh_received_date_key,
        seller_type,
        geo.city_combined AS destination_city,
        CASE WHEN LOWER(payment_type) = 'prepaid' THEN 'prepaid' ELSE 'cod' END AS payment_type,
        COUNT(DISTINCT CASE
            WHEN fsd_first_dh_received_date_key IS NOT NULL THEN ext.vendor_tracking_id
        END) AS DHin,
        COUNT(DISTINCT CASE
            WHEN fsd_first_dh_received_date_key = fsd_first_ofd_date_key THEN ext.vendor_tracking_id
        END) AS D0_OFD
    FROM bigfoot_external_neo.scp_fsgde_ns__externalisation_l1_scala_fact ext
    LEFT JOIN geo ON ext.destination_pincode_key = geo.logistics_geo_hive_dim_key
    WHERE seller_type NOT IN ('Non-FA','FA','WSR','MYN','MYS','FKW','FKH','MYE','MP_FBF_SELLER','MP_NON_FBF_SELLER','SF2','SF4')
    GROUP BY 1, 2, 3, 4
),

breach AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', DATE(ext.shipped_lpd)) AS INT64) AS shipped_lpd_date_key,
        seller_type,
        geo.city_combined AS destination_city,
        CASE WHEN LOWER(payment_type) = 'prepaid' THEN 'prepaid' ELSE 'cod' END AS payment_type,
        COUNT(DISTINCT CASE
            WHEN ext_breach_bucket NOT IN ('01 Future LPD','02 Delivered by promise','03 Genuine OFD by promise','05 RTO by promise')
            THEN vendor_tracking_id
        END) AS Breach_Num,
        COUNT(DISTINCT CASE
            WHEN ext_breach_bucket NOT IN ('01 Future LPD','02 Delivered by promise','03 Genuine OFD by promise','05 RTO by promise')
                 AND DATE_DIFF(
                     COALESCE(
                         PARSE_DATE('%Y%m%d', CAST(fsd_first_ofd_date_key AS STRING)),
                         PARSE_DATE('%Y%m%d', CAST(rto_create_date_key AS STRING)),
                         CURRENT_DATE()
                     ),
                     DATE(shipped_lpd),
                     DAY
                 ) > 1
            THEN vendor_tracking_id
        END) AS breach_plus1_num,
        COUNT(vendor_tracking_id) AS Breach_Den
    FROM bigfoot_external_neo.scp_fsgde_ns__externalisation_l1_scala_fact ext
    LEFT JOIN geo ON ext.destination_pincode_key = geo.logistics_geo_hive_dim_key
    WHERE seller_type NOT IN ('Non-FA','FA','WSR','MYN','MYS','FKW','FKH','MYE','MP_FBF_SELLER','MP_NON_FBF_SELLER','SF2','SF4')
    GROUP BY 1, 2, 3, 4
)

SELECT DISTINCT
    conv.shipment_received_at_origin_date_key AS reporting_date,
    conv.destination_city,
    conv.seller_type,
    conv.payment_type,
    conv_deno AS PHin,
    conv_num,
    EKL_RTO_num AS zero_attempt_num,
    fm_created,
    fm_picked,
    fm_d0_picked,
    DHin,
    D0_OFD,
    First_attempt_delivered,
    fac_deno,
    total_delivered_attempts,
    total_attempts,
    rfr_num,
    rfr_deno,
    Breach_Num,
    Breach_Den,
    breach_plus1_num
FROM conv
LEFT JOIN pickup
    ON pickup.shipment_created_at_date_key = conv.shipment_received_at_origin_date_key
    AND conv.seller_type = pickup.seller_type
    AND conv.payment_type = pickup.payment_type
    AND conv.destination_city = pickup.destination_city
LEFT JOIN fac
    ON conv.shipment_received_at_origin_date_key = fac.tasklist_created_date_key
    AND conv.seller_type = fac.seller_type
    AND conv.payment_type = fac.payment_type
    AND conv.destination_city = fac.destination_city
LEFT JOIN ofd
    ON conv.shipment_received_at_origin_date_key = ofd.fsd_first_dh_received_date_key
    AND conv.seller_type = ofd.seller_type
    AND conv.payment_type = ofd.payment_type
    AND conv.destination_city = ofd.destination_city
LEFT JOIN breach
    ON conv.shipment_received_at_origin_date_key = breach.shipped_lpd_date_key
    AND conv.seller_type = breach.seller_type
    AND conv.payment_type = breach.payment_type
    AND conv.destination_city = breach.destination_city
WHERE conv.shipment_received_at_origin_date_key BETWEEN {start_date} AND {end_date};
