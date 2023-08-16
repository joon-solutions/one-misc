with 
table_storage_tmp as (
    select * from `one-global-dde-prod.region-asia-southeast1`.INFORMATION_SCHEMA.TABLE_STORAGE
),

table_storage as (
    select
        table_catalog,
        table_schema,
        table_name,
        creation_time,
        storage_last_modified_time,
        total_rows,
        total_partitions,
        total_logical_bytes/pow(1024,3) total_logical_gb,
        active_logical_bytes/pow(1024,3) active_logical_gb,
        long_term_logical_bytes/pow(1024,3) long_term_logical_gb,
        total_physical_bytes/pow(1024,3) total_physical_gb,
        active_physical_bytes/pow(1024,3) active_physical_gb,
        long_term_physical_bytes/pow(1024,3) long_term_physical_gb,
        time_travel_physical_bytes/pow(1024,3) time_travel_physical_gb
    from table_storage_tmp
)

select * from table_storage
where  table_schema in ('DWH', 'OPUS')
and lower(table_name) in 
    (
    'dmc_period',
    'dwc_cntr_sz',
    'dwc_cntr_tp',
    'dwc_cntr_tp_sz',
    'dwc_com_cd_dtl',
    'dwc_location',
    'dwc_organization',
    'dwc_vendor vndr',
    'dwc_zone',
    'dwl_chss_pool',
    'dwl_lse_term',
    'dwc_organization',
    'dwc_cntr_vndr_clss',
    'dwc_com_cd_dtl',
    'dwc_continent',
    'dwc_country',
    'dwc_eq_orz_cht',
    'dwc_location',
    'dwc_organization',
    'dwc_organization',
    'dwc_region',
    'dwc_state',
    'dwc_subcontinent',
    'dwc_yard',
    'dml_cntr_ltst_mvmt_snap'
    )