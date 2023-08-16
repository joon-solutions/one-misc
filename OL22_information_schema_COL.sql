select
  table_catalog || "." || table_schema || "." || table_name as id 
, table_catalog
, table_schema
, table_name
, column_name
, clustering_ordinal_position
, is_partitioning_column
, data_type

from `one-global-dde-prod.region-asia-southeast1`.INFORMATION_SCHEMA.COLUMNS
where table_schema in ('DWH', 'OPUS')
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