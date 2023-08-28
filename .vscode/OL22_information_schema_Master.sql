with schema_info as(
  SELECT *
  FROM `one-global-dde-prod.region-asia-southeast1`.INFORMATION_SCHEMA.TABLES
),
table_storage as (
  select
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

  from `one-global-dde-prod.region-asia-southeast1`.INFORMATION_SCHEMA.TABLE_STORAGE
),

is_partition_table as (
select 
  distinct table_schema,
  table_name 
  from
`one-global-dde-prod.region-asia-southeast1`.INFORMATION_SCHEMA.COLUMNS
where is_partitioning_column = 'YES'
),
is_clustered_table as (
  select 
    distinct table_schema,
    table_name 
    from
  `one-global-dde-prod.region-asia-southeast1`.INFORMATION_SCHEMA.COLUMNS
  where clustering_ordinal_position is not NULL 
)

select 
  schema_info.*,
  table_storage.storage_last_modified_time,
  table_storage.total_rows,
  table_storage.total_partitions,
  table_storage.total_logical_gb,
  table_storage.active_logical_gb,
  table_storage.long_term_logical_gb,
  table_storage.total_physical_gb,
  table_storage.active_physical_gb,
  table_storage.long_term_physical_gb,
  table_storage.time_travel_physical_gb,
  is_partition_table.table_name is not null as is_partitioned_table, 
  is_clustered_table.table_name is not null as is_clustered_table

from schema_info
left join table_storage
  on schema_info.table_schema = table_storage.table_schema
  and schema_info.table_name = table_storage.table_name
left join is_partition_table
  on schema_info.table_schema = is_partition_table.table_schema
  and schema_info.table_name = is_partition_table.table_name
left join is_clustered_table
  on schema_info.table_schema = is_clustered_table.table_schema
  and schema_info.table_name = is_clustered_table.table_name

 
union all
select * from
  (
with schema_info as(
  SELECT *
  FROM `one-global-dde-insights-prod.region-asia-southeast1`.INFORMATION_SCHEMA.TABLES
),
table_storage as (
  select
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

  from `one-global-dde-insights-prod.region-asia-southeast1`.INFORMATION_SCHEMA.TABLE_STORAGE
),

is_partition_table as (
select 
  distinct table_schema,
  table_name 
  from
`one-global-dde-insights-prod.region-asia-southeast1`.INFORMATION_SCHEMA.COLUMNS
where is_partitioning_column = 'YES'
),
is_clustered_table as (
  select 
    distinct table_schema,
    table_name 
    from
  `one-global-dde-insights-prod.region-asia-southeast1`.INFORMATION_SCHEMA.COLUMNS
  where clustering_ordinal_position is not NULL 
)
select 
  schema_info.*,
  table_storage.storage_last_modified_time,
  table_storage.total_rows,
  table_storage.total_partitions,
  table_storage.total_logical_gb,
  table_storage.active_logical_gb,
  table_storage.long_term_logical_gb,
  table_storage.total_physical_gb,
  table_storage.active_physical_gb,
  table_storage.long_term_physical_gb,
  table_storage.time_travel_physical_gb,
  is_partition_table.table_name is not null as is_partitioned_table, 
  is_clustered_table.table_name is not null as is_clustered_table

from schema_info
left join table_storage
  on schema_info.table_schema = table_storage.table_schema
  and schema_info.table_name = table_storage.table_name
left join is_partition_table
  on schema_info.table_schema = is_partition_table.table_schema
  and schema_info.table_name = is_partition_table.table_name
left join is_clustered_table
  on schema_info.table_schema = is_clustered_table.table_schema
  and schema_info.table_name = is_clustered_table.table_name
 
)
