query_for_raw_view = ("""
    with groups_unnested as (
SELECT group_id,org_id,group_type, group_status, clusters_unnested, date FROM
(
 SELECT CAST(group_id.oid as varchar) as group_id,
CAST(group_org_id.oid as varchar) as org_id,
group_type,
clusters,
CAST(group_status.status as varchar) as group_status,
processed_date as date
from remodel_cloud.dw__cloud_backend__cloud_groups_full_history
where processed_date >= date '2019-06-01' and group_internal_flag=false and CAST(group_status.status as varchar)='ACTIVE'
) groups
CROSS JOIN UNNEST(clusters) as t(clusters_unnested)
),
groups_prepared as(
SELECT
group_id,org_id,group_type, group_status,
CAST(clusters_unnested.mdb_major_version as varchar) as mdb_major_version,
CAST(clusters_unnested.cluster_details.instance_size as varchar) as instance_size,
CAST(clusters_unnested.id.oid as varchar) as cluster_id,
CAST(clusters_unnested.is_multi_tenant_mongo as boolean) as is_multitenant_mongo,
CAST(clusters_unnested.state as varchar) as cluster_state,
CAST(clusters_unnested.cloudProvider as varchar) as cloud_provider,
week(date) as week,
month(date) as month,
year(date) as year,
date
from groups_unnested
WHERE   CAST(clusters_unnested.state as varchar)!='DELETED'
),
cluster_actives as (
SELECT properties__tier as tier,
timestamp_transformed as date,
properties__cluster_id as cluster_id,
year(timestamp_transformed) as year,
month(timestamp_transformed) as month,
week(timestamp_transformed) as week,
properties__db_version as db_version,
properties__instance_size as ca_instance_size
FROM segment.cloud__segment__production_backend_full_history
where event='Cluster Active' and timestamp_transformed > date '2019-06-01'
)

SELECT
DISTINCT *
FROM (SELECT
cluster_actives.cluster_id as cluster_id,
cluster_actives.year as year,
cluster_actives.month as month,
cluster_actives.week as week,
cluster_actives.tier as tier,
cluster_actives.date as date,
cluster_actives.ca_instance_size as ca_instance_size,
cluster_actives.db_version as mdb_version,
group_id,org_id,group_type,group_status,mdb_major_version,instance_size,
is_multitenant_mongo,cluster_state,cloud_provider


from groups_prepared JOIN cluster_actives
ON groups_prepared.date=cluster_actives.date AND groups_prepared.cluster_id=cluster_actives.cluster_id
)

""")


    query = ("""
    with groups_unnested as (
   SELECT group_id,org_id,group_type, group_status, clusters_unnested, date FROM
   (
     SELECT CAST(group_id.oid as varchar) as group_id,
   CAST(group_org_id.oid as varchar) as org_id,
   group_type,
   clusters,
   CAST(group_status.status as varchar) as group_status,
   processed_date as date
  from remodel_cloud.dw__cloud_backend__cloud_groups_full_history
   where processed_date = date '2019-05-01' and group_internal_flag=false and CAST(group_status.status as varchar)='ACTIVE'
  ) groups
   CROSS JOIN UNNEST(clusters) as t(clusters_unnested)
  ),
  groups_prepared as(
  SELECT
  group_id,org_id,group_type, group_status,
  CAST(clusters_unnested.mdb_major_version as varchar) as mdb_major_version,
  CAST(clusters_unnested.cluster_details.instance_size as varchar) as instance_size,
  CAST(clusters_unnested.id.oid as varchar) as cluster_id,
  CAST(clusters_unnested.is_multi_tenant_mongo as boolean) as is_multitenant_mongo,
  CAST(clusters_unnested.state as varchar) as cluster_state,
  CAST(clusters_unnested.cloudProvider as varchar) as cloud_provider,
  week(date) as week,
  month(date) as month,
  year(date) as year,
  date
  from groups_unnested
    WHERE   CAST(clusters_unnested.state as varchar)!='DELETED'
   ),
   cluster_actives as (
  SELECT properties__tier as tier,
  timestamp_transformed as date,
  properties__cluster_id as cluster_id,
  year(timestamp_transformed) as year,
  month(timestamp_transformed) as month,
  week(timestamp_transformed) as week,
  properties__db_version as db_version,
  properties__instance_size as ca_instance_size
  FROM segment.cloud__segment__production_backend_full_history
  where event='Cluster Active' and timestamp_transformed = date '2019-05-01'
  )

  SELECT * FROM (SELECT
  cluster_actives.cluster_id as cluster_id,
  cluster_actives.year as year,
  cluster_actives.month as month,
  cluster_actives.week as week,
  cluster_actives.tier as tier,
  cluster_actives.date as date,
  cluster_actives.ca_instance_size as ca_instance_size,
  cluster_actives.db_version as mdb_version,
  group_id,org_id,group_type,group_status,mdb_major_version,instance_size,
  is_multitenant_mongo,cluster_state,cloud_provider


from groups_prepared JOIN cluster_actives
ON groups_prepared.date=cluster_actives.date AND groups_prepared.cluster_id=cluster_actives.cluster_id
)
GROUP BY cluster_id, year, month, week, tier,date, ca_instance_size,mdb_version,group_id,org_id,group_type,group_status,mdb_major_version,instance_size,
  is_multitenant_mongo,cluster_state,cloud_provider
    """)
