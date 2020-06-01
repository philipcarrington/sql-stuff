--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Create a table to give all the tables an ID:
drop table permissions_meta.table_ids;

create table permissions_meta.table_ids (
    table_no                    int,
    table_schema                varchar(255),
    table_name                  varchar(255)    
);

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Populate the table ID's table:
SET @row_number:=0;

insert into permissions_meta.table_ids
select @row_number:=@row_number+1 as table_no,
       rel_table_base.table_schema,                           
       rel_table_base.table_name
from (                    
        select kcu.table_schema,
               kcu.table_name                                                       
        from information_schema.key_column_usage kcu
        where kcu.constraint_schema = 'permissions_meta'
            and ((kcu.constraint_name like 'fk_%') or (kcu.constraint_name like 'REL%'))
        union
        select kcu.referenced_table_schema,
               kcu.referenced_table_name
        from information_schema.key_column_usage kcu
        where kcu.constraint_schema = 'permissions_meta'
            and ((kcu.constraint_name like 'fk_%') or (kcu.constraint_name like 'REL%')) 
      ) as rel_table_base;
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Check the content of the table ID's table:
select *
from permissions_meta.table_ids;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Create a table for the meta data for the tables
drop table permissions_meta.table_meta;

create table permissions_meta.table_meta (
      table_schema          varchar(255),
      table_name            varchar(255),
      table_acronym         varchar(10),
      table_aliased_columns varchar(1000)
);

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Populate the meta data for the table:

set session group_concat_max_len = 100000;

insert into permissions_meta.table_meta
select get_column_tables.table_schema,
       get_column_tables.table_name,
       get_column_tables.table_acronym,
       group_concat(
                    concat('\t', get_column_tables.table_acronym, '.', isc.column_name, ' as ', get_column_tables.table_acronym, '_', isc.column_name)
                    separator ', \n '
        )as table_aliased_columns
from
    (
        select base.table_schema,
               base.table_name,
               acronym(base.table_name) as table_acronym
        from
            (
                select kcu.table_schema,
                       kcu.table_name                                                       
                from information_schema.key_column_usage kcu
                where kcu.constraint_schema = 'permissions_meta'
                    and ((kcu.constraint_name like 'fk_%') or (kcu.constraint_name like 'REL%'))
                union
                select kcu.referenced_table_schema,
                       kcu.referenced_table_name
                from information_schema.key_column_usage kcu
                where kcu.constraint_schema = 'permissions_meta'
                    and ((kcu.constraint_name like 'fk_%') or (kcu.constraint_name like 'REL%'))                
            ) as base
    ) as get_column_tables
    left join information_schema.columns isc
        on get_column_tables.table_schema = isc.table_schema
            and get_column_tables.table_name = isc.table_name
    group by get_column_tables.table_schema,
             get_column_tables.table_name,
             get_column_tables.table_acronym;
             
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- See what is in the table:
select *
from permissions_meta.table_meta;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Create a table to hold all the data:
drop table permissions_meta.denormalized_view_data;

create table permissions_meta.denormalized_view_data (
    table_no                    int, 
    orig_table_no               int, 
    level_no                    int, 
    table_schema                varchar(255), 
    table_name                  varchar(255), 
    column_name                 varchar(255),
    ref_table_no                int, 
    referenced_table_schema     varchar(255), 
    referenced_table_name       varchar(255),
    referenced_column_name      varchar(255)
);
     
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Populate the table: 
insert into permissions_meta.denormalized_view_data
with recursive cte_fk_tables (table_no, orig_table_no, level_no, table_schema, table_name, column_name, ref_table_no, referenced_table_schema, referenced_table_name, referenced_column_name) as(
    select tab_tids.table_no as table_no,
           tab_tids.table_no as orig_table_no,
           1 as level_no,           
           kcu.table_schema,
           kcu.table_name,
           kcu.column_name,
           ref_tids.table_no as ref_table_no,
           kcu.referenced_table_schema,
           kcu.referenced_table_name,
           kcu.referenced_column_name
    from information_schema.key_column_usage kcu
        inner join permissions_meta.table_ids as tab_tids
            on kcu.table_schema = tab_tids.table_schema
                and kcu.table_name = tab_tids.table_name   
        inner join permissions_meta.table_ids as ref_tids
            on kcu.referenced_table_schema = ref_tids.table_schema
                and kcu.referenced_table_name = ref_tids.table_name   
    where kcu.constraint_schema = 'permissions_meta'
        and ((kcu.constraint_name like 'fk_%') or (kcu.constraint_name like 'REL%'))
    union all
    select ref_tids.table_no as table_no,
           cft.table_no as orig_table_no,
           level_no + 1 as level_no,           
           cft.referenced_table_schema as table_schema,
           cft.referenced_table_name as table_name,
           cft.referenced_column_name as column_name,
           ref_tids.table_no as ref_table_no,
           kcu2.referenced_table_schema,
           kcu2.referenced_table_name,
           kcu2.referenced_column_name
    from cte_fk_tables cft
        inner join information_schema.key_column_usage kcu2
            on cft.referenced_table_schema = kcu2.table_schema
                and cft.referenced_table_name = kcu2.table_name
        inner join permissions_meta.table_ids as tab_tids
            on cft.table_schema = tab_tids.table_schema
                and cft.table_name = tab_tids.table_name   
        inner join permissions_meta.table_ids as ref_tids
            on kcu2.referenced_table_schema = ref_tids.table_schema
                and kcu2.referenced_table_name = ref_tids.table_name                
    where kcu2.constraint_schema = 'permissions_meta'
        and ((kcu2.constraint_name like 'fk_%') or (kcu2.constraint_name like 'REL%'))
)
select table_no, 
       orig_table_no, 
       level_no, 
       table_schema, 
       table_name, 
       column_name,
       ref_table_no, 
       referenced_table_schema, 
       referenced_table_name,
       referenced_column_name
from cte_fk_tables;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Take a look at the data:
select distinct *
from permissions_meta.denormalized_view_data;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Take a look at the root tables data:
select *
from permissions_meta.denormalized_view_data
where level_no = 1;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Take a look at the none root tables data:
select *
from permissions_meta.denormalized_view_data
where level_no <> 1;
*/
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- 
set session group_concat_max_len = 100000;


with recursive denormalised_views (
    orig_table_no,
    orig_table_schema,
    orig_table_name,
    cur_table_no,
    cur_table_schema,
    cur_table_name, 
    cur_column_name,
    ref_table_no,
    referenced_table_schema,
    referenced_table_name,
    referenced_column_name,
    level_no,
    denorm_depth
) as (       
        select dvd.orig_table_no,
               dvd.table_schema as orig_table_schema,
               dvd.table_name as orig_table_name,
               dvd.table_no as cur_table_no,
               dvd.table_schema as cur_table_schema,
               dvd.table_name as cur_table_name,
               dvd.column_name as cur_column_name,
               dvd.ref_table_no,
               dvd.referenced_table_schema,
               dvd.referenced_table_name,
               dvd.referenced_column_name,
               dvd.level_no,
               1 as denorm_depth
        from permissions_meta.denormalized_view_data dvd
        where dvd.orig_table_no = 13
            and dvd.level_no = 1
        union all
        select  dv.orig_table_no as orig_table_no,
                dv.orig_table_schema as orig_table_schema,
                dv.orig_table_name as orig_table_name,
                dv.ref_table_no as table_no,
                dv.referenced_table_schema,
                dv.referenced_table_name,
                dv.referenced_column_name,
                dvd.level_no,
                dvd.referenced_table_schema,
                dvd.referenced_table_name,
                dvd.referenced_column_name,
                dvd.ref_table_no,
               dv.denorm_depth + 1 as denorm_depth
        from denormalised_views dv
            inner join permissions_meta.denormalized_view_data dvd
                on dv.referenced_table_schema = dvd.table_schema
                    and dv.referenced_table_name = dvd.table_name
                    and dv.orig_table_no = dvd.orig_table_no
                            
                                             
     )
select *
from denormalised_views dv
    inner join permissions_meta.table_meta c_pmtm
        on dv.cur_table_schema = c_pmtm.table_schema
            and dv.cur_table_name = c_pmtm.table_name    
    inner join permissions_meta.table_meta r_pmtm
        on dv.referenced_table_schema = r_pmtm.table_schema
            and dv.referenced_table_name = r_pmtm.table_name    
limit 100;

with denormalised_views_base as (
        with recursive denormalised_views (
            orig_table_no,
            orig_table_schema,
            orig_table_name,
            cur_table_no,
            cur_table_schema,
            cur_table_name, 
            cur_column_name,
            ref_table_no,
            referenced_table_schema,
            referenced_table_name,
            referenced_column_name,
            level_no
        ) as (       
                select dvd.orig_table_no,
                       dvd.table_schema as orig_table_schema,
                       dvd.table_name as orig_table_name,
                       dvd.table_no as cur_table_no,
                       dvd.table_schema as cur_table_schema,
                       dvd.table_name as cur_table_name,
                       dvd.column_name as cur_column_name,
                       dvd.ref_table_no,
                       dvd.referenced_table_schema,
                       dvd.referenced_table_name,
                       dvd.referenced_column_name,
                       dvd.level_no
                from permissions_meta.denormalized_view_data dvd
                where dvd.orig_table_no = 13
                    and dvd.level_no = 1
                union all
                select  dv.orig_table_no as orig_table_no,
                        dv.orig_table_schema as orig_table_schema,
                        dv.orig_table_name as orig_table_name,
                        dv.ref_table_no as table_no,
                        dv.referenced_table_schema,
                        dv.referenced_table_name,
                        dv.referenced_column_name,
                        dvd.level_no,
                        dvd.referenced_table_schema,
                        dvd.referenced_table_name,
                        dvd.referenced_column_name,
                        dvd.ref_table_no
                from denormalised_views dv
                    inner join permissions_meta.denormalized_view_data dvd
                        on dv.referenced_table_schema = dvd.table_schema
                            and dv.referenced_table_name = dvd.table_name
                            and dv.orig_table_no = dvd.orig_table_no
                                    
                                                     
             )     
        select dv.orig_table_schema,
                       dv.orig_table_name,
                       o_pmtm.table_acronym as orig_table_acr,
                       -- select:
                       c_pmtm.table_aliased_columns as cur_table_aliased_columns, 
                       r_pmtm.table_aliased_columns as ref_table_aliased_columns,
                       -- from:
                       dv.cur_table_schema,
                       dv.cur_table_name,
                       c_pmtm.table_acronym as cur_table_acr,
                       dv.cur_column_name,
                       -- joins:
                       dv.referenced_table_schema,
                       dv.referenced_table_name,
                       r_pmtm.table_acronym,
                       dv.referenced_column_name
        from denormalised_views dv
            inner join permissions_meta.table_meta o_pmtm
                on dv.cur_table_schema = c_pmtm.table_schema
                    and dv.cur_table_name = c_pmtm.table_name
            inner join permissions_meta.table_meta c_pmtm
                on dv.cur_table_schema = c_pmtm.table_schema
                    and dv.cur_table_name = c_pmtm.table_name    
            inner join permissions_meta.table_meta r_pmtm
                on dv.referenced_table_schema = r_pmtm.table_schema
                    and dv.referenced_table_name = r_pmtm.table_name 
    ),
    denormalised_views_columns as (
        select distinct dvb.cur_table_aliased_columns
        from denormalised_views_base dvb
        union distinct
        select distinct dvb.ref_table_aliased_columns
        from denormalised_views_base dvb
    ),
    denormalised_views_tables as (
        select concat('from ', dvb.orig_table_schema, '.' dvb.orig_table_name, ' as ', dvb.orig_table_acr)
        from denormalised_views_base dvb
    )
    
select *   
from denormalised_views_tables                   
limit 100;