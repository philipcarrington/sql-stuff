set session group_concat_max_len = 1000000
go
drop view permissions_meta.db_gen_table_columns
go
create view permissions_meta.db_gen_table_columns as (
    select get_column_tables.table_schema,
           get_column_tables.table_name,
           group_concat(
                        concat('\t', get_column_tables.table_acronym, '.', isc.column_name, ' as ', get_column_tables.table_acronym, '_', isc.column_name)
                        separator ', \n '
            )as alias_column
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
                 get_column_tables.table_name
)
go
select forign_key_data.table_schema,
       forign_key_data.table_name,
       concat('drop view ', forign_key_data.schema_for_the_views, '.', forign_key_data.table_name, '_denormalised \n', forign_key_data.statement_sep, '\n') as drop_view,
       concat(
            -- Create View:
            'create view ', forign_key_data.schema_for_the_views, '.', forign_key_data.table_name, '_denormalised as ( \n'
            -- Selects:
            'select \n ',
                    orig_pmtc.alias_column, ', \n ',
                    group_concat(
                                    ref_pmtc.alias_column
                                    separator ', \n '
                                ),
            -- Froms:
            concat('\nfrom ', forign_key_data.child_table, ' as ', forign_key_data.child_table_alias, '\n'),
            group_concat(
                            concat('\t inner join ', forign_key_data.parent_table, ' as ', forign_key_data.parent_table_alias, 
                                ' \n \t \t on ', child_join_column, ' = ', parent_join_column)
                            separator ' \n '
                        ),
            -- End Create:
            '\n )',
            forign_key_data.statement_sep, '\n'
       ) as create_view                
from 
    (
        select base.table_schema,
               base.table_name,
               concat(base.table_schema, '.', base.table_name) as child_table,
               base.child_table_alias,
               concat(base.child_table_alias, '.', base.column_name) as child_join_column,
               base.referenced_table_schema,
               base.referenced_table_name,
               base.parent_table,
               base.parent_table_alias,
               concat(base.parent_table_alias, '.', base.referenced_column_name) as parent_join_column,                       
               'permissions_meta' as schema_for_the_views,
               'go' as statement_sep
        from 
            (
                select kcu.table_schema,
                       kcu.table_name,
                       kcu.column_name,                       
                       acronym(kcu.table_name) as child_table_alias,                                                     
                       kcu.referenced_table_schema,
                       kcu.referenced_table_name,
                       concat(kcu.referenced_table_schema, '.', kcu.referenced_table_name) as parent_table,
                       acronym(kcu.referenced_table_name) as parent_table_alias,
                       kcu.referenced_column_name
                from information_schema.key_column_usage kcu
                where kcu.constraint_schema = 'permissions_meta'
                    and ((kcu.constraint_name like 'fk_%') or (kcu.constraint_name like 'REL%'))
            ) as base
    ) as forign_key_data        
    inner join permissions_meta.db_gen_table_columns orig_pmtc
        on forign_key_data.table_schema = orig_pmtc.table_schema
            and forign_key_data.table_name = orig_pmtc.table_name
    inner join permissions_meta.db_gen_table_columns ref_pmtc
        on forign_key_data.referenced_table_schema = ref_pmtc.table_schema
            and forign_key_data.referenced_table_name = ref_pmtc.table_name
group by forign_key_data.table_schema,
       forign_key_data.table_name,
       orig_pmtc.alias_column,
       forign_key_data.child_table_alias,
       forign_key_data.child_table      
              
          
            
        