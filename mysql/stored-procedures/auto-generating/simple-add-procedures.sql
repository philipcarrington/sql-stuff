select distinct concat('drop procedure add_', primary_keys.table_name, '$$\n') as drop_add_procedure,
       concat(
               concat('create procedure add_', primary_keys.table_name, '( \n'),
               concat(other_fields.in_sig_field, ', \n',primary_keys.out_sig_field, '\n ) \n'),
               'begin\n\n',
               concat('declare ', primary_keys.table_name, '_exists int default 0; \n\n'),
               concat(
                        'set ', primary_keys.table_name, '_exists = ( \n',
                        '\t select count(*) \n',
                        '\t from ', primary_keys.table_name, ' \n',
                        '\t where ', unq_key_cols.where_clause, ' \n',
                        '); \n\n'

                     ),
               concat(
                        'if (', primary_keys.table_name, '_exists = 0) then \n',
                        '\t insert into ', primary_keys.table_name, '(\n',
                        other_fields.insert_fields, '\n',
                        '\t ) \n',
                        '\t values (\n',
                        other_fields.insert_param_fields, '\n',
                        '\t ); \n\n',
                        '\t set @', primary_keys.out_param, ' = LAST_INSERT_ID();\n\n',
                        'end if; \n\n'
               ),
               'end',
               '$$\n'
       ) as create_add_procedure,
      concat('call add_', primary_keys.table_name, '( \n' ,concat(other_fields.in_sig_field, ', \n',primary_keys.out_sig_field, '\n )$$\n')) as example_call,
      concat('select @', primary_keys.out_param, ' as fetched_', primary_keys.column_name, ' $$\n') as example_add_the_field
from (
        select isc.table_schema,
               isc.table_name,
               isc.column_name,
               concat('\t out_', isc.column_name, ' ', isc.data_type) as out_sig_field,
               concat('out_', isc.column_name) as out_param
        from information_schema.columns isc
        where isc.table_schema = 'permissions_meta'
            and isc.column_key = 'PRI'
     ) as primary_keys
    inner join
     (
        select isc.table_schema,
               isc.table_name,
               group_concat(
                            concat('\t in_',
                                   isc.column_name, ' ',
                                   isc.data_type,
                                   case when (isc.data_type = 'int') then
                                     ''
                                   else
                                     concat('(', isc.character_maximum_length, ')')
                                   end)
                            separator ', \n '
                           )as in_sig_field,
               group_concat(
                            concat('\t\t',isc.column_name)
                            separator', \n'
                           )as insert_fields,
                group_concat(
                            concat('\t\tin_',isc.column_name)
                            separator', \n'
                           )as insert_param_fields
        from information_schema.columns isc
        where isc.table_schema = 'permissions_meta'
            and isc.column_key <> 'PRI'
        group by isc.table_schema,
               isc.table_name
     ) as other_fields
    on primary_keys.table_schema = other_fields.table_schema
        and primary_keys.table_name = other_fields.table_name
    inner join (
                select klu.table_schema,
                       klu.table_name,
                       group_concat(
                            concat(isc.column_name, ' = in_',isc.column_name)
                            separator ' \n and '
                       )as where_clause
                from information_schema.key_column_usage klu
                    inner join information_schema.columns isc
                        on klu.table_schema = isc.table_schema
                            and klu.table_name = isc.table_name
                            and klu.column_name = isc.column_name
                where klu.table_schema = 'permissions_meta'
                    and klu.constraint_name like 'uix%'
                group by klu.table_schema,
                       klu.table_name
               ) unq_key_cols
        on primary_keys.table_schema = unq_key_cols.table_schema
            and primary_keys.table_name = unq_key_cols.table_name
go