select
      concat('drop procedure get_', primary_key_cols.primary_column_name, '_by_uk$$\n') as drop_get_procedure,
      concat(
                concat('create procedure get_', primary_key_cols.primary_column_name, '_by_uk( \n'),
                concat('\t',unq_key_cols.in_sig_field, ', \n'),
                concat('\t',primary_key_cols.out_sig_field,',\n\t',primary_key_cols.out_sig_field_message,'\n)'),
                '\nbegin \n',
                '-- Get the: ', primary_key_cols.out_field, '\n',
                concat('\tset ', primary_key_cols.out_param, ' = ', '(\n',
                      '\t\tselect ', primary_key_cols.out_field, '\n',
                      '\t\tfrom ', primary_key_cols.primary_table_name, '\n',
                      '\t\twhere ', unq_key_cols.where_clause,'\n',
                      '\t); \n'),
                '-- Return the: ', primary_key_cols.call_out_field, ' and the message:', primary_key_cols.call_out_field_message, '\n',
                '\tif (',primary_key_cols.out_param,' is null) then \n'
                '\t\tset ', primary_key_cols.call_out_field_message, ' = ', quote('ID Not Found'), ';\n',
                '\telse \n',
                '\t\tset ', primary_key_cols.call_out_field_message, ' = ', quote('ID Found'), ';\n',
                '\tend if;\n'
                'end',
                '$$ \n'
      ) as create_get_procedure,
      concat('call get_', primary_key_cols.primary_column_name, '_by_uk(', unq_key_cols.example_call_in, ',',primary_key_cols.call_out_field,',',primary_key_cols.call_out_field_message,')$$\n') as example_call,
      concat('select ', primary_key_cols.call_out_field, ' as fetched_', primary_key_cols.primary_column_name, ',', primary_key_cols.call_out_field_message,' as out_message$$\n') as example_get_the_field
from information_schema.tables ist
    -- Get the primary key column:
    inner join (
                select isc.table_schema as primary_schema_name,
                       isc.table_name as primary_table_name,
                       isc.column_name as primary_column_name,
                       concat('out out_',
                              isc.column_name, ' ',
                              isc.data_type,
                              case when (isc.data_type = 'int') then
                                ''
                              else
                                concat('(', isc.character_maximum_length, ')')
                              end) as out_sig_field,
                       concat('out out_', isc.column_name, '_message varchar(200)') as out_sig_field_message,
                       concat('out_',isc.column_name) as out_param,
                       isc.column_name as out_field,
                       concat(
                             '@out_',
                             isc.column_name
                       ) as call_out_field,
                       concat(
                             '@out_',
                             isc.column_name,
                             '_message'
                       ) as call_out_field_message
                    from information_schema.columns isc
                    where isc.column_key = 'PRI'
                    and isc.table_schema = '<REPLACE WITH YOUR DB NAME>'
               ) primary_key_cols
        on ist.table_name = primary_key_cols.primary_table_name
            and ist.table_schema = primary_key_cols.primary_schema_name
    -- Get the unique key columns:
    inner join (
                select klu.table_schema,
                       klu.table_name,
                       group_concat(
                            concat('in in_',
                                  isc.column_name, ' ',
                                  isc.data_type,
                                  case when (isc.data_type = 'int') then
                                    ''
                                  else
                                    concat('(', isc.character_maximum_length, ')')
                                  end)
                       )as in_sig_field,
                       group_concat(
                            concat(isc.column_name, ' = in_',isc.column_name)
                            separator ' \n \t\t and '
                       )as where_clause,
                       group_concat(
                            concat('<in_',
                                  isc.column_name, ' ',
                                  isc.data_type,
                                  case when (isc.data_type = 'int') then
                                    ''
                                  else
                                    concat('(', isc.character_maximum_length, ')')
                                  end, '>')
                       )as example_call_in
                from information_schema.key_column_usage klu
                    inner join information_schema.columns isc
                        on klu.table_schema = isc.table_schema
                            and klu.table_name = isc.table_name
                            and klu.column_name = isc.column_name
                where klu.table_schema = '<REPLACE WITH YOUR DB NAME>'
                    and klu.constraint_name like 'uix%'
                group by klu.table_schema,
                       klu.table_name
               ) unq_key_cols
        on ist.table_schema = unq_key_cols.table_schema
            and ist.table_name = unq_key_cols.table_name
where ist.table_type = 'BASE TABLE'
    and ist.TABLE_SCHEMA = '<REPLACE WITH YOUR DB NAME>'
go