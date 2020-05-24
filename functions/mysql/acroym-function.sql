--------------------------------------------------------------------------------
-- !! Not all my own work !!
-- No point in reinventing the wheel; adjustments made though
-- Found here: https://stackoverflow.com/questions/8672815/mysql-extract-first-letter-of-each-word-in-a-specific-column
-------------------------------------------------------------------------------

set global log_bin_trust_function_creators = 1
go
drop function if exists `initials`
go
create function `initials`(str varchar(64), expr varchar(64)) RETURNS varchar(64) CHARSET utf8
begin
    declare result varchar(64) default '';
    declare buffer varchar(64) default '';
    declare i int default 1;
    if(str is null) then
        return null;
    end if;
    set buffer = trim(str);
    while i <= length(buffer) do
        if substr(buffer, i, 1) regexp expr then
            set result = concat( result, substr( buffer, i, 1 ));
            set i = i + 1;
            while i <= length( buffer ) and substr(buffer, i, 1) regexp expr do
                set i = i + 1;
            end while;
            while i <= length( buffer ) and substr(buffer, i, 1) not regexp expr do
                set i = i + 1;
            end while;
        else
            set i = i + 1;
        end if;
    end while;
    return result;
end
go
drop function if exists `acronym`
go
CREATE FUNCTION `acronym`(str varchar(64)) RETURNS varchar(64) CHARSET utf8
begin
    declare result varchar(64) default '';
    set result = initials( str, '[[:alnum:]]' );
    return result;
end
go
select acronym('come_Again_That_Cant_Help!')
go
