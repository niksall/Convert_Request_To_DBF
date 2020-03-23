create or replace procedure uniq_create_file_dbf(type_doc   in varchar2,
                                                 table_name in varchar2) as

  TYPE t_elem IS RECORD(
    e_name        solution_eis.eisoms_format_dbf.elem_name%TYPE,
    e_data_type   solution_eis.eisoms_format_dbf.elem_data_type%TYPE,
    e_data_type_l solution_eis.eisoms_format_dbf.elem_data_type_l%TYPE,
    e_default     solution_eis.eisoms_format_dbf.elem_default%TYPE);
  TYPE elem_set IS TABLE OF t_elem;

  t_elem_collect   elem_set; -- item of collection
  rc_export_data   pkg_global.ref_cursor_type; -- cursor by log records
  string_r         varchar2(1024);
  l_result         VARCHAR(2000);
  l_export_data_id NUMBER; -- id from log records
  b_file           BLOB; -- create new file
  export_name      varchar2(1024); -- name file
  l_t_name         varchar2(1024);

begin

  l_t_name := table_name;

  if type_doc = 'b' then
    export_name := 'H' || '_' || to_char(sysdate, 'yy_mm_dd') || '_' || '1' ||
                   '.dbf';
  elsif type_doc = 'p' then
    export_name := 'S' || '_' || to_char(sysdate, 'yy_mm_dd') || '_' || '1' ||
                   '.dbf';
  end if;

  FOR r_file IN (SELECT DISTINCT f.file_name, f.table_name
                   FROM solution_eis.eisoms_format_dbf f
                  where f.table_name = l_t_name
                  ORDER BY 1 ASC) LOOP

    pkg_create_dbf.unset_columns();

    -- get new item of collection
    SELECT f.elem_name,
           f.elem_data_type,
           f.elem_data_type_l,
           f.elem_default
      BULK COLLECT
      INTO t_elem_collect
      FROM solution_eis.eisoms_format_dbf f
     WHERE f.file_name = r_file.file_name
       and f.elem_num <> 1
     ORDER BY f.elem_num ASC;

    -- create DBF header
    FOR i IN t_elem_collect.first .. t_elem_collect.last LOOP
      pkg_create_dbf.add_column(t_elem_collect(i).e_name,
                                t_elem_collect(i).e_data_type,
                                trunc(t_elem_collect(i).e_data_type_l) -- integer part of numbers
                               ,
                                to_number(ltrim(MOD(t_elem_collect(i)
                                                    .e_data_type_l,
                                                    1),
                                                '0.')) -- decimal part of numbers
                                );
    END LOOP;

    pkg_create_dbf.init('DOS');

    OPEN rc_export_data FOR '
      SELECT id
      FROM infis.' || r_file.table_name || '
      ORDER BY 1 ASC';

    loop
      fetch rc_export_data
        into l_export_data_id;
      exit when rc_export_data%notfound;

      -- cusor with items records
      FOR i IN t_elem_collect.first .. t_elem_collect.last LOOP

        execute immediate '
                SELECT to_char("' ||
                          upper(t_elem_collect(i).e_name) || '"' ||
                          CASE t_elem_collect(i).e_data_type
                            WHEN 'DATE' THEN
                             ', ''YYYYMMDD'''
                            ELSE
                             ''
                          END || ')
                  FROM infis.' ||
                          r_file.table_name || ' where id = :a'
          into l_result
          using l_export_data_id;

        -- conversion values in DOS encoding
        pkg_create_dbf.write_cell(convert(nvl(l_result,
                                              t_elem_collect(i).e_default),
                                          'RU8PC866',
                                          'CL8MSWIN1251'));

      END LOOP;
    end loop;

    CLOSE rc_export_data;

    pkg_create_dbf.get_file(b_file);

    proc_uniq_blob_to_file('EXPORT_DBF', export_name, b_file);
  end loop;
end;
/
