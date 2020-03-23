CREATE OR REPLACE PACKAGE pkg_create_dbf IS
  -- Purpose : creating DBF (dBase III) files
  -- Public function and procedure declarations

  /*Clear current file status*/
  PROCEDURE unset_columns

  /*Add column (header) to file*/
  PROCEDURE add_column(s_name      VARCHAR2
                      ,s_type      VARCHAR2
                      ,n_length    PLS_INTEGER DEFAULT NULL
                      ,n_precision PLS_INTEGER DEFAULT NULL)
  
  /* Header initialization
         s_encoding: WIN\DOS
         s_firstbyte: 03\04 */
  PROCEDURE init(s_encoding  VARCHAR2
                ,s_firstbyte VARCHAR2 := '03')

  /* Fill cell file
      p_mode: 0 - Leave in source encoding
               1 - Translate from 'CL8MSWIN1251' в 'RU8PC866' */
  PROCEDURE write_cell(d_date DATE
                      ,p_mode NUMBER DEFAULT 0)

  /* Fill cell file
      p_mode: 0 - Leave in source encoding
               1 - Translate from 'CL8MSWIN1251' в 'RU8PC866' */
  PROCEDURE write_cell(s_string VARCHAR2
                      ,p_mode   NUMBER DEFAULT 0)
    
  /* Fill cell file
      p_mode: 0 - Leave in source encoding
               1 - Translate from 'CL8MSWIN1251' в 'RU8PC866' */
  PROCEDURE write_cell(n_number NUMBER
                      ,p_mode   NUMBER DEFAULT 0)
  
  /* For call capacity acc. procedures in sql request */
  FUNCTION fn_write_cell(d_date DATE
                        ,p_mode NUMBER DEFAULT 0) RETURN NUMBER
  
  /* To be able to call the appropriate procedure in sql query */
  FUNCTION fn_write_cell(s_string VARCHAR2
                        ,p_mode   NUMBER DEFAULT 0) RETURN NUMBER
    
  /* For the possibility of calling the corresponding procedure in sql request */  
  FUNCTION fn_write_cell(n_number NUMBER
                        ,p_mode   NUMBER DEFAULT 0) RETURN NUMBER

  /* Get current status file */
  PROCEDURE get_file(b_blob OUT BLOB)

  /* Get current status memo-file */
  PROCEDURE get_memo_file(b_memo_blob OUT BLOB)

END pkg_create_dbf;
/

CREATE OR REPLACE PACKAGE BODY pkg_create_dbf IS

  -- Private type declarations
  TYPE tp_r_column_type IS RECORD(
     NAME      VARCHAR2(11)
    ,TYPE      VARCHAR2(30)
    ,length    PLS_INTEGER
    ,PRECISION PLS_INTEGER);

  TYPE tp_t_r_column_types IS TABLE OF tp_r_column_type INDEX BY PLS_INTEGER;

  -- Private variable declarations
  b_body                   BLOB;
  b_header_before_rowcount BLOB;
  b_header_after_rowcount  BLOB;
  b_memo_body              BLOB;
  --b_memo_header          BLOB;
  raw_buf                  RAW(32767);
  s_codepage               VARCHAR2(2) := '65' /* 65 - DOS, С9/57 - WIN */;
  s_nill                   VARCHAR2(1) := chr(0);
  n_curr_cell              PLS_INTEGER := 1;
  t_columns                tp_t_r_column_types;
  n_rowcount               PLS_INTEGER := 0;
  n_memocount              PLS_INTEGER := 0;

  -- Function and procedure implementations
  PROCEDURE unset_columns

  /* Clear current file status */ IS
  BEGIN
    t_columns.delete;
  END unset_columns;

  PROCEDURE add_column(s_name      VARCHAR2
                      ,s_type      VARCHAR2
                      ,n_length    PLS_INTEGER DEFAULT NULL
                      ,n_precision PLS_INTEGER DEFAULT NULL)
    /* Add column (header) to file */ IS
    n_count PLS_INTEGER := t_columns.count;
  BEGIN
    FOR i IN 1 .. n_count
    LOOP
      IF t_columns(i).name = s_name THEN
        raise_application_error(-20001, 'COLUMN_ALREADY_DEFINED');
      END IF;
    END LOOP;

    n_count := n_count + 1;
    t_columns(n_count).name := s_name;
    t_columns(n_count).type := s_type;

    IF upper(s_type) = 'DATE' THEN
      t_columns(n_count).length := 8;
    ELSIF upper(s_type) = 'MEMO' THEN
      t_columns(n_count).length := 10;
    ELSE
      t_columns(n_count).length := n_length;
    END IF;

    t_columns(n_count).precision := n_precision;
  END add_column;

  FUNCTION getraw(s VARCHAR2) RETURN RAW
    /* Convert hex string to raw, takes a string of the form type '0D 0A 19' */ IS
  BEGIN
    RETURN hextoraw(REPLACE(s, ' '));
  END getraw;

  FUNCTION prepraw(s VARCHAR2) RETURN RAW
    /* Convert string to raw */ IS
  BEGIN
    RETURN utl_raw.cast_to_raw(s);
  END prepraw;

  FUNCTION nillbyte RETURN RAW IS
  BEGIN
    RETURN prepraw(s_nill);
  END;

  FUNCTION nillbyte(n PLS_INTEGER) RETURN RAW IS
    raw_result RAW(32767);
  BEGIN
    FOR i IN 1 .. n
    LOOP
      raw_result := utl_raw.concat(raw_result, nillbyte());
    END LOOP;

    RETURN raw_result;
  END nillbyte;

  -- Convert varchar2 to raw
  FUNCTION putbyte(VALUE    PLS_INTEGER DEFAULT 0
                  ,n_length PLS_INTEGER DEFAULT 1) RETURN RAW IS
  BEGIN
    IF nvl(VALUE, 0) <= 255 THEN
      RETURN utl_raw.concat(prepraw(chr(nvl(VALUE, 0))), nillbyte(n_length - 1));
    ELSE
      RETURN utl_raw.substr(utl_raw.cast_from_binary_integer(VALUE, 2), 1, n_length);
    END IF;
  END putbyte;

  FUNCTION putbyte(VALUE    NUMBER DEFAULT 0
                  ,n_length PLS_INTEGER DEFAULT 1) RETURN RAW IS
    raw_result RAW(32767);
  BEGIN
    IF nvl(VALUE, 0) <= 255 THEN
      RETURN utl_raw.concat(prepraw(chr(nvl(VALUE, 0))), nillbyte(n_length - 1));
    ELSE
      RETURN utl_raw.substr(utl_raw.cast_from_binary_integer(VALUE, 2), 1, n_length);
    END IF;
  END putbyte;

  PROCEDURE writeblob(s_blob VARCHAR2 DEFAULT 'body') IS
  BEGIN
    IF s_blob = 'body' THEN
      dbms_lob.writeappend(b_body, utl_raw.length(raw_buf), raw_buf);
    ELSIF s_blob = 'before_rowcount' THEN
      dbms_lob.writeappend(b_header_before_rowcount, utl_raw.length(raw_buf), raw_buf);
    ELSIF s_blob = 'memo_body' THEN
      dbms_lob.writeappend(b_memo_body, utl_raw.length(raw_buf), raw_buf);
    ELSE
      dbms_lob.writeappend(b_header_after_rowcount, utl_raw.length(raw_buf), raw_buf);
    END IF;
  END writeblob;

  PROCEDURE init(s_encoding  VARCHAR2
                ,s_firstbyte VARCHAR2 := '03')
    /* Header initialization
         s_encoding: WIN\DOS
         s_firstbyte: 03\04 */ IS
    n_record_size PLS_INTEGER := 0;
    l_fisrtbyte   VARCHAR2(3) := s_firstbyte;
  BEGIN
    IF s_encoding NOT IN ('DOS', 'WIN') THEN
      raise_application_error(-20001, 'INCORRECT_ENCODING');
    ELSE
      CASE s_encoding
        WHEN 'DOS' THEN
          s_codepage := '65';
        WHEN 'WIN' THEN
          s_codepage := 'C9';
      END CASE;
    END IF;

    IF t_columns.count = 0 THEN
      raise_application_error(-20001, 'COLUMNS_NOT_DEFINED');
    END IF;

    IF l_fisrtbyte IS NULL THEN
      l_fisrtbyte := '03';
    END IF;

    -- count record size
    FOR i IN 1 .. t_columns.count
    LOOP
      n_record_size := n_record_size + t_columns(i).length;
      IF upper(t_columns(i).type) = 'MEMO' THEN
        l_fisrtbyte := '83';
      END IF;
    END LOOP;

    n_record_size := n_record_size + 1;
    dbms_lob.createtemporary(b_header_before_rowcount, TRUE);
    dbms_lob.createtemporary(b_header_after_rowcount, TRUE);
    dbms_lob.createtemporary(b_body, TRUE);
    IF l_fisrtbyte = '83' THEN
      dbms_lob.createtemporary(b_memo_body, TRUE);
      --dbms_lob.createtemporary(b_memo_header, TRUE);
    END IF;
    n_curr_cell := 1;
    n_rowcount  := 0;
    n_memocount := 0;

    -- DBF header
    raw_buf := utl_raw.concat(-- 00 Simple table code  /* 0x00 l=1 */
                              getraw(l_fisrtbyte)
                             ,-- Creation date         /* 0x01 l=3 */
                              utl_raw.concat(-- 01
                                             prepraw(chr(to_number(to_char(SYSDATE, 'YY')) + 100))
                                            ,-- 02
                                             prepraw(chr(to_number(to_char(SYSDATE, 'MM'))))
                                            ,-- 03
                                             prepraw(chr(to_number(to_char(SYSDATE, 'DD'))))));
    writeblob('before_rowcount');                      /* 0x04 l=4 */

    raw_buf := utl_raw.concat(-- 08 HeaderSize     	   /* 0x08 l=2 */
                              putbyte(32 * (t_columns.count + 1) + 1, 2)
                             ,-- 10 RecordSize         /* 0x0A l=2 */
                              putbyte(n_record_size, 2)
                             ,-- 12 Reserved           /* 0x0C l=2 */
                              nillbyte(2)
                             ,-- 14 Ignored            /* 0x0E l=1 */
                              nillbyte()
                             ,-- 15 Normal visibility  /* 0x0F l=1 */
                              nillbyte()
                             ,-- 16 Multiuser mode off /* 0x10 l=12*/
                              nillbyte(12)
                             ,-- 28 Index not used     /* 0x1C l=1 */
                              nillbyte()
                             ,-- 29 Codepage           /* 0x1D l=1 */
                              getraw(s_codepage)
                             ,-- 30 Reserved           /* 0x1E l=2 */
                              nillbyte(2));
    writeblob('after_rowcount');

    -- Field header
    FOR i IN 1 .. t_columns.count
    LOOP
      raw_buf := utl_raw.concat(-- Name
                                prepraw(rpad(t_columns(i).name, 11, s_nill))
                               ,-- Type
                                prepraw(upper(substr(t_columns(i).type, 1, 1)))
                               ,-- Ignored
                                nillbyte(4)
                               ,-- Field size
                                putbyte(nvl(t_columns(i).length, 0))
                               ,-- Precision size
                                putbyte(nvl(t_columns(i).precision, 0))
                               ,-- Reserved
                                nillbyte(2)
                               ,-- Ignored
                                nillbyte()
                               ,-- Ignored
                                nillbyte(2)
                               ,-- Ignored
                                nillbyte()
                               ,-- Reserved
                                nillbyte(7)
                               ,-- Ignored
                                nillbyte());
      writeblob;
    END LOOP;

    raw_buf := getraw('0D');
    writeblob;
  END init;

  PROCEDURE write_cell_(VALUE    RAW
                       ,s_type   VARCHAR2
                       ,p_strict BOOLEAN DEFAULT FALSE) IS
    l_value RAW(32767) := VALUE;
    l_length PLS_INTEGER;
    l_blocks PLS_INTEGER;
  BEGIN
    IF n_curr_cell = 1 THEN
      raw_buf := prepraw(' ');
      writeblob;
    END IF;

    IF p_strict = TRUE
      AND upper(t_columns(n_curr_cell).type) <> upper(s_type) THEN
      raise_application_error(-20001
                             ,'COLUMN_TYPE_MISMATCH: "' || upper(t_columns(n_curr_cell).type) || '" AND "' ||
                              upper(s_type) || '"');
    END IF;

    IF upper(t_columns(n_curr_cell).type) = 'LOGICAL' THEN
      CASE utl_raw.cast_to_varchar2(l_value)
        WHEN '0' THEN l_value := utl_raw.cast_to_raw('f');
        WHEN '1' THEN l_value := utl_raw.cast_to_raw('t');
        ELSE l_value := l_value;
      END CASE;
    END IF;

    IF upper(t_columns(n_curr_cell).type) = 'NUMERIC' THEN
      -- filling left
      raw_buf := utl_raw.concat(prepraw(rpad(' ', t_columns(n_curr_cell).length - utl_raw.length(l_value)))
                               ,l_value);
    ELSIF upper(t_columns(n_curr_cell).type) = 'MEMO' THEN
      -- filling memo-fields
       IF l_value IS NULL OR length(l_value) = 0 THEN
        -- нет значения - пустая ссылка (на нулевой блок данных)
        raw_buf := prepraw('0000000000');
      ELSE
        l_value := utl_raw.concat(l_value, 
                                  prepraw(CHR(26) || CHR(26)));
        l_length := utl_raw.length(l_value);
        IF mod(l_length, 512) = 0 THEN
          l_blocks := l_length / 512;
          raw_buf := l_value;
        ELSE
          l_blocks := (l_length - mod(l_length, 512)) / 512;
          l_blocks := l_blocks + 1;
          raw_buf := utl_raw.concat(l_value
                                    ,nillbyte(512 - mod(l_length, 512)));
        END IF;
        writeblob('memo_body');
        
        raw_buf := prepraw(to_char((n_memocount + 1), 'FM0000000000'));
        n_memocount := n_memocount + l_blocks;
      END IF;
    ELSE
      -- filling right
      raw_buf := utl_raw.concat(l_value
                               ,prepraw(rpad(' ', t_columns(n_curr_cell).length - utl_raw.length(l_value))));
    END IF;

    writeblob;

    IF n_curr_cell = t_columns.count THEN
      n_curr_cell := 1;
      n_rowcount  := n_rowcount + 1;
    ELSE
      n_curr_cell := n_curr_cell + 1;
    END IF;
  END write_cell_;

  PROCEDURE write_cell(d_date DATE
                      ,p_mode NUMBER DEFAULT 0)
    /* Fill cell file
       p_mode: 0 - Leave in source encoding
               1 - Translate from 'CL8MSWIN1251' в 'RU8PC866' */ IS
  BEGIN
    IF p_mode = 1 THEN
      write_cell_(prepraw(convert(nvl(to_char(d_date, 'YYYYMMDD'), ' ')
                                 ,'RU8PC866'
                                 ,'CL8MSWIN1251'))
                 ,'date');
    ELSE
      write_cell_(prepraw(nvl(to_char(d_date, 'YYYYMMDD'), ' ')), 'date');
    END IF;

  END write_cell;

  PROCEDURE write_cell(s_string VARCHAR2
                      ,p_mode   NUMBER DEFAULT 0)
    /* Filling cell file
       p_mode: 0 - Leave in source encoding
               1 - Translate from 'CL8MSWIN1251' в 'RU8PC866' */ IS
  BEGIN
    IF p_mode = 1 THEN
       write_cell_(prepraw(convert(nvl(substr(s_string, 1, t_columns(n_curr_cell).length), ' ')
                                  ,'RU8PC866'
                                  ,'CL8MSWIN1251'))
                  ,'char');
    ELSIF upper(t_columns(n_curr_cell).type) = 'MEMO' THEN
       write_cell_(prepraw(s_string), 'char');
    ELSE
       write_cell_(prepraw(nvl(substr(s_string, 1, t_columns(n_curr_cell).length), ' ')), 'char');
    END IF;

  END write_cell;

  PROCEDURE write_cell(n_number NUMBER
                      ,p_mode   NUMBER DEFAULT 0)
    /* Fill cell file
       p_mode: 0 - Leave in source encoding
               1 - Translate from 'CL8MSWIN1251' в 'RU8PC866' */ IS
    n_length PLS_INTEGER;
    s_mask   VARCHAR2(60);
  BEGIN
    n_length := t_columns(n_curr_cell).length - t_columns(n_curr_cell).precision;
    s_mask   := lpad('9', n_length, '9');

    IF nvl(t_columns(n_curr_cell).precision, 0) > 0 THEN
      s_mask := s_mask || '.';
      s_mask := s_mask || lpad('9', t_columns(n_curr_cell).precision, '9');
    END IF;

    IF p_mode = 1 THEN
      write_cell_(prepraw(convert(lpad(nvl(TRIM(to_char(n_number, s_mask)), ' '), t_columns(n_curr_cell).length)
                                 ,'RU8PC866'
                                 ,'CL8MSWIN1251'))
                 ,'number');
    ELSE
      write_cell_(prepraw(lpad(nvl(TRIM(to_char(n_number, s_mask)), ' '), t_columns(n_curr_cell).length)), 'number');
    END IF;
  END write_cell;

  FUNCTION fn_write_cell(d_date DATE
                        ,p_mode NUMBER DEFAULT 0) RETURN NUMBER
    /* To be able to call the appropriate procedure in sql query */ IS
  BEGIN
    pkg_create_dbf.write_cell(d_date => d_date
                             ,p_mode => p_mode);
    RETURN 1;
  END;

  FUNCTION fn_write_cell(s_string VARCHAR2
                        ,p_mode   NUMBER DEFAULT 0) RETURN NUMBER
    /* To be able to call the appropriate procedure in sql query */ IS
  BEGIN
    pkg_create_dbf.write_cell(s_string => s_string
                             ,p_mode   => p_mode);
    RETURN 1;
  END;

  FUNCTION fn_write_cell(n_number NUMBER
                        ,p_mode   NUMBER DEFAULT 0) RETURN NUMBER
    /* To be able to call the appropriate procedure in sql query */ IS
  BEGIN
    pkg_create_dbf.write_cell(n_number => n_number
                             ,p_mode   => p_mode);
    RETURN 1;
  END;

  PROCEDURE get_file(b_blob OUT BLOB)
    /* Get current file status */ IS
  BEGIN
    -- write row count info
    b_blob := b_header_before_rowcount;
    dbms_lob.writeappend(b_blob, 4, putbyte(n_rowcount, 4));

    -- close dbf format
    raw_buf := getraw('1A');
    writeblob;

    -- stick together
    dbms_lob.append(b_blob, b_header_after_rowcount);
    dbms_lob.append(b_blob, b_body);
    dbms_lob.freetemporary(b_header_before_rowcount);
    dbms_lob.freetemporary(b_header_after_rowcount);
    dbms_lob.freetemporary(b_body);
  END get_file;

  PROCEDURE get_memo_file(b_memo_blob OUT BLOB)
    /* Get current file status */ IS
  BEGIN
    IF b_memo_body IS NULL THEN
      RETURN;
    END IF;
    
    -- write row count info
    dbms_lob.createtemporary(b_memo_blob, TRUE);
    dbms_lob.writeappend(b_memo_blob, 4, putbyte(n_memocount + 1, 4));
    dbms_lob.writeappend(b_memo_blob, 508, nillbyte(508));

    -- close dbf-memo file
    --raw_buf := getraw('1A');
    --writeblob('memo_body');

    -- stick together
    dbms_lob.append(b_memo_blob, b_memo_body);
    dbms_lob.freetemporary(b_memo_body);
    b_memo_body := NULL;
  END get_memo_file;

END pkg_create_dbf;
/

