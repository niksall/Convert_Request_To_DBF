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
