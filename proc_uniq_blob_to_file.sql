CREATE OR REPLACE PROCEDURE uniq_blob_to_file (i_dir    IN VARCHAR2,
                                          i_file   IN VARCHAR2,
                                          i_blob   IN BLOB)
AS
   l_file       UTL_FILE.file_type;
   l_buffer     RAW (32767);
   l_amount     BINARY_INTEGER := 32767;
   l_pos        INTEGER := 1;
   l_blob_len   INTEGER;
BEGIN
   l_blob_len := DBMS_LOB.getlength (i_blob);

   l_file :=
      UTL_FILE.fopen (i_dir,
                      i_file,
                      'WB',
                      32767);

   WHILE l_pos < l_blob_len
   LOOP
      DBMS_LOB.read (i_blob,
                     l_amount,
                     l_pos,
                     l_buffer);
      UTL_FILE.put_raw (l_file, l_buffer, TRUE);
      l_pos := l_pos + l_amount;
   END LOOP;

   UTL_FILE.fclose (l_file);
EXCEPTION
   WHEN OTHERS
   THEN
      IF UTL_FILE.is_open (l_file)
      THEN
         UTL_FILE.fclose (l_file);
      END IF;

      RAISE;
END uniq_blob_to_file;
