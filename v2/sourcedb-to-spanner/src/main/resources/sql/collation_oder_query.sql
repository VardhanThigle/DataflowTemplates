-- Unfortunately we can't use prepared statement to set variable values.
-- SET @db_charset = 'charset_replacement_tag';
-- SET @db_collation = 'collation_replacement_tag';
SET @db_charset = 'utf8mb3';
SET @db_collation = 'utf8mb3_general_ci';
-- Any charset natively supported by java.
SET @native_charset = 'utf8';



-- a union of single byte literals from 0x00 to 0xff.
SET @byte_literals = CONCAT(
      'SELECT ''00'' AS h UNION ALL SELECT ''01'' UNION ALL SELECT ''02'' UNION ALL SELECT ''03'' UNION ALL SELECT ''04'' UNION ALL SELECT ''05'' UNION ALL SELECT ''06'' UNION ALL SELECT ''07'' UNION ALL SELECT ''08'' UNION ALL SELECT ''09'' UNION ALL SELECT ''0a'' UNION ALL SELECT ''0b'' UNION ALL SELECT ''0c'' UNION ALL SELECT ''0d'' UNION ALL SELECT ''0e'' UNION ALL SELECT ''0f''',
'UNION ALL SELECT ''10'' AS h UNION ALL SELECT ''11'' UNION ALL SELECT ''12'' UNION ALL SELECT ''13'' UNION ALL SELECT ''14'' UNION ALL SELECT ''15'' UNION ALL SELECT ''16'' UNION ALL SELECT ''17'' UNION ALL SELECT ''18'' UNION ALL SELECT ''19'' UNION ALL SELECT ''1a'' UNION ALL SELECT ''1b'' UNION ALL SELECT ''1c'' UNION ALL SELECT ''1d'' UNION ALL SELECT ''1e'' UNION ALL SELECT ''1f''',
'UNION ALL SELECT ''20'' AS h UNION ALL SELECT ''21'' UNION ALL SELECT ''22'' UNION ALL SELECT ''23'' UNION ALL SELECT ''24'' UNION ALL SELECT ''25'' UNION ALL SELECT ''26'' UNION ALL SELECT ''27'' UNION ALL SELECT ''28'' UNION ALL SELECT ''29'' UNION ALL SELECT ''2a'' UNION ALL SELECT ''2b'' UNION ALL SELECT ''2c'' UNION ALL SELECT ''2d'' UNION ALL SELECT ''2e'' UNION ALL SELECT ''2f''',
'UNION ALL SELECT ''30'' AS h UNION ALL SELECT ''31'' UNION ALL SELECT ''32'' UNION ALL SELECT ''33'' UNION ALL SELECT ''34'' UNION ALL SELECT ''35'' UNION ALL SELECT ''36'' UNION ALL SELECT ''37'' UNION ALL SELECT ''38'' UNION ALL SELECT ''39'' UNION ALL SELECT ''3a'' UNION ALL SELECT ''3b'' UNION ALL SELECT ''3c'' UNION ALL SELECT ''3d'' UNION ALL SELECT ''3e'' UNION ALL SELECT ''3f''',
'UNION ALL SELECT ''40'' AS h UNION ALL SELECT ''41'' UNION ALL SELECT ''42'' UNION ALL SELECT ''43'' UNION ALL SELECT ''44'' UNION ALL SELECT ''45'' UNION ALL SELECT ''46'' UNION ALL SELECT ''47'' UNION ALL SELECT ''48'' UNION ALL SELECT ''49'' UNION ALL SELECT ''4a'' UNION ALL SELECT ''4b'' UNION ALL SELECT ''4c'' UNION ALL SELECT ''4d'' UNION ALL SELECT ''4e'' UNION ALL SELECT ''4f''',
'UNION ALL SELECT ''50'' AS h UNION ALL SELECT ''51'' UNION ALL SELECT ''52'' UNION ALL SELECT ''53'' UNION ALL SELECT ''54'' UNION ALL SELECT ''55'' UNION ALL SELECT ''56'' UNION ALL SELECT ''57'' UNION ALL SELECT ''58'' UNION ALL SELECT ''59'' UNION ALL SELECT ''5a'' UNION ALL SELECT ''5b'' UNION ALL SELECT ''5c'' UNION ALL SELECT ''5d'' UNION ALL SELECT ''5e'' UNION ALL SELECT ''5f''',
'UNION ALL SELECT ''60'' AS h UNION ALL SELECT ''61'' UNION ALL SELECT ''62'' UNION ALL SELECT ''63'' UNION ALL SELECT ''64'' UNION ALL SELECT ''65'' UNION ALL SELECT ''66'' UNION ALL SELECT ''67'' UNION ALL SELECT ''68'' UNION ALL SELECT ''69'' UNION ALL SELECT ''6a'' UNION ALL SELECT ''6b'' UNION ALL SELECT ''6c'' UNION ALL SELECT ''6d'' UNION ALL SELECT ''6e'' UNION ALL SELECT ''6f''',
'UNION ALL SELECT ''70'' AS h UNION ALL SELECT ''71'' UNION ALL SELECT ''72'' UNION ALL SELECT ''73'' UNION ALL SELECT ''74'' UNION ALL SELECT ''75'' UNION ALL SELECT ''76'' UNION ALL SELECT ''77'' UNION ALL SELECT ''78'' UNION ALL SELECT ''79'' UNION ALL SELECT ''7a'' UNION ALL SELECT ''7b'' UNION ALL SELECT ''7c'' UNION ALL SELECT ''7d'' UNION ALL SELECT ''7e'' UNION ALL SELECT ''7f''',
'UNION ALL SELECT ''80'' AS h UNION ALL SELECT ''81'' UNION ALL SELECT ''82'' UNION ALL SELECT ''83'' UNION ALL SELECT ''84'' UNION ALL SELECT ''85'' UNION ALL SELECT ''86'' UNION ALL SELECT ''87'' UNION ALL SELECT ''88'' UNION ALL SELECT ''89'' UNION ALL SELECT ''8a'' UNION ALL SELECT ''8b'' UNION ALL SELECT ''8c'' UNION ALL SELECT ''8d'' UNION ALL SELECT ''8e'' UNION ALL SELECT ''8f''',
'UNION ALL SELECT ''90'' AS h UNION ALL SELECT ''91'' UNION ALL SELECT ''92'' UNION ALL SELECT ''93'' UNION ALL SELECT ''94'' UNION ALL SELECT ''95'' UNION ALL SELECT ''96'' UNION ALL SELECT ''97'' UNION ALL SELECT ''98'' UNION ALL SELECT ''99'' UNION ALL SELECT ''9a'' UNION ALL SELECT ''9b'' UNION ALL SELECT ''9c'' UNION ALL SELECT ''9d'' UNION ALL SELECT ''9e'' UNION ALL SELECT ''9f''',
'UNION ALL SELECT ''a0'' AS h UNION ALL SELECT ''a1'' UNION ALL SELECT ''a2'' UNION ALL SELECT ''a3'' UNION ALL SELECT ''a4'' UNION ALL SELECT ''a5'' UNION ALL SELECT ''a6'' UNION ALL SELECT ''a7'' UNION ALL SELECT ''a8'' UNION ALL SELECT ''a9'' UNION ALL SELECT ''aa'' UNION ALL SELECT ''ab'' UNION ALL SELECT ''ac'' UNION ALL SELECT ''ad'' UNION ALL SELECT ''ae'' UNION ALL SELECT ''af''',
'UNION ALL SELECT ''b0'' AS h UNION ALL SELECT ''b1'' UNION ALL SELECT ''b2'' UNION ALL SELECT ''b3'' UNION ALL SELECT ''b4'' UNION ALL SELECT ''b5'' UNION ALL SELECT ''b6'' UNION ALL SELECT ''b7'' UNION ALL SELECT ''b8'' UNION ALL SELECT ''b9'' UNION ALL SELECT ''ba'' UNION ALL SELECT ''bb'' UNION ALL SELECT ''bc'' UNION ALL SELECT ''bd'' UNION ALL SELECT ''be'' UNION ALL SELECT ''bf''',
'UNION ALL SELECT ''c0'' AS h UNION ALL SELECT ''c1'' UNION ALL SELECT ''c2'' UNION ALL SELECT ''c3'' UNION ALL SELECT ''c4'' UNION ALL SELECT ''c5'' UNION ALL SELECT ''c6'' UNION ALL SELECT ''c7'' UNION ALL SELECT ''c8'' UNION ALL SELECT ''c9'' UNION ALL SELECT ''ca'' UNION ALL SELECT ''cb'' UNION ALL SELECT ''cc'' UNION ALL SELECT ''cd'' UNION ALL SELECT ''ce'' UNION ALL SELECT ''cf''',
'UNION ALL SELECT ''d0'' AS h UNION ALL SELECT ''d1'' UNION ALL SELECT ''d2'' UNION ALL SELECT ''d3'' UNION ALL SELECT ''d4'' UNION ALL SELECT ''d5'' UNION ALL SELECT ''d6'' UNION ALL SELECT ''d7'' UNION ALL SELECT ''d8'' UNION ALL SELECT ''d9'' UNION ALL SELECT ''da'' UNION ALL SELECT ''db'' UNION ALL SELECT ''dc'' UNION ALL SELECT ''dd'' UNION ALL SELECT ''de'' UNION ALL SELECT ''df''',
'UNION ALL SELECT ''e0'' AS h UNION ALL SELECT ''e1'' UNION ALL SELECT ''e2'' UNION ALL SELECT ''e3'' UNION ALL SELECT ''e4'' UNION ALL SELECT ''e5'' UNION ALL SELECT ''e6'' UNION ALL SELECT ''e7'' UNION ALL SELECT ''e8'' UNION ALL SELECT ''e9'' UNION ALL SELECT ''ea'' UNION ALL SELECT ''eb'' UNION ALL SELECT ''ec'' UNION ALL SELECT ''ed'' UNION ALL SELECT ''ee'' UNION ALL SELECT ''ef''',
'UNION ALL SELECT ''f0'' AS h UNION ALL SELECT ''f1'' UNION ALL SELECT ''f2'' UNION ALL SELECT ''f3'' UNION ALL SELECT ''f4'' UNION ALL SELECT ''f5'' UNION ALL SELECT ''f6'' UNION ALL SELECT ''f7'' UNION ALL SELECT ''f8'' UNION ALL SELECT ''f9'' UNION ALL SELECT ''fa'' UNION ALL SELECT ''fb'' UNION ALL SELECT ''fc'' UNION ALL SELECT ''fd'' UNION ALL SELECT ''fe'' UNION ALL SELECT ''ff''');

-- 4 byte code points.
SET @union_4 = CONCAT(
  '(SELECT * FROM (SELECT ',
    ' HEX(CONVERT(UNHEX(CONCAT(t1.h, t2.h, t3.h, t4.h) USING @db_charset)) USING @native_charset) AS utf8_native_hex_value, ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h, t3.h, t4.h)) USING ', @db_charset, ') AS actual_char ',
    'FROM t AS t1 ',
    'LEFT JOIN t AS t2 ON 1=1 ',
    'LEFT JOIN t AS t3 ON 1=1 ',
    'LEFT JOIN t AS t4 ON 1=1 ',
    ') AS dt ',
    'WHERE CHAR_LENGTH(actual_char) <= 1 AND actual_char IS NOT NULL'
  ')'
);


-- 3 byte code points.
SET @union_3 = CONCAT(
  '(SELECT * FROM (SELECT ',
    ' HEX(CONVERT(UNHEX(CONCAT(t1.h, t2.h, t3.h)) USING @db_charset) USING @native_charset) AS utf8_native_hex_value, ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h, t3.h)) USING ', @db_charset, ') AS actual_char ',
    'FROM t AS t1 ',
    'LEFT JOIN t AS t2 ON 1=1 ',
    'LEFT JOIN t AS t3 ON 1=1 ',
    ') AS dt ',
    'WHERE CHAR_LENGTH(actual_char) <= 1 AND actual_char IS NOT NULL'
  ')'
);

-- 2 byte code points.
SET @union_2 = CONCAT(
  '(SELECT * FROM (SELECT ',
    ' HEX(CONVERT(UNHEX(CONCAT(t1.h, t2.h)) USING @db_charset) USING @native_charset) AS utf8_native_hex_value, ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h)) USING ', @db_charset, ') AS actual_char ',
    'FROM t AS t1 ',
    'LEFT JOIN t AS t2 ON 1=1 ',
    ') AS dt ', -- derived table
    'WHERE CHAR_LENGTH(actual_char) <= 1 AND actual_char IS NOT NULL'
  ')'
);

-- 1 byte code points.
SET @union_1 = CONCAT(
  '(SELECT * FROM (SELECT ',
    ' HEX(CONVERT(UNHEX(CONCAT(t1.h)) USING @db_charset) USING @native_charset) AS utf8_native_hex_value, ',
    'CONVERT(UNHEX(t1.h) USING ', @db_charset, ') AS actual_char ',
    'FROM t AS t1',
    ') AS dt ', -- derived table
    'WHERE CHAR_LENGTH(actual_char) <= 1 AND actual_char IS NOT NULL'
  ')'
);

-- All variable length code points.
SET @union = CONCAT(@union_3, ' UNION ALL ', @union_2, ' UNION ALL ', @union_1);

SET @inner_query=CONCAT(
 'SELECT DISTINCT ',
      'native_hex_value, actual_char,',
      'MIN(native_hex_value) OVER (PARTITION BY actual_char COLLATE ', @db_collation, ' ) AS equivalent_native_hex_value ',
      'FROM (',
        @union,
       ') As inner_query ',
       ' WHERE CHAR_LENGTH(actual_char) <= 1 AND actual_char IS NOT NULL ',
       ' ORDER BY actual_char COLLATE ', @db_collation, ' , native_hex_value '
);





SET @outer_query=CONCAT(
 'SELECT native_hex_value, actual_char, equivalent_native_hex_value, ',
    ' CONVERT(UNHEX(equivalent_native_hex_value) using ', @db_charset, ') as equivalent_char, ',
    ' CONV(native_hex_value, 16, 10) - CONV(equivalent_native_hex_value, 16, 10) as offset, ',
    'ROW_NUMBER() OVER (ORDER BY native_hex_value) as rn FROM (',
     @inner_query,
    ') AS outer_query '
);

SET @main_query=CONCAT(
' SELECT *, @offset_group := IF(@prev_offset = offset, @offset_group, @offset_group + 1) AS offset_group, @prev_offset := offset from (' ,
  @outer_query,
  ' ) as main_query',
  ' CROSS JOIN (SELECT @offset_group := 0, @prev_offset := -1) AS vars '
-- Initialize variables in cross join. Mysql nulls variables used in select clause even if they are initialized outside.
);

-- CONVERT(UNHEX(min(native_hex_value)) as start_native_hex_char,  CONVERT(UNHEX(max(native_hex_value)) as end_native_hex_char,

SET @sql = CONCAT(
  'WITH t AS (', @byte_literals,') ',
  ' SELECT ',
  'MIN(native_hex_value) as start_native_hex,',
  'MAX(native_hex_value) as end_native_hex, ',
  ' HEX(CONVERT(UNHEX(MIN(native_hex_value))) USING @native_charset) USING @db_charset) AS start_db_hex_value, ',
  ' HEX(CONVERT(UNHEX(MAX(native_hex_value))) USING @native_charset) USING @db_charset) AS end_db_hex_value, ',
  'CONVERT(UNHEX(MIN(native_hex_value)) USING ', @db_charset, ') AS start_char, ',
  'CONVERT(UNHEX(CONV(CONV(MIN(native_hex_value), 16, 10) - MIN(offset), 10, 16)) USING ', @db_charset, ') AS start_equivalent_char, ',
  'CONVERT(UNHEX(MAX(native_hex_value)) USING ', @db_charset, ') AS end_char, ',
  'CONVERT(UNHEX(CONV(CONV(MAX(native_hex_value), 16, 10) - MAX(offset), 10, 16)) USING ', @db_charset, ') AS end_equivalent_char, ',
  ' MIN(offset) as start_offset, ',
  ' MAX(offset) as end_offset, ',
  ' offset_group ',
  ' FROM (',
  @main_query,
  ') as group_query',
  ' GROUP BY offset_group ',
  ' ORDER BY ABS(offset_group), start_native_hex '
);
SELECT @sql; -- For debugging (Optional - comment out or remove before executing in MySQL)

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
