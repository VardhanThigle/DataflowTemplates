-- In this query, at a high level we sort a sequence of variable length code points as per the collation order of the database to understand how the code points for characters compare for the given collation.
-- We start with all possible variable length code points which represent a single characters, sort them by the collation order and condense the output to groups.
-- More details are explained in the query taht follows.
-- Note that although the query appears big it completes in a time < 5 seconds as the db just has to do in-memory sorting of literals.

-- Unfortunately we can't use prepared statement to set variable values.
-- The dataflow code will replace these with the actual db charset and collation.
-- SET @db_charset = 'charset_replacement_tag';
-- SET @db_collation = 'collation_replacement_tag';

SET @db_charset = 'utf8mb4';
SET @db_collation = 'utf8mb4_0900_ai_ci';


-- Any charset natively supported by java.
-- While reading strings from the db, irrespective of the db character set, jdbc always converts the string to java language native string which supports entire unicode space out of box.
-- We need to map every character of such a string to code-point representation for generating split ranges.
-- We also need to know how these code points compare for the given @db_charset,@db_collation, which the query below helps us with.
-- For the purpose of mapping a character to a code point, java.nio.charset requires all java implementations to support conversion to a few character sets like utf-8
-- Ref: https://docs.oracle.com/javase/8/docs/api/java/nio/charset/Charset.html
-- utf8 is one of the charsets all java implementations are required to support.
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
'UNION ALL SELECT ''f0'' AS h UNION ALL SELECT ''f1'' UNION ALL SELECT ''f2'' UNION ALL SELECT ''f3'' UNION ALL SELECT ''f4'' UNION ALL SELECT ''f5'' UNION ALL SELECT ''f6'' UNION ALL SELECT ''f7'' UNION ALL SELECT ''f8'' UNION ALL SELECT ''f9'' UNION ALL SELECT ''fa'' UNION ALL SELECT ''fb'' UNION ALL SELECT ''fc'' UNION ALL SELECT ''fd'' UNION ALL SELECT ''fe'' UNION ALL SELECT ''ff'''
);

SET @four_byte_codepoints = CONCAT(
  '(SELECT * FROM (SELECT ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h, t3.h, t4.h)) USING ', @db_charset, ') AS charset_char ',
    'FROM (', @byte_literals, ') AS t1 ',
    'LEFT JOIN (', @byte_literals, ') AS t2 ON 1=1 ',
    'LEFT JOIN (', @byte_literals, ') AS t3 ON 1=1 ',
    'LEFT JOIN (', @byte_literals, ') AS t4 ON 1=1 ',
    ') AS dt ',
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

SET @three_byte_codepoints = CONCAT(
  '(SELECT * FROM (SELECT ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h, t3.h)) USING ', @db_charset, ') AS charset_char ',
    'FROM (', @byte_literals, ') AS t1 ',
    'LEFT JOIN (', @byte_literals, ') AS t2 ON 1=1 ',
    'LEFT JOIN (', @byte_literals, ') AS t3 ON 1=1 ',
    ') AS dt ',
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

SET @two_byte_codepoints = CONCAT(
  '(SELECT * FROM (SELECT ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h)) USING ', @db_charset, ') AS charset_char ',
    'FROM (', @byte_literals, ') AS t1 ',
    'LEFT JOIN (', @byte_literals, ') AS t2 ON 1=1 ',
    ') AS dt ',
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

SET @one_byte_codepoints = CONCAT(
  '(SELECT * FROM (SELECT ',
    'CONVERT(UNHEX(t1.h) USING ', @db_charset, ') AS charset_char ',
    'FROM (', @byte_literals, ') AS t1',
    ') AS dt ', -- derived table
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

-- all variable length code points representing a single character within the @db_charset from length 0 till 4.
SET @codepoints = CONCAT(@three_byte_codepoints, ' UNION ALL ', @two_byte_codepoints, ' UNION ALL ', @one_byte_codepoints);

-- We use native_codepoint to map character of the string to a java code-point.
-- equivalent_native_codepoint is the minimum code point which is equal to a given character for the given @db_collation.
-- For example in any case insensitive collation, the equivalent_native_codepoint for 'a' will be the native_codepoint for 'A'
-- Refer to the comment on @native_charset above for more details.
SET @find_equivalents_query=CONCAT(
 'SELECT DISTINCT ',
      'CONV(HEX(CONVERT(charset_char using ', @native_charset ,')), 16, 10) as native_codepoint, charset_char,',
      'MIN(CAST(CONV(HEX(CONVERT(charset_char using ', @native_charset ,')), 16, 10) AS UNSIGNED)) OVER (PARTITION BY charset_char COLLATE ', @db_collation, ' ORDER BY charset_char  COLLATE ', @db_collation,'  ) AS equivalent_native_codepoint ',
      'FROM (',
        @codepoints,
       ') As find_equivalents ',
       ' WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL ',
       ' ORDER BY CAST(native_codepoint as UNSIGNED) ASC'
);

-- We find offset of each character from the equivalent_character
-- For example in all case insensitive collations, the offset for 'a' will be 32 (as it's equal to 'A') and for 'A' will be 0.
-- Similarly variants of A/a with accents will map to A if the collation is both case and accent insensitive.
SET @find_offsets_query=CONCAT(
 'SELECT native_codepoint, charset_char, equivalent_native_codepoint, ',
    ' CAST(native_codepoint AS UNSIGNED) - CAST(equivalent_native_codepoint AS UNSIGNED) as offset , ',
    ' DENSE_RANK() OVER (ORDER BY CAST(equivalent_native_codepoint AS UNSIGNED)) - 1 as codepoint_idx, ',
    ' ROW_NUMBER() OVER (ORDER BY CAST(native_codepoint AS UNSIGNED)) as rn FROM ( ',
     @find_equivalents_query,
    ' ) AS find_offsets ',
    ' ORDER BY CAST(native_codepoint AS UNSIGNED) ASC '
);


-- We group the offsets for continuous set of characters, this helps us condense the output.
-- If we were to exchange, an entire collation table and use it as sideinput, it will have more than 1.1 million entries.
-- Building on the previous example, all characters from 'a' to 'z' map to either 'A' to 'Z' or themselves('a' to 'z') depending on whether the collation is case sensetive or not.
-- So all characters from 'a' to 'z' will have fall within the same offset_group (with offset either 32 or 0)
SET @group_offsets_query=CONCAT(
' SELECT *, @offset_group := IF(rn = 1, 0, IF(@prev_offset = offset, @offset_group, @offset_group + 1)) AS offset_group, @prev_offset := offset from (' ,
  @find_offsets_query,
  ' ) as main_query',
  ' ORDER BY CAST(native_codepoint AS UNSIGNED) ASC'
);

-- Initialize variables in cross join. Mysql nulls variables used in select clause even if they are initialized outside.

-- Gets the start and end of the groups.
-- the start_codepoint_idx or end_codepoint_idx could be replaced by STRING_WEIGHT() in mysql though it is a debug only function and there is no equivalent in PG and ORCA.
-- ' WITH t AS (', @byte_literals,') ',
SET @grouped_output = CONCAT(
  ' SELECT ',
  ' MIN(CAST(native_codepoint AS UNSIGNED)) as start_native_codepoint,',
  ' MAX(CAST(native_codepoint AS UNSIGNED)) as end_native_codepoint, ',
  ' offset_group ',
  ' FROM (',
  @group_offsets_query,
  ') as group_query',
  ' GROUP BY offset_group ',
  ' ORDER BY ABS(offset_group), start_native_codepoint '
);

SET @grouped_output_with_codepoint_idx = CONCAT(
 ' SELECT ',
 '  grouped_output.*, ',
 '  start_match.offset as start_offset, ',
 '  end_match.offset as end_offset, ',
 '  start_match.equivalent_native_codepoint as start_equivalent_native_codepoint, ',
 '  end_match.equivalent_native_codepoint as end_equivalent_native_codepoint, ',
 '  start_match.codepoint_idx AS start_codepoint_idx, ',
 '  end_match.codepoint_idx AS end_codepoint_idx, ',
 '  start_match.charset_char AS start_charset_char, ',
 '  end_match.charset_char AS end_charset_char ',
 ' FROM ( ',
    @grouped_output,
 ' ) AS grouped_output '
 ' INNER JOIN ( ',
    @group_offsets_query,
 ' ) AS start_match ',
 '  ON grouped_output.start_native_codepoint = start_match.native_codepoint ',
 ' INNER JOIN ( ',
    @group_offsets_query,
 ' ) AS end_match ',
 '  ON grouped_output.end_native_codepoint = end_match.native_codepoint ',
 ' ORDER BY ABS(grouped_output.offset_group), grouped_output.start_native_codepoint'
);




-- Here we just get the start and end of each group.
-- We need only few columns from the final query, but for (manual/test) verification, we are adding more.
-- Adding more derived columns does not really add any overhead to the query optimization.
SET @condensed_output_query = CONCAT(
  ' SELECT *, ',
  '             CONVERT(UNHEX(CONV(start_equivalent_native_codepoint, 10, 16)) USING ', @native_charset, ') AS start_equivalent_char, ',
  '             CONVERT(UNHEX(CONV(end_equivalent_native_codepoint, 10,16)) USING ', @native_charset, ') AS end_equivalent_char, ',
  ' HEX(CONVERT(CONVERT(UNHEX(CONV(start_native_codepoint, 10 , 16)) USING ', @native_charset, ') USING ', @db_charset ,')) AS start_db_codepoint_hex, ',
  ' HEX(CONVERT(CONVERT(UNHEX(CONV(end_native_codepoint, 10, 16)) USING ', @native_charset, ') USING ', @db_charset ,')) AS end_db_codepoint_hex ',
  ' FROM (',
    @grouped_output_with_codepoint_idx,
  ' ) as condensed_output_query '
);

PREPARE stmt FROM @condensed_output_query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
