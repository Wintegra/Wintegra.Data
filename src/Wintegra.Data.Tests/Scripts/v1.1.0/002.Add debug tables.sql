-- String data types
CREATE TABLE "DBG_TABLE_CHARACTER"  ( 
        -- 	Fixed-length character strings with a length of n bytes. 
        -- n must be greater than 0 and not greater than 255. The default length is 1.
        "FIELD"     CHARACTER(254),
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_VARCHAR"  ( 
        -- 	Varying-length character strings with a maximum length of n bytes. 
        -- n must be greater than 0 and less than a number that depends on 
        --   the page size of the table space. The maximum length is 32704.
        "FIELD"     VARCHAR(2048), 
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_CLOB"  ( 
        -- 	Varying-length character strings with a maximum of n characters.
        --  n cannot exceed 2 147 483 647. The default length is 1M.
        "FIELD"     CLOB(2147483647) LOGGED NOT COMPACT, 
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_GRAPHIC"  ( 
        -- Fixed-length graphic strings that contain n double-byte characters.
        -- n must be greater than 0 and less than 128. The default length is 1.
        "FIELD"     GRAPHIC(127),
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_VARGRAPHIC"  ( 
        -- Varying-length graphic strings. 
        -- The maximum length, n, must be greater than 0 and less than a number that depends on 
        --  the page size of the table space. The maximum length is 16352.
        "FIELD"     VARGRAPHIC(1024),
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_DBCLOB"  ( 
        -- Varying-length strings of double-byte characters with a maximum of n double-byte characters. 
        -- n cannot exceed 1 073 741 824. The default length is 1M.
        "FIELD"     DBCLOB(10485760) LOGGED NOT COMPACT,
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_BLOB"  ( 
        -- Varying-length binary strings with a length of n bytes. 
        -- n cannot exceed 2 147 483 647. The default length is 1M.
        "FIELD"     BLOB(2147483647) LOGGED NOT COMPACT,
        "EMPTY"     CHARACTER(1)
)
GO
-- Numeric data types
CREATE TABLE "DBG_TABLE_SMALLINT"  ( 
        -- Small integers. 
        -- A small integer is binary integer with a precision of 15 bits. 
        -- The range is -32768 to +32767.
        "FIELD"     SMALLINT,
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_INTEGER"  ( 
        -- Large integers. 
        -- A large integer is binary integer with a precision of 31 bits. 
        -- The range is -2147483648 to +2147483647.
        "FIELD"     INTEGER,
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_BIGINT"  ( 
        -- Big integers. 
        -- A big integer is a binary integer with a precision of 63 bits. 
        -- The range of big integers is -9223372036854775808 to +9223372036854775807.
        "FIELD"     BIGINT,
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_DECIMAL"  ( 
        -- A decimal number is a packed decimal number with an implicit decimal point. 
        -- The position of the decimal point is determined by the precision and the scale of the number. 
        -- The scale, which is the number of digits in the fractional part of the number, 
        --  cannot be negative or greater than the precision. The maximum precision is 31 digits.
        -- All values of a decimal column have the same precision and scale. 
        -- The range of a decimal variable or the numbers in a decimal column is -n to +n, 
        --  where n is the largest positive number that can be represented with the applicable precision and scale. 
        -- The maximum range is 1 - 10³¹ to 10³¹ - 1.
        "FIELD"     DECIMAL(31,7),
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_DECFLOAT"  ( 
        -- A decimal floating-point value is an IEEE 754r number with a decimal point. 
        -- The position of the decimal point is stored in each decimal floating-point value. 
        -- The maximum precision is 34 digits.
        -- The range of a decimal floating-point number is either 16 or 34 digits of precision; 
        --  the exponent range is respectively 10-383 to 10+384 or 10-6143 to 10+6144.
        "FIELD"     DECFLOAT(34),
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_REAL"  ( 
        -- A single-precision floating-point number is a short floating-point number of 32 bits. 
        -- The range of single-precision floating-point numbers is approximately -7.2E+75 to 7.2E+75. 
        -- In this range, the largest negative value is about -5.4E-79, and the smallest positive value is about 5.4E-079.
        "FIELD"     REAL,
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_DOUBLE"  ( 
        -- A double-precision floating-point number is a long floating-point number of 64-bits. 
        -- The range of double-precision floating-point numbers is approximately -7.2E+75 to 7.2E+75. 
        -- In this range, the largest negative value is about -5.4E-79, and the smallest positive value is about 5.4E-79.
        "FIELD"     DOUBLE,
        "EMPTY"     CHARACTER(1)
)
GO
-- Date, time, and timestamp data types
CREATE TABLE "DBG_TABLE_DATE"  ( 
        -- A date is a three-part value representing a year, month, and day in the range of 0001-01-01 to 9999-12-31.
        "FIELD"     DATE,
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_TIME"  ( 
        -- A time is a three-part value representing a time of day in hours, minutes, and seconds, in the range of 00.00.00 to 24.00.00.
        "FIELD"     TIME,
        "EMPTY"     CHARACTER(1)
)
GO
CREATE TABLE "DBG_TABLE_TIMESTAMP"  ( 
        -- A timestamp is a seven-part value representing a date and time by year, month, day, hour, minute, second, and microsecond, 
        --  in the range of 0001-01-01-00.00.00.000000000 to 9999-12-31-24.00.00.000000000 with nanosecond precision. 
        -- Timestamps can also hold timezone information.
        "FIELD"     TIMESTAMP,
        "EMPTY"     CHARACTER(1)
)
GO
-- XML data type
CREATE TABLE "DBG_TABLE_XML"  ( 
        -- The size of an XML value in a DB2® table has no architectural limit. 
        -- However, serialized XML data that is stored in or retrieved from an XML column is limited to 2 GB.
        "FIELD"     XML,
        "EMPTY"     CHARACTER(1)
)
GO