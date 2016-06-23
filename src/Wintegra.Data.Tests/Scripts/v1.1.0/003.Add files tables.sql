CREATE TABLE "FILE_ENTRY"  (
	"ID"				VARGRAPHIC(32) NOT NULL,
	"NO"				BIGINT NOT NULL,
	"PACK_ID"		VARGRAPHIC(32) NOT NULL,
	"INCOME"			TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,

	"FNAME"			VARGRAPHIC(128) NOT NULL,
	"LNAME"			VARGRAPHIC(128) NOT NULL,
	"MNAME"			VARGRAPHIC(128),
	"BDATE"			VARGRAPHIC(128) NOT NULL,

	"ADDRESS"		DBCLOB(10485760) LOGGED NOT COMPACT,
	"CLOSE_DATE"		BIGINT NOT NULL,
	"CATEGORY_ID"	BIGINT NOT NULL,
	CONSTRAINT "FILE_ENTRY" PRIMARY KEY("ID")
)
GO

CREATE INDEX FK_FILE_ENTRY_SEARCH ON FILE_ENTRY (PACK_ID ASC, ID ASC)
GO

CREATE INDEX FK_FILE_ENTRY_SORT ON FILE_ENTRY (PACK_ID ASC, "NO" ASC)
GO

CREATE TABLE "PACK_ENTRY" (
	"ID"				VARGRAPHIC(32) NOT NULL,
	"INCOME"			TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,

	"CLOSE_DATE"		BIGINT NOT NULL,
	"CATEGORY_ID"	BIGINT NOT NULL,

	"GU_CODE"		BIGINT NOT NULL,
	"NO"				BIGINT NOT NULL,

	CONSTRAINT "PACK_ENTRY" PRIMARY KEY("ID")
)
GO

ALTER TABLE FILE_ENTRY
	ADD COLUMN SNILS VARGRAPHIC(30)
GO

CREATE TABLE "CATEGORY_LIST"  ( 
	 "ID"         BIGINT NOT NULL,
	 "SHORTNAME"  VARGRAPHIC(128) NOT NULL,
	 "NAME"       DBCLOB(10485760) LOGGED NOT COMPACT,
	 CONSTRAINT "PK_CATEGORY_LIST" PRIMARY KEY("ID")
)
GO

INSERT INTO CATEGORY_LIST(ID, SHORTNAME, NAME) VALUES 
	(1, 'По старости', 'Получатели пенсии по старости страховой пенсии и пенсии по государственному пенсионному обеспечению'),
	(2, 'По инвалидности', 'Получатели пенсии по инвалидности страховой пенсии и пенсии по государственному пенсионному обеспечению'),
	(3, 'По СПК', 'Получатели пенсии по случаю потери кормильца страховой пенсии и пенсии по государственному пенсионному обеспечению'),
	(4, 'Дети инвалиды', 'Дети инвалиды и инвалиды с детства'),
	(5, 'ЕВД', 'Выплатные дела получателей ЕДВ')
GO