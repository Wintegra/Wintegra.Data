CREATE TABLE "HEAD" (
	"ID"					VARGRAPHIC(32) NOT NULL,
	"NOTE"				VARGRAPHIC(128),
	CONSTRAINT "HEAD" PRIMARY KEY(ID)
)
GO

CREATE TABLE "LINE" (
	"ID"					VARGRAPHIC(32) NOT NULL,
	"HEAD_ID"			VARGRAPHIC(32) NOT NULL,
	"NOTE"				VARGRAPHIC(128),
	CONSTRAINT "line" PRIMARY KEY(ID)
)
GO

ALTER TABLE "LINE"
	ADD CONSTRAINT INX_LINE_HEAD
	FOREIGN KEY(HEAD_ID)
	REFERENCES HEAD(ID)
	ON DELETE NO ACTION 
	ON UPDATE NO ACTION 
GO