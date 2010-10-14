CREATE TABLE SumCPU (
	ExecutingSite	varchar(50)	NOT NULL default '',
	LCGUserVO	varchar(255)	NOT NULL default '',
	Njobs		int(11)		default NULL,
	SumCPU		decimal(10,0)	default NULL,
	NormSumCPU	decimal(10,0)	default NULL,
	SumWCT		decimal(10,0)	default NULL,
	NormSumWCT	decimal(10,0)	default NULL,
	Month		int(11)		NOT NULL default 0,
	Year		int(11)		NOT NULL default 0,
	RecordStart	date		default NULL,
	RecordEnd	date		default NULL,
	PRIMARY KEY (ExecutingSite,LCGUserVO,Month,Year)
) TYPE=MyISAM;