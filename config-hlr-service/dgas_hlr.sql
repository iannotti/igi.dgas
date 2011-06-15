# MySQL dump 8.14
#
# Host: localhost    Database: hlr
#--------------------------------------------------------
# Server version	3.23.38


CREATE TABLE trans_in (
  tid bigint(20) unsigned NOT NULL auto_increment,
  rid varchar(128) default NULL,
  gid varchar(128) default NULL,
  from_dn varchar(160) default NULL,
  amount smallint(5) unsigned default NULL,
  tr_stamp int(10) unsigned default NULL,   
  dgJobId varchar(160) default NULL,
  uniqueChecksum char(32) default NULL,
  accountingProcedure varchar(32) default NULL,
  PRIMARY KEY  (tid), key (rid), key (dgJobId), key (uniqueChecksum)
) ENGINE=MyISAM;

CREATE TABLE transInLog (
  dgJobId varchar(160) NOT NULL default '',
  log blob NOT NULL,
  PRIMARY KEY  (dgJobId)
) ENGINE=MyISAM;

# Table structure for table 'acctdesc'
#

CREATE TABLE acctdesc (
  id varchar(128) NOT NULL default '',
  email varchar(128) default NULL,
  descr varchar(128) default NULL,
  ceId varchar(160) default NULL,
  acl blob default NULL,
  gid varchar(128) default NULL,
  PRIMARY KEY  (id)
) ENGINE=MyISAM;


CREATE TABLE hlrAdmin (
  acl blob NOT NULL,
  PRIMARY KEY (acl(255))
) ENGINE=MyISAM;


# deprecated
CREATE TABLE voAdmin (
  vo_id varchar(160) NOT NULL,
  acl blob NOT NULL,
  PRIMARY KEY (vo_id,acl(255))
) ENGINE=MyISAM;

