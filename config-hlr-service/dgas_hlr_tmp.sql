# MySQL dump 8.14
#
# Host: localhost    Database: hlr_tmp
#--------------------------------------------------------
# Server version	3.23.41-log

#
# Table structure for table 'trans_queue'
#

CREATE TABLE trans_queue (
  transaction_id varchar(160) NOT NULL default '',
  from_cert_subject blob NOT NULL default '',
  to_cert_subject blob NOT NULL default '',
  from_hlr_url varchar(160) NOT NULL default '',
  to_hlr_url varchar(160) NOT NULL default '',
  amount int(11) NOT NULL default '0',
  timestamp varchar(12) NOT NULL default '',
  log_data blob NOT NULL,
  priority int(11) default NULL,
  status_time int(11) default NULL,
  type varchar(32) NOT NULL default '',
  PRIMARY KEY  (transaction_id)
) ENGINE=MyISAM;

#
# Dumping data for table 'trans_queue'
#


