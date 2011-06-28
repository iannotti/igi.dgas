/* DGAS (DataGrid Accounting System)
 * definition of error codes.
// $Id: hlr_prot_errcode.h,v 1.1.2.1.4.2 2011/06/28 15:28:55 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
*/
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *             
 *  
 ***************************************************************************/
#ifndef hlr_prot_errcodes_h
#define hlr_prot_errcodes_h
#include <map>
#include <string>

using namespace std;

/*Following errors are 'int' */
#define E_GM_ALREADY_PRESENT	113 //OBSOLETED
#define E_GM_INSERT		114 //OBSOLETED
#define ACCESS_DENIED 		-1
/*UI : 5* */
#define INFO_NOT_FOUND 		"51"
#define E_UI_PARSE_ID 		"55"
/*BANK: 2* */
#define CREDIT_ERROR 		"21"
#define CREDIT_DUPL  		"22"
#define E_NO_USER 		"23"
#define E_DEBIT_ERROR_A 	"24"
#define E_BANK_PARSE_ID 	"25"
#define E_NO_RES_BANK_ID 	"26"
#define E_DEBIT_ERROR_B 	"27"
#define E_DEBIT_ERROR_C 	"28"
#define E_DEBIT_ERROR_D 	"29"
/*generic 1* */
#define E_MAXRETRY 		"11"
#define E_PARSE_ERROR 		"12"
#define E_NO_CONNECTION 	"13"
#define E_SEND_MESSAGE 		"14"
#define E_RECEIVE_MESSAGE 	"15"
#define E_NO_ATTRIBUTES		"16"
#define E_NO_DB			"17"
#define E_LOCK_OPEN		"18"
#define E_LOCK_REMOVE		"19"
#define E_SERVER_START		"110"
#define E_PARAM			"111"
#define E_FOPEN			"112"
#define E_HB_OPEN                 "113"
#define E_HB_GETTIME                 "114"
#define E_HB_GET                 "115"

/*PA: 3* */
#define PA_ERR_NO_PRICE 	"31"
#define PA_ERR_MDS_INFO 	"32"
#define PA_ERR_PUT_PRICE 	"33"
#define PA_ERR_RES_INIT 	"34"
#define E_PA_PARSE_ID 		"35"
#define PA_ERR_DL_OPEN          "36"
#define PA_ERR_DL_SYMBOL        "37"
#define E_NO_PA       		"38"

/*jobAuth 4* */
#define JOB_DUPL  		"41"
#define E_JA_PARSE_ID 		"45"

/*ATM 6* */
#define ATM_E_AUTH 		"61"
#define ATM_E_PRICE 		"62"
#define ATM_E_COST 		"63"
#define ATM_E_TRANS 		"64"
#define ATM_E_DUPLICATED 	"65"
#define E_WRONG_RES_HLR		"66"
#define ATM_E_TRANS_A 		"67"
#define ATM_E_TRANS_B 		"68"
#define ATM_E_TRANS_C 		"69"
#define ATM_E_DUPLICATED_A 	"70"
#define ATM_E_DUPLICATED_B 	"71"
#define ATM_E_DUPLICATED_C 	"72"
#define ATM_E_DUPLICATED_D 	"73"
#define E_STRICT_AUTH		"74"

/*RUI 7* */
#define E_RUI_PARSE_ID          "75"

/*PING 8* */
#define E_PING_PARSE_ID		"85"

#define SUCCESFULL_EXIT		"0"

#define E_FUNCT_OBSOLETED	"99"

class hlrError {
	public:
		hlrError()
		{
			error.insert(mval::value_type (
				SUCCESFULL_EXIT,
				"Success."));
			error.insert(mval::value_type (
				INFO_NOT_FOUND,
				"Info not found in the database."));
			error.insert(mval::value_type (
				E_UI_PARSE_ID,
				"Error parsing the ID."));
			error.insert(mval::value_type (
				CREDIT_ERROR,
				"bankEngine: error crediting the account."));
			error.insert(mval::value_type (
				CREDIT_DUPL,
				"bankEngine: the transctioin is already present in the database."));
			error.insert(mval::value_type (
				E_NO_USER,
				"bankEngine: the account doesn't exists in the database."));
			error.insert(mval::value_type (
				E_DEBIT_ERROR_A,
				"bankEngine: Error Inserting record. Error parsing record log data contained in trans_queue record."));
			error.insert(mval::value_type (
				E_DEBIT_ERROR_B,
				"bankEngine: Error Inserting record. Error in hlrTransaction.process()"));
			error.insert(mval::value_type (
				E_DEBIT_ERROR_C,
				"bankEngine: Error Inserting record. Error inseritng record in transInLog, trans_in rollback failed."));
			error.insert(mval::value_type (
				E_DEBIT_ERROR_D,
				"bankEngine: Error Inserting record. Error inseritn record in transInLog, trans_in rollback succeded."));
			error.insert(mval::value_type (
				E_BANK_PARSE_ID,
				"bankEngine: Error parsing the hlr location."));
			error.insert(mval::value_type (
				E_NO_RES_BANK_ID,
				"bankEngine: remote HLR not specified"));
			error.insert(mval::value_type (
				E_MAXRETRY,
				"generic: the operation has exceeded the maximum nuber of retrials."));
			error.insert(mval::value_type (
				E_PARSE_ERROR,
				"generic: Error parsing the message."));
			error.insert(mval::value_type (
				E_NO_CONNECTION,
				"generic: error trying to contact the server."));
			error.insert(mval::value_type (
				E_SEND_MESSAGE,
				"generic: connection opened, but error sending message through the socket."));
			error.insert(mval::value_type (
				E_RECEIVE_MESSAGE,
				"generic: connection opened, but error receiving message from socket."));
			error.insert(mval::value_type (
				E_NO_ATTRIBUTES,
				"generic: E_NO_ATTRIBUTES"));
			error.insert(mval::value_type (
				E_NO_DB,
				"generic: databse not found, may be mysql is down or the DB structure is not present."));
			error.insert(mval::value_type (
				E_LOCK_OPEN,
				"generic: could not open the service lock file."));
			error.insert(mval::value_type (
				E_LOCK_REMOVE,
				"generic: could not remove the lock file."));
			error.insert(mval::value_type (
				E_SERVER_START,
				"generic: Error starting the listening server."));
			error.insert(mval::value_type (
				E_PARAM,
				"generic: Error in the parameters set."));
			error.insert(mval::value_type (
				E_FOPEN,
				"generic: Error opening the file."));
			error.insert(mval::value_type (
				E_HB_OPEN,
				"generic: Error opening the file heart beat file."));
			error.insert(mval::value_type (
				E_HB_GETTIME,
				"generic: Error retrieving current timestamp for heart beat."));
			error.insert(mval::value_type (
				E_HB_GET,
				"generic: Error getting heart beat info from file."));
			error.insert(mval::value_type (
				PA_ERR_NO_PRICE,
				"price authority: Could not find the requesteed price."));
			error.insert(mval::value_type (
				PA_ERR_MDS_INFO,
				"price authority: Could not retrieve information needed to compute the price."));
			error.insert(mval::value_type (
				PA_ERR_PUT_PRICE,
				"price authority: Could not insert the price in the databse."));
			error.insert(mval::value_type (
				PA_ERR_RES_INIT,
				"price authority: Could not create an entry for the requested resource in the database."));
			error.insert(mval::value_type (
				E_PA_PARSE_ID,
				"price authority: Could not parse the PA location string."));
			error.insert(mval::value_type (
				PA_ERR_DL_OPEN,
				"price authority: Could not load the plugin."));
			error.insert(mval::value_type (
				PA_ERR_DL_SYMBOL,
				"price authority: Could not find required symbol in the plugin library."));
			error.insert(mval::value_type (
				E_NO_PA,
				"price authority: oops, it seems there's not such Price Authority."));
			error.insert(mval::value_type (
				JOB_DUPL,
				"jobAuth engine: the job is registered already"));
			error.insert(mval::value_type (
				E_JA_PARSE_ID,
				"jobAuth: Error parsing the ID."));
			error.insert(mval::value_type (
				ATM_E_AUTH,
				"atmEngine: the job accounting is not authorised."));
			error.insert(mval::value_type (
				ATM_E_PRICE,
				"atmEngine: Could not find resource Price."));
			error.insert(mval::value_type (
				ATM_E_COST,
				"atmEngine: Could not compute job cost."));
			error.insert(mval::value_type (
				ATM_E_TRANS,
				"atmEngine: Error inserting transaction in trans_queue table."));
			error.insert(mval::value_type (
				ATM_E_TRANS_A,
				"atmResourceEngine: Error Declaring the transaction obsolete. Notify the problem to the _resource_ HLR administrator."));
			error.insert(mval::value_type (
				ATM_E_TRANS_B,
				"atmResourceEngine: Error inserting the transaction in hlr trans_queue. The trans_queue table is not present or mysql is down on the resource HLR. Notify the administrator."));
			error.insert(mval::value_type (
				ATM_E_TRANS_C,
				"atmiResourceEngine: Another instance for this transaction is waiting to be processed on the resource HLR trans_queue."));
			error.insert(mval::value_type (
				ATM_E_DUPLICATED,
				"atmEngine: This record is already present in the HLR database!."));
			error.insert(mval::value_type (
				ATM_E_DUPLICATED_A,
				"atmResourceEngine: Request to insert an UR with outOfBand procedure, but more accurate record for the same job has been inserted by another sensor. This can happen for instance if dgas OutOfBand runs after a job has been accounted by dgas gianduia or urCollector. In such cases this is not an error and is perfectly Normal."));
			error.insert(mval::value_type (
				ATM_E_DUPLICATED_B,
				"atmResourceEngine: It has been requested to insert an UR which is already present in the database. This can happen if dgas outOfBand is being run two times over the same jobs."));
			error.insert(mval::value_type (
				ATM_E_DUPLICATED_C,
				"atmResourceEngine: Transaction Exists in the database but was impossble to retrieve more info on it. This Should never happen. Notify the problem to the _resource_ HLR administrator specifying the jobId."));
			error.insert(mval::value_type (
				ATM_E_DUPLICATED_D,
				"atmResourceEngine: While checking for re-submission, The transaction is already present in the database."));
			error.insert(mval::value_type (
				E_STRICT_AUTH,
				"atmEngine: The resoure is not registered on the HLR with the correct DN, or the _resource_ account specified in this UR does not exists in the specified resource HLR. Contact the HLR system Manager."));
			error.insert(mval::value_type (
				E_WRONG_RES_HLR,
				"atmEngine: Wrong resource HLR specified in the CE conf file."));
			error.insert(mval::value_type (
				E_RUI_PARSE_ID,
				"userInterface: Error parsing the ID."));
			error.insert(mval::value_type (
				E_PING_PARSE_ID,
				"ping: Error parsing the ID."));
			error.insert(mval::value_type (
				E_FUNCT_OBSOLETED,
				"DGAS: The functionality requested is obsoleted and no more available in the current version of the code."));
		}
	typedef map<string,string> mval;
	mval error;
};

#endif
