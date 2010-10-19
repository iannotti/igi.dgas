// DGAS (DataGrid Accounting System) 
// Server Daeomn and protocol engines.
// 
// $Id: xmlHlrHelper.h,v 1.1.2.1.4.1 2010/10/19 09:11:05 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
 /***************************************************************************
 * Code borrowed from:
 *  authors   : Apache Software Fundation
 *  copyright : (C) 1999 Apache Software Fundation
 ***************************************************************************/
//
//    

// ---------------------------------------------------------------------------
//  Includes
// ---------------------------------------------------------------------------
#ifndef XML_HELPER
#define XML_HELPER
#include <string>
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/comm_struct.h"

using namespace std;

// global definitions




int parse_xml( string &xml_input, connInfo &connectionInfo, listenerStatus& lStatus ,string *output);

#endif
