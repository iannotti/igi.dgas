// $Id: libPA_comm.h,v 1.1.2.1.4.1 2010/10/19 09:00:16 aguarise Exp $
// -------------------------------------------------------------------------
// Copyright (c) 2001-2002, The DataGrid project, INFN, 
// All rights reserved. See LICENSE file for details.
// -------------------------------------------------------------------------
// Author: Andrea Guarise <andrea.guarise@to.infn.it>
/***************************************************************************
 * Code borrowed from:
 *  authors   : 
 *             
 *  
 ***************************************************************************/

#ifndef PA_COMM_H
#define PA_COMM_H

using namespace std;

struct resource{
  string resID;
  int d_minTTL;
};

struct price {
  string resID;
  int time;
  int priceValue;
  int minTTL;
};


#endif
