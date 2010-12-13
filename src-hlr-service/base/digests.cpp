// $Id: digests.cpp,v 1.1.2.1 2010/12/13 09:56:58 aguarise Exp $

#include "glite/dgas/common/base/digests.h"

string md5sum(string &input_st)
{
  MD5_CTX md5;

  string st_in(input_st);
  unsigned char * out_vec;
  ostringstream out_st;

  out_vec = new unsigned char [MD5_DIGEST_LENGTH];

  MD5_Init(&md5);
  MD5_Update(&md5, (void *) st_in.c_str(), st_in.size());
  MD5_Final(out_vec, &md5);
 
  for (int i=0; i < MD5_DIGEST_LENGTH; i++)
    {   
      out_st.fill('0');     
      out_st.width(2);
      out_st << hex << (int) out_vec[i];
    }
  delete [] out_vec;
  
  string digest = out_st.str();
  return digest;
}



string sha1sum(string &input_st)
{
  SHA_CTX sha;

  string st_in(input_st);
  unsigned char * out_vec;
  ostringstream out_st;

  out_vec = new unsigned char [SHA_DIGEST_LENGTH];

  SHA1_Init(&sha);
  SHA1_Update(&sha, (void *) st_in.c_str(), st_in.size());
  SHA1_Final(out_vec, &sha);
 
  for (int i=0; i < SHA_DIGEST_LENGTH; i++)
    {
      out_st.fill('0');      // fill with 0 the empty fields
      out_st.width(2);       // fields are large 2 units
      out_st << hex << (unsigned int) out_vec[i];
    }
  delete [] out_vec;
  
  string digest = out_st.str();
  return digest;
}
