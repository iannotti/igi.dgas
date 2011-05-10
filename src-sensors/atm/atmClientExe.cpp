#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/hlr-sensors/atm/atmClient.h"

#define OPTION_STRING "3hv:j:t:p:l:C:U:"

using namespace std;

bool needs_help = 0;

string dgJobId_buff = "NULL"; // DgJobId of the current job
string timeStamp_buff = "0"; // Submission time of the current job
string res_acct_PA_id_buff = ""; // Price authority for this CE (URL)
string res_acct_bank_id_buff = ""; // HLR for this CE (URL)
string resX509cert_buff = "NULL"; // X509 subject of this CE
string usrX509cert_buff = ""; // X509 subject of the user owning the job
int verbosity = 3;
vector<string> info_v;

string cpu_time = "0";
string wall_time = "0";
string mem = "0";
string vmem = "0";


void help()
{
	cerr << "DGAS ATM client" << endl;
	cerr << "Author: Andrea Guarise <andrea.guarise@to.infn.it>" << endl;
	cerr << "Version:" << ATM_CLIENT_VERSION << VERSION << endl;
	cerr << "Usage:" << endl;
	cerr << "ATM_client <OPTIONS> [USAGE RECORD LIST]" << endl;
	cerr << "Where options are:" << endl;
	cerr << "-h  --help                       Display this help and exit." << endl;
	cerr << "-v  --verbosity <verbosity>      (0-3) default 3 maximum verbosity" << endl;
	cerr << "-j  --jobid <jobId>              Global job ID." << endl;
	cerr << "-t  --time <timestamp>           Submission time of the job." << endl;
	cerr << "-p  --paid <paID>                Contact string of the CE's Price authority." << endl;
	cerr << "-l  --localbankid <HLR contact>  Contact string of the (local) Resource HLR." << endl;
	cerr << "-C  --resgridid <ceID>           Global CE ID" << endl;
	cerr << "-U  --usrcert <cert_subject>     User's X509 certificate subject" << endl;
	cerr << endl;
	cerr << "The HLR an PA contact strings have the form: \"host:port:host_cert_subject\"." << endl;
	cerr << "The timestamp is specified in seconds since Jan. 1, 1970 0:00:00 GMT." << endl;
	cerr << endl;
	cerr << "[USAGE RECORD LIST]:" << endl;
	cerr << "CPU_TIME=<cputime> WALL_TIME=<walltime> PMEM=<physicalmem> VMEM=<virtualmem>" << endl;
	cerr << "\"QUEUE=<queuename>\" \"USER=<localuser>\" \"LRMSID=<lrmsid>\" \"PROCESSORS=<numproc>\"" << endl;
	cerr << "\"URCREATION=<creationtime>\" \"group=<localgroup>\" \"jobName=<lrmsjobname>\"" << endl;
	cerr << "\"start=<starttimestamp>\" \"end=<endtimestamp>\" \"ctime=<ctimestamp>\"" << endl;
	cerr << "\"qtime=<qtimestamp>\" \"etime=<etimestamp>\" \"exitStatus=<exitstatus>\"" << endl;
	cerr << "\"si2k=<specint>\" \"sf2k=<specfloat>\" \"tz=<numerictimezone>\"" << endl;
	cerr << "\"fqan=<vomscertfqan>\" \"accountingProcedure=outOfBand\"" << endl;
	cerr << endl;
	cerr << "IMPORTANT: \"accountingProcedure=outOfBand\" should be specified when" << endl;
	cerr << "periodically processing LRMS logs instead of using DGAS Gianduia. This will" << endl;
	cerr << "cause the HLR server to effect additional controls to prevent from duplicate" << endl;
	cerr << "entries. This may also be used when the grid job ID is unknown (e.g. for jobs" << endl;
	cerr << "directly submitted to the CE without a UI that generates the grid job ID)." << endl;
	cerr << "In this case a unique job ID should be generated on the fly having the" << endl;
	cerr << "form \"<hostname>:<lrmsID>:<timestamp>\"." << endl;
	cerr << endl;
	cerr << "Example:" << endl;
	cerr << "CPU_TIME=2 WALL_TIME=7 PMEM=1280KB VMEM=4472KB \"QUEUE=short\" " << endl;
	cerr << "\"USER=egee003\" \"LRMSID=96128.lxb2077.cern.ch\" \"PROCESSORS=2\" " << endl;
	cerr << "\"URCREATION=Mon Sep  5 17:39:45 2005\" \"group=gm\" \"jobName=blahjob_zu1755\" " << endl;
	cerr << "\"start=1123518309\" \"end=1123518315\" \"ctime=1123518299\" \"qtime=1123518299\" " << endl;
	cerr << "\"etime=1123518299\" \"exitStatus=0\" \"si2k=400\" \"sf2k=380\" \"tz=+0200\" " << endl;
	cerr << "\"fqan=/EGEE/Role=NULL/Capability=NULL\"" << endl;
	cerr << endl;

}

int options ( int argc, char **argv )
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
		{"verbosity",1,0,'v'},
		{"jobid",1,0,'j'},
		{"time",1,0,'t'},
		{"paid",1,0,'p'},
		{"localbankid",1,0,'l'},
		{"resgridid",1,0,'C'},
		{"usrcert",1,0,'U'},
		{"help",0,0,'h'},
		{0,0,0,0}
	};
	while (( option_char = getopt_long( argc, argv, OPTION_STRING,
					long_options, &option_index)) != EOF)
		switch (option_char)
		{
			case 'v': verbosity=atoi(optarg); break;
			case 'j': dgJobId_buff=optarg; break;
			case 't': timeStamp_buff = optarg; break;
			case 'p': res_acct_PA_id_buff = optarg; break;
			case 'l': res_acct_bank_id_buff = optarg; break;
			case 'C': resX509cert_buff = optarg; break;
			case 'U': usrX509cert_buff = optarg; break;
			case 'h': needs_help =1; break;		  
			default : break;
		}
		if (optind < argc) 
		{
                      while (optind < argc)
                         info_v.push_back(argv[optind++]);
                }
	return 0;
}


void listMandatoryParameters() {
  cerr << "Error: At least the following job usage metrics have to be specified:" << endl;
  cerr << " ==> LRMSID <==\n";
}

bool usageMetricPresent(string param) {
  bool present = false;

  vector <string>::iterator info_v_it = info_v.begin();

  while (info_v_it != info_v.end() && !present) {
    if ((*info_v_it).find(param+"=") == 0) {
      present = true;
    }
    info_v_it++;
  }

  return present;
}

int main (int argc, char *argv[])
{
	options(argc, argv);
	if (needs_help)
	{
		help();
		return 0;
	}
	ATM_job_data job_data ={
		dgJobId_buff, //dgJobId
		timeStamp_buff, //time
		res_acct_PA_id_buff, //res_PA_url
		res_acct_bank_id_buff, //res_HLR_url
		usrX509cert_buff, // usr_cert_subj
		resX509cert_buff //res_cert_subj
	};
	vector <string>::iterator info_v_it = info_v.begin();
	while (info_v_it != info_v.end())
	{
		size_t pos = (*info_v_it).find("CPU_TIME=");
		if ( pos != string::npos)
		{
			cpu_time = (*info_v_it).substr(pos+9);
		}
		pos = (*info_v_it).find("WALL_TIME=");
		if ( pos != string::npos)
		{
			wall_time = (*info_v_it).substr(pos+10);
		}
		pos = (*info_v_it).find("PMEM=");
		if ( pos != string::npos)
		{
			mem = (*info_v_it).substr(pos+5);
		}
		pos = (*info_v_it).find("VMEM=");
		if ( pos != string::npos)
		{
			vmem = (*info_v_it).substr(pos+5);
		}
		info_v_it++;
	}

	// check for the presence of the mandatory metrics:
	if (!usageMetricPresent("LRMSID"))
	{
	        listMandatoryParameters();
		return 1;
	}
	ATM_usage_info usage_info = { atoi(cpu_time.c_str()), 
		atoi(wall_time.c_str()),
		mem,
		vmem 
			};
	int res;
	string output;
	res = ATM_client_toResource( 
		job_data, 
		usage_info,
		info_v, 
		&output  );
	if ( verbosity > 2 )
	{
		cout << output << endl;
	}
	if ( verbosity > 0 )
	{
		cout << "Return code:" << res << endl;
	}
	if ( res != 0 )
	{
		if ( verbosity > 1 )
		{
			hlrError e;
			cerr << e.error[int2string(res)] << endl;
		}
	}
	return res;
}

