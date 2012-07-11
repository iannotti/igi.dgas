#include <string>
#include <iostream>
#include <getopt.h>
#include <vector>
#include <sys/stat.h>

#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/dgas-consumers/consumers/AMQConsumer.h"

#include "glite/dgas/hlr-service/base/db.h"
#include "glite/dgas/common/base/comm_struct.h"
#include "glite/dgas/common/hlr/hlr_prot_errcode.h"
#include "glite/dgas/common/base/dgas_config.h"
#include "glite/dgas/common/base/dgas_lock.h"
#include "glite/dgas/common/base/libdgas_log.h"
#include "glite/dgas/common/base/int2string.h"
#include "glite/dgas/common/base/stringSplit.h"
#include "glite/dgas/common/base/xmlUtil.h"
#include "../../src-hlr-service/base/serviceVersion.h"

#define OPTION_STRING "3hv:B:t:c:TQAu:p:n:s:i:NDFm:o:d:"
#define E_CONFIG 10
#define E_BROKER_URI 11

ofstream logStream;

const char * hlr_sql_server;
const char * hlr_sql_user;
const char * hlr_sql_password;
const char * hlr_tmp_sql_dbname;
const char * hlr_sql_dbname;

using namespace std;

class recordConsumerParms
{
public:
	string configFile;
	string amqBrokerUri;
	string amqUsername;
	string amqPassword;
	string amqClientId;
	string lockFileName;
	string logFileName;
	string dgasAMQTopic;
	bool useTopics;
	bool clientAck;
	string hlrSqlTmpDBName;
	string hlrSqlDBName;
	string hlrSqlServer;
	string hlrSqlUser;
	string hlrSqlPassword;
	string name;
	string selector;
	bool noLocal;
	bool durable;
	bool foreground;
	string outputType;
	string outputDir;
	string messageNumber;
};

int system_log_level = 9;
bool needs_help = false;
int verbosity = 3;
string brokerUri = "";
string topic = "";
string configFile = "";
bool useTopics = false;
bool clientAck = false;
string username = "";
string password = "";
string clientId = "";
string name = "";
string selector = "";
bool noLocal = false;
bool durable = false;
bool foreground = false;
string messageNumber = "";
string outputType = "";
string outputDir = "";
//string configFile = GLITE_DGAS_DEF_CONF;


bool is_number(const std::string& s)
{
	std::string::const_iterator it = s.begin();
	while (it != s.end() && std::isdigit(*it))
		++it;
	return !s.empty() && it == s.end();
}

int putLock(string lockFile)
{
	dgasLock Lock(lockFile);
	if (Lock.exists())
	{
		return 1;
	}
	else
	{
		if (Lock.put() != 0)
		{
			return 2;
		}
		else
		{
			return 0;
		}
	}
}

int removeLock(string lockFile)
{
	dgasLock Lock(lockFile);
	if (Lock.exists())
	{
		if (Lock.remove() != 0)
		{
			//hlr_log("Error removing the lock file", &logStream, 1);
			return 2;
		}
		else
		{
			//hlr_log("lock file removed", &logStream, 4);
			return 0;
		}
	}
	else
	{
		//hlr_log("lock file doesn't exists", &logStream, 1);
		return 1;
	}
}

class AMQConsumerStdOut: public SimpleAsyncConsumer
{

public:

	AMQConsumerStdOut(const std::string& brokerURI, const std::string& destURI,
			bool useTopic = false, bool clientAck = false,
			std::string name = "", std::string selector = "",
			bool nolocal = false, bool durable = false,
			std::string username = "", std::string password = "",
			std::string clientId = "", long int numMessages = 1)
	{
		doneLatch = new CountDownLatch(numMessages);
		this->useTopic = useTopic;
		this->brokerURI = brokerURI;
		this->topicName = destURI;
		this->clientAck = clientAck;
		this->username = username;
		this->password = password;
		this->clientId = clientId;
		this->name = name;
		this->selector = selector;
		this->noLocal = noLocal;
		this->durable = durable;
		this->numMessages = numMessages;
	}
	//overrides AsyncConsumer useMessage() method. Can be overridden by parent classes if any.
	void useMessage(std::string messageString)
	{
		std::cout << messageString << std::endl;
	}

};

class AMQConsumerDataBase: public SimpleAsyncConsumer
{

public:

	std::string sqlServer;
	std::string sqlUser;
	std::string sqlPassword;
	std::string sqlDbname;
	db* hlrDb;

	void setSqlDbname(std::string sqlDbname)
	{
		this->sqlDbname = sqlDbname;
	}
	void setSqlPassword(std::string sqlPassword)
	{
		this->sqlPassword = sqlPassword;
	}
	void setSqlServer(std::string sqlServer)
	{
		this->sqlServer = sqlServer;
	}
	void setSqlUser(std::string sqlUser)
	{
		this->sqlUser = sqlUser;
	}

	int prepareDb()
	{
		//check if Database  exists. Create it otherwise.
		hlrDb = new db(sqlServer, sqlUser, sqlPassword, sqlDbname);
		if (hlrDb->errNo != 0)
		{
			hlr_log("Error connecting to SQL database", &logStream, 2);
			return (1);
		}
		string queryString = "DESCRIBE messages";
		dbResult queryResult = hlrDb->query(queryString);
		if (hlrDb->errNo != 0)
		{
			hlr_log("Table messages doesn't exists, creating it.", &logStream,
					5);
			queryString = "CREATE TABLE messages";
			queryString += " (";
			queryString += " id bigint(20) unsigned auto_increment, ";
			queryString += " status int DEFAULT '0', ";
			queryString += " message blob, ";
			queryString += " primary key (id) , key(status))";
			hlrDb->query(queryString);
			if (hlrDb->errNo != 0)
			{
				hlr_log("Error creating table messages.", &logStream, 2);
				return (1);
			}
		}
	}

	AMQConsumerDataBase(const std::string& brokerURI,
			const std::string& destURI, bool useTopic = false,
			bool clientAck = false, std::string name = "",
			std::string selector = "", bool nolocal = false,
			bool durable = false, std::string username = "",
			std::string password = "", std::string clientId = "",
			long int numMessages = 1)
	{
		doneLatch = new CountDownLatch(numMessages);
		this->useTopic = useTopic;
		this->brokerURI = brokerURI;
		this->topicName = destURI;
		this->clientAck = clientAck;
		this->username = username;
		this->password = password;
		this->clientId = clientId;
		this->name = name;
		this->selector = selector;
		this->noLocal = noLocal;
		this->durable = durable;
		this->numMessages = numMessages;
	}

	~AMQConsumerDataBase()
	{
		delete hlrDb;
	}
	//overrides AsyncConsumer useMessage() method. Can be overridden by parent classes if any.
	void useMessage(std::string messageString)
	{
		std::cout << "Insert in Database:" << messageString << std::endl;
		string messageQuery = "INSERT INTO messages SET ";
		messageQuery += "message=";
		messageQuery += "\'" + hlrDb->escape_string(messageString) + "\'";
		hlrDb->query(messageQuery);
		if (hlrDb->errNo != 0)
		{
			hlr_log("Error Inserting message", &logStream, 1);
			//FIXME should throw here
		}
		return;
	}

};

class AMQConsumerDir: public SimpleAsyncConsumer
{

public:
	std::string directory;
	long int count;

	AMQConsumerDir(const std::string& brokerURI, const std::string& destURI,
			bool useTopic = false, bool clientAck = false,
			std::string name = "", std::string selector = "",
			bool nolocal = false, bool durable = false,
			std::string username = "", std::string password = "",
			std::string clientId = "", long int numMessages = 1)
	{
		doneLatch = new CountDownLatch(numMessages);
		this->useTopic = useTopic;
		this->brokerURI = brokerURI;
		this->topicName = destURI;
		this->clientAck = clientAck;
		this->username = username;
		this->password = password;
		this->clientId = clientId;
		this->name = name;
		this->selector = selector;
		this->noLocal = noLocal;
		this->durable = durable;
		this->numMessages = numMessages;
		this->count = 0;
	}
	//overrides AsyncConsumer useMessage() method. Can be overridden by parent classes if any.
	void useMessage(std::string messageString)
	{
		std::ofstream fileS;
		std::string fname = directory + "/" + fileName(int2string(count));
		fileS.open(fname.c_str(), ios::app);
		if (!fileS)
		{
			std::string logBuff = "Error Inserting message in file: " + fname;
			hlr_log(logBuff, &logStream, 1);
		}
		else
		{
			fileS << messageString << endl;
			fileS.close();
		}
		count++;
	}

	void setDirectory(std::string directory)
	{
		this->directory = directory;
	}

	string fileName(string messageNumber)
	{
		//DGAS log
		time_t curtime;
		struct tm *timeLog;
		curtime = time(NULL);
		timeLog = localtime(&curtime);
		char timeBuff[16];
		strftime(timeBuff, sizeof(timeBuff), "%Y%m%d%H%M%S", timeLog);
		string buffer = "M_" + (string) timeBuff + "_" + messageNumber;
		return buffer;
	}

	bool checkDirectory()
	{
		//check if parms.recordDir exists. Create it otherwise.
		struct stat st;
		if (stat(directory.c_str(), &st) != 0)
		{
			return false;
		}
		return true;
	}

	bool createDirectory()
	{
		if ((mkdir(directory.c_str(), 0777)) != 0)
		{
			string logBuff = "Error creating UR directory:" + directory;
			cerr << logBuff << endl;
			hlr_log(logBuff, &logStream, 1);
			return false;
		}
		return true;
	}

};

int AMQRecordConsumer(recordConsumerParms& parms)
{
	int returncode = 0;
	map < string, string > confMap;
	if (parms.configFile != "")
	{
		if (dgas_conf_read(parms.configFile, &confMap) != 0)
		{
			cerr << "WARNING: Error reading conf file: " << parms.configFile
					<< endl;
			cerr << "There can be problems processing the transaction" << endl;
			return E_CONFIG;

		}
		if (parms.logFileName == "")
		{
			if (confMap["consumerLogFileName"] != "")
			{
				parms.logFileName = confMap["consumerLogFileName"];
				if (bootstrapLog(parms.logFileName, &logStream) != 0)
				{
					cerr << "Error bootstrapping Log file " << endl;
					cerr << parms.logFileName << endl;
					exit(1);
				}
			}
			else
			{
				cerr << "WARNING: Error reading conf file: "
						<< parms.configFile << endl;
				return E_BROKER_URI;
			}

		}
		if (parms.lockFileName == "")
		{
			if (confMap["consumerLockFileName"] != "")
			{
				parms.lockFileName = confMap["consumerLockFileName"];
			}
			else
			{
				cerr << "WARNING: Error reading conf file: "
						<< parms.configFile << endl;
				return E_BROKER_URI;
			}
		}
		if (!parms.foreground)
		{
			if (putLock(parms.lockFileName) != 0)
			{
				hlr_log(
						"hlr_qMgr: Startup failed, Error creating the lock file.",
						&logStream, 1);
				exit(1);
			}
		}
		if (parms.hlrSqlTmpDBName == "")
		{
			if (confMap["hlr_tmp_sql_dbname"] != "")
			{
				parms.hlrSqlTmpDBName = confMap["hlr_tmp_sql_dbname"];
			}
			else
			{
				cerr << "WARNING: Error reading conf file: "
						<< parms.configFile << endl;
				return E_BROKER_URI;
			}
		}

		if (parms.hlrSqlDBName == "")
		{
			if (confMap["hlr_sql_dbname"] != "")
			{
				parms.hlrSqlDBName = confMap["hlr_sql_dbname"];
			}
			else
			{
				cerr << "WARNING: Error reading conf file: "
						<< parms.configFile << endl;
				return E_BROKER_URI;
			}
		}

		if (parms.hlrSqlServer == "")
		{
			if (confMap["hlr_sql_server"] != "")
			{
				parms.hlrSqlServer = confMap["hlr_sql_server"];
			}
			else
			{
				cerr << "WARNING: Error reading conf file: "
						<< parms.configFile << endl;
				return E_BROKER_URI;
			}
		}

		if (parms.hlrSqlUser == "")
		{
			if (confMap["hlr_sql_user"] != "")
			{
				parms.hlrSqlUser = confMap["hlr_sql_user"];
			}
			else
			{
				cerr << "WARNING: Error reading conf file: "
						<< parms.configFile << endl;
				return E_BROKER_URI;
			}
		}

		if (parms.hlrSqlPassword == "")
		{
			if (confMap["hlr_sql_password"] != "")
			{
				parms.hlrSqlPassword = confMap["hlr_sql_password"];
			}
			else
			{
				cerr << "WARNING: Error reading conf file: "
						<< parms.configFile << endl;
				return E_BROKER_URI;
			}
		}
		hlr_sql_server = (parms.hlrSqlServer).c_str();
		hlr_sql_user = (parms.hlrSqlUser).c_str();
		hlr_sql_password = (parms.hlrSqlPassword).c_str();
		hlr_tmp_sql_dbname = (parms.hlrSqlTmpDBName).c_str();
		hlr_sql_dbname = (parms.hlrSqlDBName).c_str();
		serviceVersion thisServiceVersion(hlr_sql_server, hlr_sql_user,
				hlr_sql_password, hlr_sql_dbname);
		if (!thisServiceVersion.tableExists())
		{
			thisServiceVersion.tableCreate();
		}
		thisServiceVersion.setService("dgas-AMQConsumer");
		thisServiceVersion.setVersion(VERSION);
		thisServiceVersion.setHost("localhost");
		thisServiceVersion.setConfFile(parms.configFile);
		thisServiceVersion.setLockFile(parms.lockFileName);
		thisServiceVersion.setLogFile(parms.logFileName);
		thisServiceVersion.write();
		thisServiceVersion.updateStartup();

	}

	std::string outputType = parms.outputType;
	long int numMessages = -1;
	if (parms.messageNumber != "" && is_number(parms.messageNumber))
	{
		numMessages = atol((parms.messageNumber).c_str());
	}
	AMQConsumer consumer;
	if (outputType == "database")
	{

		AMQConsumerDataBase* consumerDataBaseImpl = new AMQConsumerDataBase(
				parms.amqBrokerUri, parms.dgasAMQTopic, parms.useTopics,
				parms.clientAck, parms.name, parms.selector, parms.noLocal,
				parms.durable, parms.amqUsername, parms.amqPassword,
				parms.amqClientId, numMessages);
		consumerDataBaseImpl->setSqlDbname(parms.hlrSqlTmpDBName);
		consumerDataBaseImpl->setSqlServer(parms.hlrSqlServer);
		consumerDataBaseImpl->setSqlUser(parms.hlrSqlUser);
		consumerDataBaseImpl->setSqlPassword(parms.hlrSqlPassword);
		if (consumerDataBaseImpl->prepareDb() != 0)
		{
			//error
			delete consumerDataBaseImpl;
			return 1;
		}
		consumer.registerConsumer(consumerDataBaseImpl);
		delete consumerDataBaseImpl;
	}
	if (outputType == "file")
	{
		std::string logBuff = "Messages will be written inside directory: "
				+ parms.outputDir;
		hlr_log(logBuff, &logStream, 6);

		AMQConsumerDir* consumerDirImpl = new AMQConsumerDir(
				parms.amqBrokerUri, parms.dgasAMQTopic, parms.useTopics,
				parms.clientAck, parms.name, parms.selector, parms.noLocal,
				parms.durable, parms.amqUsername, parms.amqPassword,
				parms.amqClientId, numMessages);
		consumerDirImpl->setDirectory(parms.outputDir);
		if (!consumerDirImpl->checkDirectory())
			consumerDirImpl->createDirectory();
		consumer.registerConsumer(consumerDirImpl);
		delete consumerDirImpl;
	}
	if (outputType == "stdout")
	{
		AMQConsumerStdOut* consumerOutImpl = new AMQConsumerStdOut(
				parms.amqBrokerUri, parms.dgasAMQTopic, parms.useTopics,
				parms.clientAck, parms.name, parms.selector, parms.noLocal,
				parms.durable, parms.amqUsername, parms.amqPassword,
				parms.amqClientId, numMessages);
		consumerOutImpl->readConf(parms.configFile);
		consumer.registerConsumer(consumerOutImpl);
		delete consumerOutImpl;
	}

	// All CMS resources should be closed before the library is shutdown.
	if (!parms.foreground)
		removeLock(parms.lockFileName);
	string logBuff = "Removing:" + parms.lockFileName;
	hlr_log(logBuff, &logStream, 1);
	return returncode;
}

void help(string progname)
{
	cerr << endl;
	cerr << "DGAS AMQ Consumer" << endl;
	cerr << "Version :" << VERSION << endl;
	cerr << "Author: A.Guarise <andrea.guarise@to.infn.it>" << endl;
	cerr << endl << "Usage: " << endl;
	cerr << progname << " [OPTIONS]" << endl << endl;
	;
	cerr << "OPTIONS:" << endl;
	cerr
			<< "-B  --brokerUri <URI>    The URI specifying the listening AMQ Broker. "
			<< endl;
	cerr
			<< "-t  --topic <dgas topic> Specifies the queue to poll for incoming messages."
			<< endl;
	cerr
			<< "-c  --config <confFile>  HLR configuration file name, if different"
			<< endl;
	cerr
			<< "-u  --username <amq username>  AMQ user if needed for authentication"
			<< endl;
	cerr
			<< "-p  --password <amq password>  AMQ password for user 'user' if needed for authentication"
			<< endl;
	cerr
			<< "-i  --clientId <amq clientId>  unique identifier for the connection of a durable subscriber"
			<< endl;
	cerr
			<< "-n  --name <subscription name>  set a name to identify the subscription"
			<< endl;
	cerr << "-s  --selector <selector>  pass a selector string to the consumer"
			<< endl;
	cerr << "-N  --nolocal  set CMS noLocal flag" << endl;
	cerr << "-T  --useTopic  Use Topic" << endl;
	cerr << "-Q  --useQueue  Use Queue" << endl;
	cerr << "-A  --clientAck  Enable consumer client ack mode" << endl;
	cerr << "-D  --durable  Enable consumer as durable" << endl;
	cerr
			<< "-F  --foreground  Do not run as deamon, consume up to -m --messages number of messages and exit."
			<< endl;
	cerr
			<< "-m  --messageNumber <number> If called with -F --foreground, consume up to -m --messages number of messages and exit."
			<< endl;
	cerr
			<< "-o  --outputType <database or stdout or file>  put the message in the database or output it on stdout or in file"
			<< endl;
	cerr
			<< "-d  --outputDir <directory> Output messages in file, one per message within the specified directory, to be used with -o file"
			<< endl;
	cerr << "-h  --help               Print this help message." << endl;
}

int options(int argc, char **argv)
{
	int option_char;
	int option_index = 0;
	static struct option long_options[] =
	{
	{ "verbosity", 1, 0, 'v' },
	{ "brokerUri", 1, 0, 'B' },
	{ "topic", 1, 0, 't' },
	{ "config", 1, 0, 'c' },
	{ "username", 1, 0, 'u' },
	{ "password", 1, 0, 'p' },
	{ "clientId", 1, 0, 'i' },
	{ "name", 1, 0, 'n' },
	{ "selector", 1, 0, 's' },
	{ "noLocal", 0, 0, 'N' },
	{ "useTopic", 0, 0, 'T' },
	{ "useQueue", 0, 0, 'Q' },
	{ "clientAck", 0, 0, 'A' },
	{ "durable", 0, 0, 'D' },
	{ "foreground", 0, 0, 'F' },
	{ "messageNumber", 1, 0, 'm' },
	{ "outputType", 1, 0, 'o' },
	{ "outputDir", 1, 0, 'd' },
	{ "help", 0, 0, 'h' },
	{ 0, 0, 0, 0 } };
	while ((option_char = getopt_long(argc, argv, OPTION_STRING, long_options,
			&option_index)) != EOF)
		switch (option_char)
		{
		case 'v':
			verbosity = atoi(optarg);
			break;
		case 'B':
			brokerUri = optarg;
			break;
		case 't':
			topic = optarg;
			break;
		case 'c':
			configFile = optarg;
			break;
		case 'u':
			username = optarg;
			break;
		case 'p':
			password = optarg;
			break;
		case 'i':
			clientId = optarg;
			break;
		case 'n':
			name = optarg;
			break;
		case 's':
			selector = optarg;
			break;
		case 'm':
			messageNumber = optarg;
			break;
		case 'o':
			outputType = optarg;
			break;
		case 'd':
			outputDir = optarg;
			break;
		case 'N':
			noLocal = "true";
			break;
		case 'T':
			useTopics = "true";
			break;
		case 'Q':
			useTopics = "false";
			break;
		case 'A':
			clientAck = "true";
			break;
		case 'D':
			durable = "true";
			break;
		case 'F':
			foreground = "true";
			break;
		case 'h':
			needs_help = true;
			break;
		default:
			break;
		}
	return 0;
}

int main(int argc, char *argv[])
{
	options(argc, argv);
	if (needs_help)
	{
		help(argv[0]);
		return 0;
	}
	recordConsumerParms parms;
	parms.amqBrokerUri = brokerUri;
	parms.dgasAMQTopic = topic;
	parms.configFile = configFile;
	parms.useTopics = useTopics;
	parms.clientAck = clientAck;
	parms.amqUsername = username;
	parms.amqPassword = password;
	parms.amqClientId = clientId;
	parms.noLocal = noLocal;
	parms.selector = selector;
	parms.name = name;
	parms.durable = durable;
	parms.foreground = foreground;
	parms.outputType = outputType;
	parms.outputDir = outputDir;
	parms.messageNumber = messageNumber;
	int res = AMQRecordConsumer(parms);
	if (verbosity > 0)
	{
		cout << "Return code:" << res << endl;
	}
	if (res != 0)
	{
		if (verbosity > 1)
		{
			hlrError e;
			cerr << e.error[int2string(res)] << endl;
		}
	}
	exit(res);
}

