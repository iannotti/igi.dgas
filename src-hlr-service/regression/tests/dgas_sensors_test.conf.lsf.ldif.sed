s/\$lrmsType/lsf/; 
s/\$jobsToProcess/all/; 
s/\$useUrKeyDefFile/yes/;
s/\$glueLdifFile/\${DGAS_LOCATION}\/libexec\/dgastests\/tests\/static-test.ldif/; 
s/\$lsfAcctLogDir/\${DGAS_LOCATION}\/libexec\/dgastests\/tests\/lrms-acct\//
