dd-dans-deposit-to-dataverse
============================
[![Build Status](https://travis-ci.org/DANS-KNAW/dd-dans-deposit-to-dataverse.png?branch=master)](https://travis-ci.org/DANS-KNAW/dd-dans-deposit-to-dataverse)

Imports DANS deposit directories into Dataverse datasets.

SYNOPSIS
--------

    dd-dans-deposit-to-dataverse run-service
    dd-dans-deposit-to-dataverse import <inbox>

DESCRIPTION
-----------
Service that watches an inbox directory for new deposit directories. Each deposit directory that appears is checked for conformity 
to DANS BagIt Profile (SIP) and subsequently added as a dataset to the configured Dataverse.

ARGUMENTS
---------

    Options:
    
      -h, --help      Show help message
      -v, --version   Show version of this program
    
    Subcommands:
      run-service   Starts DANS Deposit To Dataverse as a daemon that processes deposit directories as they appear in the configured inbox.
      import        Imports deposits in the directory specified. Does not monitor for new deposits to arrive, but instead terminates after importing the batch.

INSTALLATION AND CONFIGURATION
------------------------------
Currently this project is built as an RPM package for RHEL7/CentOS7 and later. The RPM will install the binaries to
`/opt/dans.knaw.nl/dd-dans-deposit-to-dataverse` and the configuration files to `/etc/opt/dans.knaw.nl/dd-dans-deposit-to-dataverse`. 

To install the module on systems that do not support RPM, you can copy and unarchive the tarball to the target host.
You will have to take care of placing the files in the correct locations for your system yourself. For instructions
on building the tarball, see next section.

BUILDING FROM SOURCE
--------------------
Prerequisites:

* Java 8 or higher
* Maven 3.3.3 or higher
* RPM

Steps:
    
    git clone https://github.com/DANS-KNAW/dd-dans-deposit-to-dataverse.git
    cd dd-dans-deposit-to-dataverse 
    mvn clean install

If the `rpm` executable is found at `/usr/local/bin/rpm`, the build profile that includes the RPM 
packaging will be activated. If `rpm` is available, but at a different path, then activate it by using
Maven's `-P` switch: `mvn -Pprm install`.

Alternatively, to build the tarball execute:

    mvn clean install assembly:single
