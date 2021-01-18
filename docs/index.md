dd-dans-deposit-to-dataverse
============================
Converts DANS deposit directories into Dataverse dataset-versions.

SYNOPSIS
--------

    dd-dans-deposit-to-dataverse run-service
    dd-dans-deposit-to-dataverse import [-d,--draft] <inbox>
    dd-dans-deposit-to-dataverse import [-d,--draft] -s <single-deposit>

DESCRIPTION
-----------
### Ways to run the program
Converts one or more [deposit directories](deposit-directory.md) into Dataverse dataset-versions. 
The `import` subcommand will read the deposit directories currently present in the inbox specified
as its argument, process those and then exit. When run as a service (`run-service`) the tool will 
first enqueue all deposit directories found in the configured inbox (so those will be processed first),
but then it will also process newly arriving deposit directories. 

!!! note "Import vs deposit in future versions of the tool" 
        
    In the current version of the tool the processing of each deposit directory is the same for both
    modes of operation. In the future this is likely to change, as we would want to leverage all the
    capabilities of Dataverse's [import API](https://guides.dataverse.org/en/latest/api/native-api.html#import-a-dataset-into-a-dataverse){:target=__blank}
    in the migration of datasets from EASY.

### Order of deposits in `import`
When using the `import` subcommand the deposits in the inbox are first put in the correct order. This order
is based on the value of the `Created` element in the bag's `bag-info.txt` file. 

### Processing of a deposit
The processing of a deposit consists of the following steps:

1. Check that the deposit is a valid [deposit directory](deposit-directory.md)
2. Check that the bag in the deposit is a valid DANS bag.
3. Map the dataset level metadata to the metadata fields expected in the target Dataverse.
4. If:
    * deposit represents first version of a dataset: create a new dataset draft
    * deposit represents an update to an existing dataset: [draft a new version](#update-deposit)  
5. Publish the new dataset-version if auto-publish is on.

#### Update deposit
<!--  How the update of the files is derived from the diff of latest published version and deposit  -->

### Mapping to Dataverse dataset

!!! note "Target Dataverse variations in mapping"

    In the current version of the tool there is only one target Dataverse, and therefore only one set of
    mapping rules. This will change in the future, as the target Dataverses will be different data stations   
    with different requirements.

#### Dataset level metadata
Currently documented in a set of internal documents.

#### File level metadata


ARGUMENTS
---------

    Options:
    
      -h, --help      Show help message
      -v, --version   Show version of this program
    
    Subcommands:
      run-service   Starts DANS Deposit To Dataverse as a daemon that processes deposit directories as they appear in the configured inbox.
      import        Imports one ore more deposits. Does not monitor for new deposits to arrive, but instead terminates after importing the batch.

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