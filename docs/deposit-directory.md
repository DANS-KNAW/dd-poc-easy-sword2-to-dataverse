Deposit directory layout
========================

`dd-dans-deposit-to-dataverse` consumes so-called *deposit directories*. This section documents what
the requirements for such a directory are.

Layout
------
The overall layout of a deposit directory is summarized below. Simply put, it consists of a
[bag]{:target=__blank} and a [properties file]{:target=__blank}, with extra requirements for both 
(see following subsections).

    .
    └── deposit-directory
        ├── bag-dir
        │   ├── bag-info.txt
        │   ├── bagit.txt
        │   ├── data
        │   │   └── file1.txt
        │   │       ...
        │   ├── metadata
        │   │   └── dataset.xml
        │   │       ...
        │   ├── manifest-sha1.txt
        │   └── tagmanifest-sha1.txt
        └── deposit.properties

Bag
---
The bag contains all the data and metadata for a dataset-version to be created in the target Dataverse.
It must conform to specifications of a SIP [^1] dans-bag as detailed in [DANS BagIt Profile]({{ dans_bagit_profile }}){:target=__blank}.
This can be validated using the [Validate DANS Bag]({{ easy_validate_dans_bag }}){:target=__blank} tool.

deposit.properties
------------------
The available properties can be found in [this spreadsheet](./deposit-properties.ods)


[bag]: https://tools.ietf.org/html/rfc8493
[properties file]: https://en.wikipedia.org/wiki/.properties

[^1]: Submission Information Package; see [OAIS]({{ oais }}){:target=__blank}
