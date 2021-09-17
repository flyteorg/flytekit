fsspec data plugin for flytekit - Experimental
=================================================

This plugin provides an implementation of the data persistence layer in flytekit, that uses fsspec. Once this plugin
is installed, it overrides all default implementation of dataplugins and provides ones supported by fsspec. this plugin
will only install the fsspec core. To actually install all fsspec plugins, please follow fsspec documentation.
