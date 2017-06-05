# SGCN Processing

This repository contains code for processing the data that USGS integrates and synthesizes for the State Species of Greatest Conservation Need. The scripts here start from the [repository](https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5) for original source data collected from the states and stored in ScienceBase. Data are processed into a still experimental online data system we are experimenting with via an API.

Much of the processing in these notebooks is now dependent on modules from the bis and bis2 packages. The bis package can be found under the same usgs-bcb org on GitHub. The bis2 package contains sensitive information and is available elsewhere.

## Current working stuff

The following code is mostly working here. Everything else is still under development and may or may not work for you.

* Process SGCN repository source files (notebook and separate script) - This code works through the ScienceBase collection for SWAP source files, checks to see if the data are already in the system, reprocesses as configured, and syncs the data in our online GC2 instance with the data from the source files.
* Register SGCN Unique Species Names in TIR (notebook and separate script) - This code grabs up all unique species names from the SGCN data that have not been previously registered in the TIR, packages up registration informaiton, and inserts the data into the TIR for further processing.

## Provisional Software Disclaimer
Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.