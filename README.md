# PythonTestHarness

Introduction

This test harness was developped to ...
  send xml files to msmq
  send csv files to a file location
  execute Rast GET requests
  execute Rest POST requests
  execute SQL requests
  
The test suite is driven by BDD test cases (Given, When, Then) which refers to the steps defined in the steps.py file. The implementation, such as xml and json file handling, is stored in the core.py file. The integration with msmq, rest and sql are implemented in respective files.
 
Installation
 pip install behave
 
 Project specific data, configuration and files have been removed from this project.
