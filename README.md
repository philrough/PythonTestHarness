# PythonTestHarness

<b>Introduction</b>

This test harness was developped to ...<br>
<ul>
  <li>Send xml files to msmq</li>
  <li>Execute Rest GET and Post requests</li>
  <li>Execute SQL requests</li>
</ul>


The test suite is driven by BDD test cases (Given, When, Then) which refers to the steps defined in the steps.py file. The implementation, such as xml and json file handling, is stored in the core.py file. The integration with msmq, rest and sql are implemented in respective files.
 
 <br>
<b>Installation and execution</b>
<ul>
  <li>pip install behave</li>
  <li>> behave suites/projects/module --include testname.feature --tags=~@wip
</ul>

 
Note - Project specific data, configuration and files have been removed from this project. Folder structure, template files and test will need to be created.
