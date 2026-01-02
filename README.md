# Data Stream Control Orchestration

Orchestration component in data control processes. Replication, virtualization, control, alert and response of data and database environments


## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

## Introduction

In the context of a production/development environment that requires an high volume of data(sensitive or not), this component has been developed to handle the orchestration of tasks to assert the integrity, compliance of data and its structures,nwith customable granularity. Current implementation involve the definition of controls and its related data in a configuration file(config.json), including data indexing(database, table and column), control identification and its related informatoin needed to communicate with all the components involved in the control. 
This component play the role of orchestrator within a ransomware control process. The control leverages Delphix(database virtualization and masking SaaS) capabilities to replicate data and control its integrity and compliance to a predefined list of values, with cell granularity, and very low time compared to industry benchmarks. Hence the overhead in the production environment is almost in-existent, whereby the time to alert who is responsible for the control.
This control can be replicated in any other kind of data control scenario.

## Installation
Download the repository: 

`git clone https://github.com/Diekgbbtt/ransomControlOrchestration.git`

Move to the installed folder and install required depeendencies:

`python -m pip install -r requirements.txt`

## Usage
Once installed, the next step is to fill the config.json file with the data control information. This file is in json format and has the following structure:
    - dpx_engines : delphix machines used to replicate, virtualize and execute the control.
        - source_engines : engines that host the source data to be replicated.
        - vault_engines : engines that receive the replication in the control environment and virtaulize the data for the control.
        - discovery_engines : engines that execute the effective control on the virtualized database.
    - vdbs_control : databases dedicated to be provisioned with virtualized data.
    - mail : mailing service configuration, smtpServer and sender account credentials.
    - Controls : list of controls to be executed. Each control includes peculiar data, including identifications of machines involved in the control and information required by the implementation logic. 

Config sensitive data is expected to be encrypted with CBC cipher, AES algorithm and 32 bytes key. Encryption key should be a env variable base64 encoded.
To encrypt sensitive data in utils a json file encryption script is provided, with customizable keys and ancryption alg, it will also generate encryption key if not present yet. 
From the parent dir use the following command: 

`python utils.py <path_to_config_file> <key1> <key2> <key3> ...`
 
All controls will be executed subsequently. The results will be stored in a zipped file that will be attached to the alert mail, if any dicrepancies with the expected values is found.
Each control process status will be reported in a sqlite database. After the data evaluation, discrepancies will be reported in the local database cell indexed, showing found and expected value, adding also the name of the zipped file where the results are written.

## License
This project is licensed under the MIT License. Copyright (c) 2024 Diego Gobbetti.
