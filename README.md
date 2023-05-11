# Generic Dyconits in Different Topologies in .NET
This Github repository contains the code for the thesis on "Generic Dyconits in Different Topologies" by Jurre Brandsen. The code is implemented in .NET and includes Dyconit and Producer_old directories along with several configuration files.

## Dyconit
This directory contains the implementation of the Dyconit data structure in C#. The code includes the implementation of the a->b configuration, transitive property, and cluster consumer that accepts from multiple brokers.

## Producer_old
This directory contains an old implementation of the producer code.

## Configuration Files
* a-b-c_config: The configuration file for successfully implementing a transitive consumption for Kafka a->b->c
* a-b_config: The configuration file for the cluster that is now working
* active-active_replication: The modified cluster consumer to accept from multiple brokers
* producer_config_test: The configuration file for the a->b configuration that is working correctly

## Docker-Compose Files
* docker-compose-work.yml: The docker-compose file used for the working configuration
* docker-compose.yml: The docker-compose file used for trying to figure out the producer and consumer on different machines
* docker-compose_cluster.yml: The docker-compose file used for the cluster that is now working

## Author
Jurre Brandsen (Github username: JurreBrandsen1709) is the author of this thesis code.
