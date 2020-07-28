# Protein Interaction Analysis

As cancer is a genetic disease strong effort has been invested in the research of molecular genetics over the last decades
to identify genes that are responsible for the genesis of various cancer types. To understand the relationship between
genes and cancer (and other diseases) complex molecular interaction networks are investigated by performing network
analysis on network models. Goal of such a network analysis is to find the molecules in the network that cause cancer
[Wu et al].

In this data analysis project the objective is to predict cancer-related proteins in a human protein-protein-interaction
network. Based on all available information about protein function types, cancerous proteins and network
characteristics protein attributes are generated. Based on these attributes the proteins are evaluated with a scoring
scheme that attaches a scoring value to each of the proteins in the network representing tenancy of the protein to be
cancerous or not. Finally the proteins are ranked according to their score and based on some condition a number of
protein candidates are predicted to be cancer causing. Thereby the scoring model mainly contributes to the quality of the
prediction.

For the data analysis in our project we used SPSS Model, a very handy tool to for quick data manipulation, Cytoscape, a
network visualization platform, and Apache Spark to write some small Scala programs for the attribute generation and
scoring model implementation. As the data that we are provided with does not have the size that would require a
distributed data processing framework like Apache Spark we mainly use it for the functionality that it provides to
transform the data in Resilient Distributive Data format. Exemplary functions that we apply during our project are
groupByKey() and join().
