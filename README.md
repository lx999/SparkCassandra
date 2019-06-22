# SparkCassandra
NF26- School Project

Comprendre la connaissance de clé de partition et tri.
Manipuler cassandra et spark

Lors de la sélection de données, la clé de partition doit être affecté avec une valeur explicitement.
Les contraintes genre l'ordre ne s'applique sur les clés de primaires. ( <, >, >=...)
On doit respecter l'ordre de clé primaire. (ex: ((day, hour), minute) on peut pas faire minute = 20 sans donner une valeur à day et hour)
 
