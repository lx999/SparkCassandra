# Projet NF26 TD2 groupe 19-20

Deuxième projet de NF26 basé sur le jeu de données ASOS en Italie de 2005 à 2014.

Sur tous les fichiers python, à la fin, il est possible de trouver des exemples d'utilisation commentés avec #.
Pour tester, merci de décommenter les fonctions.

## Dossier Database

### creationBases.cql

Fichier contenant les requêtes SQL pour créer nos deux bases Cassandra.
Leur structure est détaillée dans le rapport.

### insertion.py

Lecture d'une fichier csv asos et insértion des données dans les deux bases de donnée.

### stationList.csv

La liste des stations avec leurs coordonnées, y compris la station en Irak (présente dans le jeu de données).

### stationList.py

Fichier qui permet de créer le fichier stationList.csv.

## Dossier Questions

### question1.py

Réponse à la première question.
Format de la fonction à appeler :

```
drawStation(12.3, 42.24, 2011)
```

### question2.py

Réponse à la deuxième question.
Format de la fonction à appeler :

```
drawmap(2013,7,14,14,50, 'windDirection')
```

### question2-var.txt

Liste des variables possibles. 
Dans le même ordre que dans le rapport à partir de la température.

### question3.py

Réponse à la troisième question.
Format de la fonction à appeler :

```
partitionMap('2011-01', '2011-10', 3)
```

## Dossier images

Contient des exemples d'images et de fichiers html pouvant être générées avec nos fonctions.
Pour les fichiers html, il est normal d'avoir un point en Irak : il existe bien une station en Irak (QLQ) dans notre jeu de données.
