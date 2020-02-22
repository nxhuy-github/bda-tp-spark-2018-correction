# TP TIW6 2018 - Spark

L'objectif de ce TP est de prendre en main le _framework_ [Spark](http://spark.apache.org).
Vous avez le choix de réaliser ce TP en Scala ([tutoriel Scala](https://forge.univ-lyon1.fr/EMMANUEL.COQUERY/tutoriel-scala-bda/blob/master/README.md)) ou en Python (version 2.7).

> Attention pour scala: si vous utilisez les machines de TP, configurer votre compte comme indiqué [dans l'aide](http://liris.cnrs.fr/~ecoquery/dokuwiki/doku.php?id=enseignement:aide:maven) (faires les deux configurations: maven et sbt)

## Jeu de données

Dans ce TP, on travaillera sur un jeu de données issue d'observations astronomiques issues du projet [PetaSky](http://com.isima.fr/Petasky).
On considèrera essentiellement deux relations: SOURCE et OBJECT.
SOURCE contient des données observationnelles.
OBJECT contient des informations sur les objets célestes associés aux observations.
Ces relations sont fournies sous forme de fichiers CSV compressés sans entête, dont les valeurs sont séparées par `,`.
Les valeurs nulles sont représentées par les quatre lettres `NULL` qui remplacent alors la valeur.
On dispose également de deux fichiers SQL contenant des informations de schéma.
Ces deux relations comportent de nombreux attributs.
Même si les attributs d'intérêt sont mentionnés dans le TP, il sera nécessaire de connaître leur position dans les lignes.
On préfèrera une méthode où les positions des attributs sont déterminées automatiquement à partir des fichiers schéma SQL.

Fichiers utiles:

- [Source.sql](samples/Source.sql)
- [Object.sql](samples/Object.sql)
- [source-sample](samples/source-sample)
- [object-sample](samples/object-sample)

## Accès au cluster Hadoop

Chacun dispose d'un compte sur le cluster Hadoop (sur la machine indiquée dans [tomuss](https://tomuss.univ-lyon1.fr), case `se_connecter_a` de l'UE `UE-INF2339M Big Data Analytics`).
Le login est votre login étudiant.

> Dans la suite du TP, il faut systématiquement remplacer:
>
> - `p1234567` par votre login étudiant.
> - `192.168.73.202` par l'adresse de la machine mentionnée ci-dessus.

Il faut se connecter en utilisant la clé SSH fournie dans la case `Cle_SSH` de l'UE BigDataAnalytics sur [tomuss](http://tomusss.univ-lyon1.fr).
Attention, il faut changer les droits pour que la clé ne soit lisible que par vous (en utilisant `chmod go-rwx fichier-cle-ssh`).

```shell
ssh -i fichier-cle-ssh p1234567@192.168.73.202
```

Pour copier un fichier sur la machine `192.168.73.202` on peut utiliser la commande suivante:

```shell
scp -i fichier-cle-ssh fichier-a-copier p1234567@192.168.73.202:
```

Pour éviter d'ajouter `-i fichier-cle-ssh`, il est possible d'utiliser ssh-agent via la commande suivante:

```shell
ssh-add fichier-cle-ssh
```

> Dans la suite du TP la machine `192.168.73.202` sera référencée en tant que `master`.

## HDFS

Avant de commencer à écrire des requêtes Spark, il est nécessaire de se familiariser avec les commandes d'accès au HDFS.

L'accès aux commandes permettant d'accéder au HDFS se fera sur la machine `master`

Lister le contenu d'un répertoire

```shell
hdfs dfs -ls /le/repertoire/a/lister
```

Copier un fichier vers HDFS

```shell
hdfs dfs -copyFromLocal fichier-a-copier /destination/dans/hdfs
```

Copier un fichier depuis HDFS

```shell
hdfs dfs -copyToLocal /fichier/dans/hdfs fichier-local
```

Lire le contenu d'un fichier depuis HDFS

```shell
hdfs dfs -cat /fichier/dans/hdfs
```

à combiner avec, par exemple `| less`

> Remarque: dans HDFS, les fichiers avec un nom relatifs sont traités dans HDFS comme s'il étaient référencés à partir du répertoire `/user/p1234567`

> Remarque: dans le système de fichier Linux, votre repertoire utilisateur est le la forme `/home/p1234567` alors que dans le système de fichier HDFS, il est de la forme `/user/p1234567`.

La commande suivante affiche toutes commandes disponibles pour interagir avec HDFS:

```shell
hdfs dfs
```

### Exercice

- lister le contenu du répertoire `/user/p1234567`
- copier le présent énoncé (_i.e._ `README.md` de votre machine vers `master`)
- copier ensuite ce fichier `README.md` de votre compte sur `master` vers le répertoire `/user/p1234567` dans le HDFS
- vérifier que le fichier a bien été copié en listant le contenu du répertoire, puis en affichant le contenu du fichier `README.md` après sa copie dans HDFS

## Premiers pas avec Spark

### Avec spark-shell (Scala) ou pyspark (Python)

Sur la machine `master`, réaliser le tutoriel [Quick start](http://spark.apache.org/docs/2.2.0/quick-start.html) jusqu'à la partie Caching inclue.
Le fichier `README.md` à utiliser est celui que vous avez copié dans le HDFS.
Attention, les résultats seront différents de ceux du tutoriel car le fichier n'est pas le même (c'est celui de l'énoncé du TP).

Copier le fichier `README.md` dans votre répertoire HDFS, puis reprendre le début du tutoriel en accédant cette fois si au fichier dans le HDFS (supprimer le fichier du répertoire Linux courant après l'avoir copié dans HDFS pour être sûr(e) de soi).
Pour indiquer à Spark d'accéder à un fichier dans HDFS au lieu d'un fichier dans le système de fichier standard, il suffit de précéder le nom du fichier de `hdfs://`.
On peut par exemple utiliser `hdfs:///user/p1234567/README.md`.

Remarques:

- Il ne faut pas mettre `./bin/` avant `spark-shell` et `pyspark`
- Il y a une erreur dans le tutoriel Python: pour `wordCounts`, il faut remplace `.as` par `.name`
- Ignorer les avertissements concernant "Error while looking for metadata directory" et "lineage"
- Le shell que vous utilisez se connecte par défaut sur le cluster Spark.

### Application utilisant Spark

Si ce n'est pas déjà fait, créer un nouveau projet sur la [forge Lyon 1](https://forge.univ-lyon1.fr), cloner ce projet, puis dans le répertoire obtenu récupérez le projet du TP:

```shell
git pull https://forge.univ-lyon1.fr/EMMANUEL.COQUERY/tp-spark-2018.git
```

#### Si vous utilisez Scala

Sur votre machine, continuer le [tutoriel](http://spark.apache.org/docs/2.2.0/quick-start.html#self-contained-applications) avec les modifications suivantes, afin d'utiliser le projet pré-configuré fourni:

- Le fichier `build.sbt`contient déjà la configuration nécessaire
- Le fichier scala de l'application, déjà partiellement rempli est `src/main/scala/SparkTPApp1.scala`
- Remplacer le `"YOUR_SPARK_HOME/README.md"` par `"hdfs:///user/p1234567/README.md"`
- lancer `sbt assembly`

> Sur les machines des salles TP, mieux vaux utiliser maven à la place de sbt qui est buggé
> la commande à lancer est `mvn package`

- copier le fichier `target/scala-2.11/SparkTPApp-assembly-1.0.jar` sur `master`
- lancer l'application directement depuis `master`:
  ```
  spark-submit --class SparkTPApp1 /home/p1234567/SparkTPApp-assembly-1.0.jar
  ```

#### Si vous utilisez Python

Sur votre machine, continuer le [tutoriel](http://spark.apache.org/docs/2.2.0/quick-start.html#self-contained-applications) avec les modifications suivantes:

- Remplacer le `"YOUR_SPARK_HOME/README.md"` par `"hdfs:///user/p1234567/README.md"`
- Copier le fichier Python (_e.g._ `python/SparkTPApp1.py`) sur `master`
  ```
  spark-submit /home/p1234567/SparkTPApp1.py
  ```

Attention, contrairement à ce qui est indiqué dans le code, il ne faut pas mettre `()` après `builder`.
Attention, il faut supprimer l'appel `.master(master)`.

## Tester localement avant de déployer

Avant de déployer une tâche Spark pouvant prendre du temps sur le cluster, il est bon de pouvoir tester son fonctionnement local.

### Scala

Le fichier `src/test/scala/SampleSparkTest.scala` donne un exemple de test unitaire comptant le nombre de lignes du fichier `samples/object-sample`.

### Python

Le fichier `python/test/test_spark_sample.py` donne des exemples de tests unitaires. Le premier test nécessite de se trouver au sein du cluster pour l'exécuter. Pour lancer les tests, il faut avoir installé `pytest` (typiquement via `pip`). Il suffit ensuite de lancer `pytest` depuis le répertoire `python`.

- Sur votre machine les tests se lancent via `pytest` dans le répertoire `python` du projet. Les tests spark ne sont pas exécutés.
- Sur `master`, après avoir copié tout le répertoire `python` depuis votre machine, lancer `pytest --on-spark`, depuis le répertoire `python`. Tous les tests sont exécutés, y compris les tests spark.
- Le test spark nécessite que le fichier `samples/object-sample` soit présent dans HDFS, il afut donc créer le répertoire sample (`hdfs dfs -mkdir samples`) et le copier (`hdfs dfs -copyFromLocal object-sample samples/object-sample`) avant de lancer le test la première fois.

Quelques liens :

- [pytest](https://docs.pytest.org/en/latest/)
- [pytest-spark](https://pypi.org/project/pytest-spark/)

### Exercice

Créer un test unitaire pour l'application `SparkTPApp1`.
Pour cela, il faut séparer la fonctionnalité de mise en place du contexte Spark du calcul fait sur les données en plaçant ce dernier dans une fonction ou une méthode séparée.

## Fichiers CSV et correspondance attribut - index

Avant de traiter les données d'astronomie, il est nécessaire de se doter de fonctions utilitaires qui vont simplifier la suite des opérations.
En particulier, il est utile de construire une liste d'attributs pour Source et Object, ainsi que de connaître la position de chacun des attributs.
Pour cela, on va lire le fichier SQL contenant le schéma, en utilisant les remarques suivantes:

- Le fichier contient un attribut par ligne.
- La première et la dernière ligne du fichier ne concernent pas les attributs.
- Dans une ligne, le premier mot est le nom de l'attribut.

### Exercice

Créer un objet Scala / une classe Python `PetaSkySchema` contenant une fonction de lecture de schéma qui:

- prend un nom de fichier en argument
- lit le fichier
- produit la liste ordonnée des attributs

Ajouter à cet objet

- deux valeurs: la liste des attributs de Object et celle des attributs de Source
- pour chaque attribut utilisé dans le TP, une valeur donnant sont index

> Ne pas passer trop de temps sur cet exercice: s'il ne fonctionne pas rapidement, déclarer directement les index des attributs d'intérêt en comptant les lignes à la main dans les fichiers SQL.

## Lecture d'un fichier CSV et compte d'occurrences

Pour la suite de ce TP, on utilisera plutôt les RDD que les DataFrames. Les Dataframes sont plus rapides et mieux typés (donc plus sûrs), et sont à préférer en temps normal. Les RDD permettent cependant de mieux de comprendre le traitement en flux via des transformations qui est sous-jacent à Spark.

Créer une application Spark `SparkTPApp2` qui lit depuis le HDFS un fichier csv issu de Source dont le nom est passé en ligne de commande, puis affiche pour chaque valeur d'`objectId` le nombre de ligne qui la contient.

Cela revient à la requête SQL suivante:

```sql
SELECT object_id, count(*)
FROM source
WHERE object_id IS NOT NULL
GROUP BY object_id
```

Écrire un test Spark pour vérifier votre fonction de calcul sur `samples/source-sample` avant de lancer l'application sur le cluster.

Modifier le programme pour prendre un deuxième argument optionnel qui indique un fichier HDFS dans lequel écrire le résultat (au format CSV).

Copier le fichier `/tp-data/Source/Source-001` dans un sous-répertoire `Source` de votre répertoire HDFS et tester en indiquant ce fichier à lire.

Vérifier ce qui est créé dans HDFS.
Est-ce ce à quoi on s'attend?
Expliquer le comportement de Spark.
Essayer de lancer le travail en passant en argument le répertoire HDFS `Source` au lieu du fichier `Source-001` directement, déviner et vérifier le comportement de Spark.
Ajouter le fichier `Source-002` dans le HDFS, également dans `Source`, puis relancer le job Spark et vérifier qu'on obtient plus de résultats.

Supprimer les fichiers résultats.

### Remarques

- La méthode `s.split(",")` permet de découper la `String` `s` suivant la sous-chaîne `","` et permet ainsi d'obtenir la liste des valeurs.
  Par exemple:
  ```
  scala> "a,b,c,d".split(",")
  res0: Array[String] = Array(a, b, c, d)
  ```
  Cette méthode peut aussi être utilisée en Python (même syntaxe).
- Les valeurs `NULL` pour `object_id` ne doivent pas être prises en compte.
- Il existe de nombreuses méthodes sur les RDDs similaires aux `Traversable`s Scala.
  Il est également disponible de travailler sur le contenu d'un RDD, pour le sauver, l'afficher[^1], le compter, etc.
  Voir [une liste de ces méthodes](http://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#actions) ainsi que la [ScalaDoc pour les RDD](http://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.rdd.RDD) / l'[API Python](http://spark.apache.org/docs/2.2.0/api/python/pyspark.html#pyspark.RDD) et pour les RDD contenant des paires (clé,valeur), l'[API Scala](http://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) et le [guide Scala/Python](https://spark.apache.org/docs/latest/rdd-programming-guide.html#working-with-key-value-pairs).
- Pour passer des arguments à une application lors de sa soumission avec `spark-submit`, il suffit de les ajouter à la fin de la commande.
- Attention `countByKey` ne fonctionne que pour de petites collections, mais la documentation de la fonction donne une indication pour les grandes collections via `map` et `reduceByKey`.

## Calcul de valeurs extrémales

Créer une application Spark `SparkTPApp3` qui prend en argument un répertoire de fichiers de Source, puis calcule pour chaque valeur d'`object_id`:

- le nombre de sources
- le plus grand `source_id`
- les valeurs minimales et maximales de `ra` et `decl`

Cela correspond à la requêtes SQL suivante:

```sql
SELECT object_id, count(*), max(source_id), min(ra), max(ra), min(decl), max(decl)
FROM source
WHERE object_id IS NOT NULL
GROUP BY object_id
```

Tester avec un test unitaire localement, puis sur les données du répertoire HDFS `Source`.

### Remarques

- La fonction de réduction est plus complexe, car elle nécessite de traiter en même temps plusieurs variables.
- En Scala, un moyen de manipuler facilement un n-uplet de valeurs est de créer une _case class_ ad-hoc. En Python, le plus simple est d'utiliser une liste ou un tuple (à condition de ne pas se mélanger dans les indices).

## L'objet qui s'est le plus déplacé

Créer une classe `SparkTPApp4` qui va trouver l'objet qui s'est le plus déplacé dans le ciel.
On se contentera ici d'une approximation grossière du déplacement qui consistera en une distance euclidienne calculée à partir des valeurs extrémales de ra et decl calculées par le job précédent (dont la sortie sera ainsi utilisée pour ce job).

# Notes de corrigé

## Environnement local Python

Si [`pipenv`](https://pipenv.readthedocs.io/en/latest/) est disponible, il est possible de l'utiliser en lançant le script `init-pipenv.sh` une fois sur sa machine.
Sinon il faut avoir installé (typiquement via `pip`) les packages suivants (qui sont déjà installé sur le cluster):

- pyspark, version `2.2.0.post0`
- pytest
- pytest-spark

Commande `pip`:

```shell
pip install pyspark==2.2.0.post0 pytest pytest-spark
```

Dans tous les cas, pyspark sera téléchargé et il est assez gros, donc à ne pas faire sur les machines de TP de l'université sous peine de voir votre quota dépassé.

## Tests en Python

Les test en python peuvent être lancé localement:

- sans Spark en se limitant aux tests unitaires basiques
- avec Spark, soit sur le cluster, soit en local en utilisant `--spark_home repertoire/ou/spark/est/installe`

Ces tests sont à lancer via la commande `pytest`:

- en local depuis la racine du dépôt cloné: `pytest python` (sans `pipenv`) ou `pipenv run pytest python` (avec `pipenv`);
- en local avec Spark depuis la racine du dépôt cloné: `pytest python --spark_home repertoire/ou/spark/est/installe --on-spark` (sans `pipenv`) ou `pipenv run pytest python --spark_home repertoire/ou/spark/est/installe --on-spark` (avec `pipenv`);
- sur le cluster avec Spark: `pytest python --on-spark` depuis le répertoire parent du répertoire `python`.

