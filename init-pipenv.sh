#!/usr/bin/env bash
echo "Ce script utilise pipenv pour virtualiser vos dépendances dans un environment python virtuel"
echo "Il est inutile de lancer ce script dans le cluster"
if ! pipenv --version
then
    echo "pipenv ne semble pas installé ou semble défecteux"
    exit 1
fi

# Installation dans pipenv de pip 18.0 car pip 18.1 est buggé
pipenv run pip install pip==18.0
pipenv install
