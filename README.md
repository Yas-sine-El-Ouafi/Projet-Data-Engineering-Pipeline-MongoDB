<div align="center">
  <a href="https://blent.ai">
    <img src="https://blent-static-media.s3.eu-west-3.amazonaws.com/images/logo/logo_blent_300x.png" alt="Logo Blent.ai" width="200" />
  </a>

  <h2 align="center">Mise en place d'un pipeline ETL depuis une base MongoDB</h2>

  <p align="center">
    Projet Data Engineering - <a href="https://blent.ai">Blent.ai</a>
    <br />
    <a href="https://blent.ai/app/projects" target="_blank"><strong>Explorer tous les projets »</strong></a>
</div>

<div align="center"><img src="https://cdn.static-media.blent.ai/images/projects/badge_mongodb.svg" width="120" alt="Badge du projet" /></div>

## À propos du projet

Une enseigne de jeux vidéos cherche à améliorer son catalogue de vente en ligne. Pour cela, elle veut proposer sur sa page d’accueil et dans ses campagnes de communication (newsletter, réseaux sociaux) une  **liste des jeux les mieux notés et les plus appréciés**  de la communauté sur les derniers jours.

Afin de refléter au mieux l’avis des internautes, elle souhaite  **récupérer les avis les plus récents**  de ses propres clients en ligne pour déterminer les jeux les mieux notés. Les développeurs Web de l’entreprise souhaitent pouvoir requêter ces informations sur une base de données SQL qui va historiser au jour le jour les jeux les mieux notés.

Les données brutes stockées dans une base MongoDB, et il est supposé que celles-ci sont ajoutées au fur et à mesure par d’autres programmes (API backend). L’objectif est de construire un pipeline de données qui va alimenter automatiquement un Data Warehouse (représenté par une base de données SQL) tous les jours en utilisant les données depuis la base MongoDB.

> TODO : Compléter cette partie pour apporter plus d'informations sur le contexte du projet.

## Étapes du projet

- [ ] Ajouter les données brutes dans une base MongoDB
- [ ] Créer la base de données SQL avec le schéma associé
- [ ] Développer le script Python du pipeline ETL
- [ ] Automatiser le pipeline avec un outil de planification
- [ ] Publier le code source et les résultats sur GitHub

La description des étapes est disponible sur la page associée au projet.

> TODO : Cocher les cases au fur et à mesure de l'avancement.

## Structure du projet

Le dépôt Git contient les éléments suivantes.

- `notebooks/` contient les Notebooks Jupyter du projet.
- `src/` contient les codes sources Python principaux du projet.
- `data/` contient les données du projet.
- `config/` contient les configurations et paramètres du projet.
- `LICENSE.txt` : licence du projet.
- `requirements.txt` : liste des dépendances Python nécessaires.
- `README.md` : fichier d'accueil.

## Premiers pas

Les instructions suivantes permettent d'exécuter le projet sur son PC.

### Pré-requis

Le projet nécessite Python 3 d'installé sur le système.

> TODO : Ne pas hésiter à compléter/adapter cette partie en fonction des dépendances logicielles.

### Installation

1. Cloner le projet Git.
	```
	git clone https://github.com/blent-ai/Projet-Data-Engineering-Pipeline-MongoDB.git
	```
2. Installer les dépendances du fichier `requirements.txt` dans un environnement virtuel.

	**Linux / MacOS**
	```
	python3 -m venv venv/
	source venv/bin/activate
	pip install -r requirements.txt
	```
	**Windows**
	```
	python3 -m venv venv/
	C:\<chemin_dossir>\venv\Scripts\activate.bat
	pip install -r requirements.txt
	```

> TODO :
> - Remplir le fichier `requirements.txt` pour y ajouter les dépendances (Pandas, PyMongo, Airflow, etc).
> - Compléter la procédure d'installation pour l'adapter en fonction des besoins (MongoDB, base SQL).

### Démarrage

> TODO : Expliquer en quelques lignes et avec des exemples de ligne de commande comment l'utilisateur peut entraîner ou utiliser lui-même le modèle. 

## Licence

*Ce projet est proposé par <a href="https://blent.ai">Blent.ai</a>. Les données utilisées pour ce projet peuvent être soumises à des droits d'auteur et de propriété intellectuelle. Blent.ai ne peut être responsable des utilisations faites des données utilisées dans le cadre de ce projet.*

> TODO : Ajouter les licences supplémentaires au projet (autres données, outils propriétaires, etc).

## Citations

[1]  [Jianmo Ni, Jiacheng Li, Julian McAuley "Justifying recommendations using distantly-labeled reviews and fined-grained aspects", Empirical Methods in Natural Language Processing (EMNLP), 2019](https://cseweb.ucsd.edu//~jmcauley/pdfs/emnlp19a.pdf)
