# 📦 Amazon Industry Insights  
**Certification Data Engineer**

Ce dépôt regroupe l’ensemble des **livrables, documents et codes** produits dans le cadre du projet **Amazon Industry Insights**, structuré selon les blocs de compétence de la certification.

L’objectif global du projet est de **concevoir, implémenter, déployer et piloter un projet de données industriel** permettant de collecter, transformer, enrichir et exploiter des avis clients Amazon à des fins analytiques et métier.

---

## 🧱 Vue d’ensemble du projet

Le projet est découpé en **4 blocs complémentaires**, chacun correspondant à un ensemble cohérent de compétences :

- **Bloc 1 – Analyse & Conception**
- **Bloc 2 – ETL & Architecture Data**
- **Bloc 3 – Déploiement & Exploitation**
- **Bloc 4 – Gouvernance & Support**

Chaque bloc contient :
- des **livrables formels (PDF)** destinés à l’évaluation,
- et, lorsque nécessaire, un **repository de code** associé.

---

## 📁 Arborescence globale du dépôt

```
amazon-industry-insights/
│
├── bloc_1_analyse_conception/
│   ├── livrables/
│   ├── repository/
│   └── presentation.md
│
├── bloc_2_etl/
│   ├── livrables/
│   └── repository/
│
├── bloc_3_deploiement_exploitation/
│   ├── livrables/
│   └── repository/
│
├── bloc_4_gouvernance_support/
│   └── livrables/
│
└── README.md
```

---

## 🔹 Bloc 1 – Analyse & Conception

Analyse stratégique, étude du besoin métier, veille technologique et réglementaire (RGPD), et formalisation des exigences fonctionnelles et techniques.

📄 Livrables :
- 1. Rapport d'analyse strétégique + Ideation et Besoins   
- 2. Analyse de veille technologique et réglementaire   
- 3. Exigences + Spécifications fonctionnelles et techniques  
- presentation.md  

📁 Repository : 
- review_relevance_prototype.ipynb pour le prototype

---

## 🔹 Bloc 2 – ETL & Architecture Data

Cœur technique du projet :
- Pipeline ETL complet (Extract / Transform / Load)
- Data Lake S3 (Bronze / Silver / Gold)
- MongoDB pour la couche Bronze
- NLP Zero-Shot + scoring métier dans un notebook
- Tests unitaires

📄 Livrables :  
- Documentation d'architecture  

📁 Repository : 
- Code de l'ETL développé
- data_analysis.ipynb pour l'analyse des résultats

---

## 🔹 Bloc 3 – Déploiement & Exploitation

Mise en production, supervision, maintenance et support utilisateur.

📄 Livrables :  
- 1. Compte Rendu de Mise en Production
- 2. Documentation de Support Utilisateur
- 3. Dossier de maintenance.pdf

📁 Repository : 
- Code du pipeline orchestré

---

## 🔹 Bloc 4 – Gouvernance & Support

Pilotage projet, budget, risques, gouvernance d’équipe et plan de formation.

📄 Livrables :  
- 1. Plan de Projet
- 2. Budget et Risques
- 3. Gestion d'équipe et Suivi RH
- 4. Plan de Formation et Support Utilisateur

---

## ⚙️ Technologies principales

- Python 3.10  
- PostgreSQL  
- MongoDB  
- AWS S3  
- Apache Airflow  
- Docker / Docker Compose  
- Pandas, Hugging Face Transformers  

---

## 🎯 Objectif

Démontrer la capacité à concevoir et déployer un pipeline data industrialisable, documenté et gouverné, intégrant ETL, NLP et orchestration.

---

## 📌 Note au jury

Les repositories sont volontairement séparés par bloc afin de faciliter l'évaluation de ces derniers
