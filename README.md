# iiba-crm-streamlit

📌 IIBA Cameroun – CRM Streamlit

Application CRM légère, développée avec Streamlit, pour gérer les contacts, interactions, événements, participations, paiements et certifications d’IIBA Cameroun.
Elle inclut des fonctionnalités avancées de migration de données et d’administration.

🚀 Installation
1. Cloner le projet
git clone https://github.com/votre-repo/iiba-crm-streamlit.git
cd iiba-crm-streamlit

2. Créer un environnement virtuel
python -m venv venv
source venv/bin/activate   # macOS / Linux
venv\Scripts\activate      # Windows

3. Installer les dépendances
pip install -r requirements.txt

4. Lancer l’application
streamlit run app.py


L’application s’ouvrira automatiquement dans votre navigateur (par défaut http://localhost:8501
).

📂 Fonctionnalités principales
👥 CRM (Contacts)

Affichage des contacts via AgGrid (pagination par défaut à 20).

Sélection d’un contact → affichage de la fiche détaillée + actions liées.

Déduplication automatique des doublons (email, téléphone, nom + prénom + société).

📅 Événements

Gestion des événements (webinaires, afterworks, formations…).

Filtre dynamique dans le tableau (par type, année, lieu, etc.).

Suivi des participations (contacts liés aux événements).

📑 Migration / Administration

Import Excel Global : un seul onglet Global avec la colonne __TABLE__.

Import Excel Multi-onglets : un onglet par table (Contacts, Interactions, Événements, etc.).

Import CSV : global ou par table.

Export : Excel/CSV global ou par table.

Réinitialiser la base : supprime tous les fichiers CSV → base vide.

Purger un ID : supprime un contact ou événement spécifique.

Logs horodatés : suivi de chaque import/export avec comptage et erreurs éventuelles.

📝 Bonnes pratiques CRM appliquées

Prospect actif : contact ayant au moins 1 interaction ou participation en cours.

Prospect converti : contact ayant réglé un paiement lié à un événement.

Taux de conversion = Nombre de prospects convertis / Nombre total de prospects actifs.

Normalisation automatique des noms d’entreprises, types d’événements et sources (via page Paramètres).

Mobile Money par défaut pour les paiements, sauf si autre mode est précisé.

📊 Exemple de flux typique

Importer un fichier Excel (multi-onglets ou global) depuis la page Migration.

Vérifier les logs : contacts importés, doublons rejetés, erreurs éventuelles.

Consulter la page CRM : parcourir, filtrer ou éditer les contacts.

Ajouter un événement dans la page Événements, puis lier des participations.

Suivre les paiements dans la page Paiements.

Exporter un fichier Excel Global pour sauvegarde ou reporting.

⚙️ Dépendances

Voir requirements.txt
 :

streamlit, pandas, numpy

openpyxl, xlsxwriter

st-aggrid

matplotlib, plotly

python-dateutil

📧 Support

Ce projet est maintenu par IIBA Cameroun.
Pour toute question :
📩 contact@iibacameroun.org

🌍 www.iibacameroun.org
