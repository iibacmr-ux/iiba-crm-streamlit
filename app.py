import streamlit as st
from datetime import datetime

# --- CONFIGURATION STYLES ET COULEURS
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon=":bar_chart:", layout="wide")

COLORS = {
    "header": "#003366",       # Bleu foncé
    "sidebar": "#005599",      # Bleu plus clair
    "btn": "#FFC300",          # Jaune bouton
    "contact": "#F7F6F3",
    "interaction": "#E3F2FD",
    "event": "#E8F5E9",
    "participation": "#FFF3E0",
    "payment": "#FCE4EC",
    "certif": "#E0F7FA"
}

st.markdown(
    f"""<style>
    .reportview-container {{
        background: {COLORS['contact']};
    }}
    .sidebar .sidebar-content {{
        background: {COLORS['sidebar']};
    }}
    h2 {{
        color: {COLORS['header']};
    }}
    .stButton>button {{background-color: {COLORS['btn']}; color: #003366; border-radius: 7px; border: none; padding: 6px 20px;}}
    </style>""",
    unsafe_allow_html=True
)

# --- NAVIGATION ---

PAGES = [
    "Dashboard 360",
    "Contacts",
    "Interactions",
    "Événements",
    "Participations",
    "Paiements",
    "Certifications"
]
choice = st.sidebar.selectbox("Navigation IIBA CRM", PAGES)

# --- PAGE 1 : VUE 360 TABLEAU DE BORD ---
if choice == "Dashboard 360":
    st.markdown("<h2>Vue 360 & Rapport</h2>", unsafe_allow_html=True)
    st.success("Bienvenue sur la vue globale du CRM ! 📊 Ici, tu trouveras la synthèse, la recherche, les KPI et l'accès à tes listes de contacts.")

    st.write("🚩 Ajouter ici tes graphiques, stats, recherche, exports...")

# --- PAGE 2 : CONTACTS ---
elif choice == "Contacts":
    st.markdown(f"<h2 style='background-color:{COLORS['contact']}'>Fiche Contact</h2>", unsafe_allow_html=True)
    with st.form("form_contacts"):
        nom = st.text_input("Nom*", "")
        prenom = st.text_input("Prénom*", "")
        genre = st.selectbox("Genre*", ["Homme", "Femme", "Autre"])
        titre = st.text_input("Titre / Fonction")
        societe = st.text_input("Société")
        secteur = st.selectbox("Secteur d'activité", ["IT", "Finance", "Éducation", "Santé", "Autre"])
        email = st.text_input("Email*", "")
        telephone = st.text_input("Téléphone*", "")
        ville = st.text_input("Ville")
        pays = st.selectbox("Pays", ["Cameroun", "France", "Canada", "Autre"])
        linkedin = st.text_input("LinkedIn")
        notes = st.text_area("Notes")
        attentes = st.multiselect("Attentes", ["Contact commercial", "Newsletter", "Invitation événement"])
        submitted = st.form_submit_button("Enregistrer Contact")
        if submitted:
            st.success(f"Contact '{nom} {prenom}' enregistré ! ✅")

# --- PAGE 3 : INTERACTIONS ---
elif choice == "Interactions":
    st.markdown(f"<h2 style='background-color:{COLORS['interaction']}'>Gestion des Interactions</h2>", unsafe_allow_html=True)
    with st.form("form_interactions"):
        id_contact = st.text_input("ID Contact*")
        date_interaction = st.date_input("Date")
        canal = st.selectbox("Canal", ["Email", "Téléphone", "WhatsApp", "LinkedIn", "Autre"])
        objet = st.text_input("Objet")
        resume = st.text_area("Résumé")
        resultat = st.selectbox("Résultat", ["Positif", "Négatif", "Neutre", "À relancer"])
        responsable = st.text_input("Responsable")
        prochaine_action = st.text_area("Prochaine action")
        relance = st.date_input("Date de relance", key="relance_interaction")
        submitted = st.form_submit_button("Enregistrer Interaction")
        if submitted:
            st.success("Interaction enregistrée ! ✅")

# --- PAGE 4 : ÉVÉNEMENTS ---
elif choice == "Événements":
    st.markdown(f"<h2 style='background-color:{COLORS['event']}'>Gestion des Événements</h2>", unsafe_allow_html=True)
    with st.form("form_events"):
        nom_event = st.text_input("Nom Événement*")
        type_event = st.selectbox("Type", ["Atelier", "Conférence", "Rencontre", "Formation"])
        date_event = st.date_input("Date")
        duree = st.number_input("Durée (h)", min_value=0.0, step=0.5)
        lieu = st.text_input("Lieu")
        formateurs = st.text_input("Formateur(s)")
        invites = st.text_area("Invité(s)")
        objectif = st.text_area("Objectif")
        notes = st.text_area("Notes")
        submitted = st.form_submit_button("Enregistrer Événement")
        if submitted:
            st.success("Événement enregistré ! ✅")

# --- PAGE 5 : PARTICIPATIONS ---
elif choice == "Participations":
    st.markdown(f"<h2 style='background-color:{COLORS['participation']}'>Participations</h2>", unsafe_allow_html=True)
    with st.form("form_participation"):
        id_contact = st.text_input("ID Contact*")
        id_event = st.text_input("ID Événement*")
        role = st.selectbox("Rôle", ["Participant", "Organisateur", "Formateur", "Invité"])
        inscription = st.date_input("Date d'inscription")
        arrivee = st.text_input("Arrivée (hh:mm)")
        feedback = st.slider("Feedback", 0, 5, 3)
        note = st.number_input("Note", min_value=0, max_value=20)
        commentaire = st.text_area("Commentaire")
        submitted = st.form_submit_button("Enregistrer Participation")
        if submitted:
            st.success("Participation enregistrée ! ✅")

# --- PAGE 6 : PAIEMENTS ---
elif choice == "Paiements":
    st.markdown(f"<h2 style='background-color:{COLORS['payment']}'>Paiements</h2>", unsafe_allow_html=True)
    with st.form("form_payment"):
        id_contact = st.text_input("ID Contact*")
        id_event = st.text_input("ID Événement*")
        date_pay = st.date_input("Date Paiement")
        montant = st.number_input("Montant", min_value=0.0, step=100.0)
        moyen = st.selectbox("Moyen", ["Chèque", "Espèces", "Virement", "CB", "Autre"])
        statut = st.selectbox("Statut", ["En attente", "Payé", "Remboursé", "Annulé"])
        reference = st.text_input("Référence")
        notes = st.text_area("Notes")
        relance = st.date_input("Date de relance", key="relance_paiement")
        submitted = st.form_submit_button("Enregistrer Paiement")
        if submitted:
            st.success("Paiement enregistré ! ✅")

# --- PAGE 7 : CERTIFICATIONS ---
elif choice == "Certifications":
    st.markdown(f"<h2 style='background-color:{COLORS['certif']}'>Certifications</h2>", unsafe_allow_html=True)
    with st.form("form_certif"):
        id_contact = st.text_input("ID Contact*")
        type_certif = st.selectbox("Type de Certif", ["CBAP", "CCBA", "Autre"])
        date_examen = st.date_input("Date Examen")
        resultat = st.selectbox("Résultat", ["Réussi", "Échoué", "En attente"])
        score = st.number_input("Score", min_value=0, max_value=100)
        date_obtention = st.date_input("Date Obtention")
        notes = st.text_area("Notes")
        renouvellement = st.date_input("Date Renouvellement")
        submitted = st.form_submit_button("Enregistrer Certif")
        if submitted:
            st.success("Certification enregistrée ! ✅")

