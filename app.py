import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime, date, timedelta

# --- CONFIGURATION GLOBALE ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")
DATA_FILES = {
    "contacts": "contacts.csv",
    "interactions": "interactions.csv",
    "evenements": "evenements.csv",
    "participations": "participations.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv",
    "settings": "settings.json"
}
DEFAULT_SETTINGS = {
    "types_contact": ["Membre", "Prospect", "Formateur", "Partenaire"],
    "sources": ["Afterwork", "Formation", "LinkedIn", "Recommandation", "Site Web", "Salon", "Autre"],
    "statuts_engagement": ["Actif", "Inactif", "À relancer"],
    "secteurs": ["IT", "Finance", "Éducation", "Santé", "Consulting", "Autre"],
    "pays": ["Cameroun", "France", "Canada", "Belgique", "Autre"],
    "canaux": ["Email", "Téléphone", "WhatsApp", "LinkedIn", "Réunion", "Autre"],
    "types_evenements": ["Atelier", "Conférence", "Formation", "Webinaire", "Afterwork"],
    "moyens_paiement": ["Chèque", "Espèces", "Virement", "CB", "Mobile Money", "Autre"]
}

# --- FONCTIONS DE CHARGEMENT / SAUVEGARDE ---
@st.cache_data
def load_settings():
    if os.path.exists(DATA_FILES["settings"]):
        return json.load(open(DATA_FILES["settings"], encoding="utf-8"))
    else:
        save_settings(DEFAULT_SETTINGS)
        return DEFAULT_SETTINGS

def save_settings(settings):
    json.dump(settings, open(DATA_FILES["settings"], "w", encoding="utf-8"), indent=2, ensure_ascii=False)
    st.cache_data.clear()

@st.cache_data
def load_data(filename, columns):
    if os.path.exists(filename):
        df = pd.read_csv(filename, encoding="utf-8")
        # Migration auto des colonnes manquantes
        for col, default in columns.items():
            if col not in df.columns:
                df[col] = default() if callable(default) else default
        return df[list(columns)]
    else:
        return pd.DataFrame({col: [] for col in columns})

def save_data(df, filename):
    df.to_csv(filename, index=False, encoding="utf-8")
    st.cache_data.clear()

def generate_id(prefix, df, id_col):
    if df.empty: return f"{prefix}_001"
    last = df[id_col].iloc[-1]
    num = int(last.split("_")[1]) + 1
    return f"{prefix}_{num:03d}"

# --- PARAMÉTRAGES DES STRUCTURES ---
SETTINGS = load_settings()
CONTACT_COLS = {
    "ID": lambda: None, "Nom": "", "Prénom": "", "Genre": "", "Titre": "",
    "Société": "", "Secteur": SETTINGS["secteurs"], "Email": "", "Téléphone": "",
    "Ville": "", "Pays": SETTINGS["pays"], "Type_Contact": SETTINGS["types_contact"],
    "Source": SETTINGS["sources"], "Statut_Engagement": SETTINGS["statuts_engagement"],
    "LinkedIn": "", "Notes": "", "Date_Creation": lambda: datetime.now().strftime("%Y-%m-%d")
}
INT_COLS = {
    "ID_Interaction": lambda: None, "ID_Contact": "", "Date": date.today().isoformat(),
    "Canal": SETTINGS["canaux"], "Objet": "", "Résumé": "",
    "Résultat": "Positif", "Responsable": "", "Prochaine_Action": "", "Relance": ""
}
EVT_COLS = {
    "ID_Événement": lambda: None, "Nom_Événement": "", "Type": SETTINGS["types_evenements"],
    "Date": date.today().isoformat(), "Durée_h": 0, "Lieu": "",
    "Formateur(s)": "", "Invité(s)": "", "Objectif": "", "Période": "Matinée", "Notes": ""
}
PART_COLS = {
    "ID_Participation": lambda: None, "ID_Contact": "", "ID_Événement": "", "Rôle": "Participant",
    "Inscription": date.today().isoformat(), "Arrivée": "", "Temps_Présent": "", 
    "Feedback": 3, "Note": 0, "Commentaire": "", "Nom Participant": "", "Nom Événement": ""
}
PAY_COLS = {
    "ID_Paiement": lambda: None, "ID_Contact": "", "ID_Événement": "", "Date_Paiement": date.today().isoformat(),
    "Montant": 0.0, "Moyen": SETTINGS["moyens_paiement"][0], "Statut": "En attente", 
    "Référence": "", "Notes": "", "Relance": "", "Nom Contact": "", "Nom Événement": ""
}
CERT_COLS = {
    "ID_Certif": lambda: None, "ID_Contact": "", "Type_Certif": SETTINGS["types_contact"],
    "Date_Examen": date.today().isoformat(), "Résultat": "Réussi", "Score": 0,
    "Date_Obtention": date.today().isoformat(), "Validité": "", "Renouvellement": "", "Notes": "", "Nom Contact": ""
}

# --- NAVIGATION ---
PAGES = ["Dashboard 360", "Contacts", "Interactions", "Événements", "Participations", "Paiements", "Certifications", "Paramètres"]
choice = st.sidebar.selectbox("Navigation", PAGES)

# --- DASHBOARD 360 (avec filtre Année/Mois) ---
if choice == "Dashboard 360":
    st.title("📊 Dashboard 360")
    # Charger données
    df_c = load_data(DATA_FILES["contacts"], CONTACT_COLS)
    df_i = load_data(DATA_FILES["interactions"], INT_COLS)
    df_e = load_data(DATA_FILES["evenements"], EVT_COLS)
    df_p = load_data(DATA_FILES["participations"], PART_COLS)
    df_pay = load_data(DATA_FILES["paiements"], PAY_COLS)
    df_cert = load_data(DATA_FILES["certifications"], CERT_COLS)
    # Filtres
    years = sorted({d[:4] for d in df_c["Date_Creation"]}) or [str(datetime.now().year)]
    months = ["Tous"] + [f"{i:02d}" for i in range(1,13)]
    colf1, colf2 = st.columns(2)
    year = colf1.selectbox("Année", years, index=len(years)-1)
    month = colf2.selectbox("Mois", months, index=0)
    def fil(df, date_col):
        if df.empty: return df
        df2 = df[df[date_col].str[:4]==year]
        return df2 if month=="Tous" else df2[df2[date_col].str[5:7]==month]
    df_c2 = fil(df_c, "Date_Creation")
    df_e2 = fil(df_e, "Date")
    df_p2 = fil(df_p, "Inscription")
    df_pay2 = fil(df_pay, "Date_Paiement")
    df_cert2 = fil(df_cert, "Date_Obtention")
    # KPI
    c1,c2,c3,c4=st.columns(4)
    c1.metric("Prospects Actifs", len(df_c2[df_c2["Type_Contact"]=="Prospect"]))
    c1.metric("Membres IIBA", len(df_c2[df_c2["Type_Contact"]=="Membre"]))
    c2.metric(f"Événements {year}", len(df_e2))
    c2.metric("Participations Totales", len(df_p2))
    c3.metric("CA Total Réglé", f"{df_pay2[df_pay2['Statut']=='Payé']['Montant'].sum():,.0f}")
    c3.metric("Paiements en Attente", len(df_pay2[df_pay2['Statut']=='En attente']))
    c4.metric("Certif. Obtenues", len(df_cert2[df_cert2["Résultat"]=="Réussi"]))
    score = df_p2["Feedback"].mean() if not df_p2.empty else 0
    c4.metric("Score Moyen", f"{score:.1f}")

# --- FORMULAIRES CRÉATION / ÉDITION + CONSULTATION ---
elif choice == "Contacts":
    st.title("👤 Contacts")
    df = load_data(DATA_FILES["contacts"], CONTACT_COLS)
    select = st.selectbox("Sélectionner un contact", [""]+df["ID"].tolist())
    mode_edit = bool(select)
    rec = df[df["ID"]==select].iloc[0] if mode_edit else None
    with st.form("f"):
        col1,col2 = st.columns(2)
        if mode_edit:
            col1.text_input("ID", rec["ID"], disabled=True)
        nom=col1.text_input("Nom*", rec["Nom"] if rec else "", help="Nom de famille")
        prenom=col1.text_input("Prénom*", rec["Prénom"] if rec else "", help="Prénom")
        genre=col1.selectbox("Genre", ["", "Homme","Femme","Autre"], 
                            help="Genre du contact", index=(["","Homme","Femme","Autre"].index(rec["Genre"]) if rec else 0))
        titre=col1.text_input("Titre", rec["Titre"] if rec else "")
        soc=col1.text_input("Société", rec["Société"] if rec else "")
        sect=col1.selectbox("Secteur", SETTINGS["secteurs"], index=(SETTINGS["secteurs"].index(rec["Secteur"]) if rec else 0))
        typec=col2.selectbox("Type*", SETTINGS["types_contact"], index=(SETTINGS["types_contact"].index(rec["Type_Contact"]) if rec else 0))
        src=col2.selectbox("Source*", SETTINGS["sources"], index=(SETTINGS["sources"].index(rec["Source"]) if rec else 0))
        stat=col2.selectbox("Statut*", SETTINGS["statuts_engagement"], index=(SETTINGS["statuts_engagement"].index(rec["Statut_Engagement"]) if rec else 0))
        email=col2.text_input("Email*", rec["Email"] if rec else "")
        tel =col2.text_input("Téléphone*", rec["Téléphone"] if rec else "")
        ville=col2.text_input("Ville", rec["Ville"] if rec else "")
        pays=col2.selectbox("Pays", SETTINGS["pays"], index=(SETTINGS["pays"].index(rec["Pays"]) if rec else 0))
        linkedin=col2.text_input("LinkedIn", rec["LinkedIn"] if rec else "")
        notes=st.text_area("Notes", rec["Notes"] if rec else "")
        dat=col2.text_input("Date Création", rec["Date_Creation"] if rec else datetime.now().strftime("%Y-%m-%d"))
        sub=st.form_submit_button("Mettre à jour" if mode_edit else "Créer")
        if sub:
            if mode_edit:
                idx=df[df["ID"]==select].index[0]
                df.loc[idx]=[select,nom,prenom,genre,titre,soc,sect,email,tel,ville,pays,typec,src,stat,linkedin,notes,dat]
            else:
                new=dict(ID=generate_id("CNT", df,"ID"),Nom=nom,Prénom=prenom,Genre=genre,Titre=titre,
                         Société=soc,Secteur=sect,Email=email,Téléphone=tel,Ville=ville,Pays=pays,
                         Type_Contact=typec,Source=src,Statut_Engagement=stat,LinkedIn=linkedin,
                         Notes=notes,Date_Creation=dat)
                df=pd.concat([df,pd.DataFrame([new])],ignore_index=True)
            save_data(df, DATA_FILES["contacts"])
            st.success("Enregistré ✅")

elif choice == "Interactions":
    st.title("💬 Interactions")
    df_int=load_data(DATA_FILES["interactions"], INT_COLS)
    df_c=load_data(DATA_FILES["contacts"], CONTACT_COLS)
    opts=[""]+[f"{r.ID} - {r.Nom} {r.Prénom}" for _,r in df_c.iterrows()]
    with st.form("fi"):
        sel=st.selectbox("Contact",opts)
        date_i=st.date_input("Date",date.today())
        canal=st.selectbox("Canal",SETTINGS["canaux"])
        obj=st.text_input("Titre*")
        resu=st.text_area("Résumé*")
        resultat=st.selectbox("Résultat",["Positif","Négatif","Neutre","À relancer"])
        resp=st.text_input("Responsable*")
        pa=st.text_area("Prochaine action")
        rel=st.date_input("Relance (opt.)",value=None)
        sub=st.form_submit_button("Enregistrer")
        if sub:
            if not sel or not obj or not resu or not resp: st.error("Champs obligatoires*")
            else:
                idc=sel.split(" - ")[0]
                new={"ID_Interaction":generate_id("INT",df_int,"ID_Interaction"), 
                     "ID_Contact":idc,"Date":date_i.isoformat(),"Canal":canal,
                     "Objet":obj,"Résumé":resu,"Résultat":resultat,
                     "Responsable":resp,"Prochaine_Action":pa,
                     "Relance":rel.isoformat() if rel else ""}
                df_int=pd.concat([df_int,pd.DataFrame([new])],ignore_index=True)
                save_data(df_int, DATA_FILES["interactions"])
                st.success("Interaction créée✅")

elif choice=="Événements":
    st.title("📅 Événements")
    df_e=load_data(DATA_FILES["evenements"], EVT_COLS)
    with st.form("fe"):
        nom=st.text_input("Nom*")
        typ=st.selectbox("Type",SETTINGS["types_evenements"])
        dt=st.date_input("Date",date.today())
        duree=st.number_input("Durée (h)",min_value=0.0,step=0.5)
        lieu=st.text_input("Lieu")
        form=st.text_area("Formateur(s)")
        inv=st.text_area("Invité(s)")
        obj=st.text_area("Objectif")
        per=st.selectbox("Période",["Matinée","Après-midi","Journée"])
        notes=st.text_area("Notes")
        sub=st.form_submit_button("Enregistrer")
        if sub:
            new={"ID_Événement":generate_id("EVT",df_e,"ID_Événement"),"Nom_Événement":nom,
                 "Type":typ,"Date":dt.isoformat(),"Durée_h":duree,"Lieu":lieu,
                 "Formateur(s)":form,"Invité(s)":inv,"Objectif":obj,"Période":per,"Notes":notes}
            df_e=pd.concat([df_e,pd.DataFrame([new])],ignore_index=True)
            save_data(df_e, DATA_FILES["evenements"])
            st.success("Événement créé✅")

elif choice=="Participations":
    st.title("🙋 Participations")
    df_p=load_data(DATA_FILES["participations"], PART_COLS)
    df_c=load_data(DATA_FILES["contacts"], CONTACT_COLS)
    df_e=load_data(DATA_FILES["evenements"], EVT_COLS)
    opts_c=[""]+[r.ID for _,r in df_c.iterrows()]
    opts_e=[""]+[r["ID_Événement"] for _,r in df_e.iterrows()]
    with st.form("fp"):
        idc=st.selectbox("ID Contact",opts_c)
        ide=st.selectbox("ID Événement",opts_e)
        role=st.selectbox("Rôle",["Participant","Organisateur","Formateur","Invité"])
        insc=st.date_input("Inscription",date.today())
        arr=st.text_input("Arrivée (hh:mm)")
        feedback=st.slider("Feedback",1,5,3)
        note=st.number_input("Note",min_value=0,max_value=20)
        comm=st.text_area("Commentaire")
        sub=st.form_submit_button("Enregistrer")
        if sub:
            tp="AUTO"  # calcul si durée+horaire dispo
            new={"ID_Participation":generate_id("PAR",df_p,"ID_Participation"),"ID_Contact":idc,
                 "ID_Événement":ide,"Rôle":role,"Inscription":insc.isoformat(),"Arrivée":arr,
                 "Temps_Présent":tp,"Feedback":feedback,"Note":note,"Commentaire":comm,
                 "Nom Participant":"","Nom Événement":""}
            df_p=pd.concat([df_p,pd.DataFrame([new])],ignore_index=True)
            save_data(df_p, DATA_FILES["participations"])
            st.success("Participation créée✅")

elif choice=="Paiements":
    st.title("💳 Paiements")
    df_pay=load_data(DATA_FILES["paiements"], PAY_COLS)
    with st.form("fpay"):
        idc=st.text_input("ID Contact")
        ide=st.text_input("ID Événement")
        dp=st.date_input("Date Paiement",date.today())
        mont=st.number_input("Montant",min_value=0.0,step=100.0)
        moy=st.selectbox("Moyen",SETTINGS["moyens_paiement"])
        stat=st.selectbox("Statut",["En attente","Payé","Remboursé","Annulé"])
        ref=st.text_input("Référence")
        notes=st.text_area("Notes")
        rel=st.date_input("Relance (opt.)",value=None)
        sub=st.form_submit_button("Enregistrer")
        if sub:
            new={"ID_Paiement":generate_id("PAY",df_pay,"ID_Paiement"),"ID_Contact":idc,
                 "ID_Événement":ide,"Date_Paiement":dp.isoformat(),"Montant":mont,
                 "Moyen":moy,"Statut":stat,"Référence":ref,"Notes":notes,
                 "Relance":rel.isoformat() if rel else "","Nom Contact":"","Nom Événement":""}
            df_pay=pd.concat([df_pay,pd.DataFrame([new])],ignore_index=True)
            save_data(df_pay, DATA_FILES["paiements"])
            st.success("Paiement créé✅")

elif choice=="Certifications":
    st.title("📜 Certifications")
    df_cert=load_data(DATA_FILES["certifications"], CERT_COLS)
    with st.form("fcert"):
        idc=st.text_input("ID Contact")
        tcert=st.selectbox("Type Certif",SETTINGS["types_contact"])
        dex=st.date_input("Date Examen",date.today())
        res=st.selectbox("Résultat",["Réussi","Échoué","En attente"])
        score=st.number_input("Score",min_value=0,step=1)
        dob=st.date_input("Date Obtention",date.today())
        valid=f"Calc"  # placeholder validité
        ren=f"Calc"   # placeholder renouvellement
        comm=st.text_area("Notes")
        sub=st.form_submit_button("Enregistrer")
        if sub:
            new={"ID_Certif":generate_id("CER",df_cert,"ID_Certif"),"ID_Contact":idc,
                 "Type_Certif":tcert,"Date_Examen":dex.isoformat(),"Résultat":res,"Score":score,
                 "Date_Obtention":dob.isoformat(),"Validité":valid,"Renouvellement":ren,
                 "Notes":comm,"Nom Contact":""}
            df_cert=pd.concat([df_cert,pd.DataFrame([new])],ignore_index=True)
            save_data(df_cert, DATA_FILES["certifications"])
            st.success("Certification créée✅")

# --- PARAMÈTRES DROPDOWNS ---
elif choice=="Paramètres":
    st.title("⚙️ Paramètres")
    s=load_settings()
    t1,t2,t3,t4=st.tabs(["Contacts","Localisation","Communication","Événements/Paiement"])
    with t1:
        ts=st.text_area("Types Contact","\n".join(s["types_contact"]))
        so=st.text_area("Sources","\n".join(s["sources"]))
        stts=st.text_area("Statuts Engagement","\n".join(s["statuts_engagement"]))
    with t2:
        sec=st.text_area("Secteurs","\n".join(s["secteurs"]))
        pays=st.text_area("Pays","\n".join(s["pays"]))
    with t3:
        can=st.text_area("Canaux","\n".join(s["canaux"]))
    with t4:
        ev=st.text_area("Types Événements","\n".join(s["types_evenements"]))
        mp=st.text_area("Moyens Paiement","\n".join(s["moyens_paiement"]))
    if st.button("Sauvegarder Paramètres"):
        save_settings({
            "types_contact":ts.split("\n"),"sources":so.split("\n"),"statuts_engagement":stts.split("\n"),
            "secteurs":sec.split("\n"),"pays":pays.split("\n"),"canaux":can.split("\n"),
            "types_evenements":ev.split("\n"),"moyens_paiement":mp.split("\n")
        })
        st.success("Paramètres mis à jour✅")
