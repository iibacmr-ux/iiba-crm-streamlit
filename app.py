import streamlit as st
import pandas as pd
import os, json
from datetime import datetime, date
from st_aggrid import AgGrid, GridOptionsBuilder

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")
DATA = {
    "contacts":"contacts.csv","interactions":"interactions.csv",
    "evenements":"evenements.csv","participations":"participations.csv",
    "paiements":"paiements.csv","certifications":"certifications.csv",
    "settings":"settings.json"
}
DEFAULT = {
    "statuts_paiement":["Réglé","Partiel","Non payé"],
    "resultats_inter":["Positif","Négatif","Neutre","À relancer","À suivre","Sans suite"],
    "types_contact":["Membre","Prospect","Formateur","Partenaire"],
    "sources":["Afterwork","Formation","LinkedIn","Recommandation","Site Web","Salon","Autre"],
    "statuts_engagement":["Actif","Inactif","À relancer"],
    "secteurs":["IT","Finance","Éducation","Santé","Consulting","Autre","Côte d’Ivoire","Sénégal"],
    "pays":["Cameroun","France","Canada","Belgique","Autre"],
    "canaux":["Email","Téléphone","WhatsApp","LinkedIn","Réunion","Autre"],
    "types_evenements":["Atelier","Conférence","Formation","Webinaire","Afterwork","BA MEET UP","Groupe d’étude"],
    "moyens_paiement":["Chèque","Espèces","Virement","CB","Mobile Money","Autre"]
}

# --- PARAMÈTRES ---
@st.cache_data
def load_settings():
    if os.path.exists(DATA["settings"]):
        return json.load(open(DATA["settings"], encoding="utf-8"))
    json.dump(DEFAULT, open(DATA["settings"], "w", encoding="utf-8"), indent=2)
    return DEFAULT

def save_settings(s):
    json.dump(s, open(DATA["settings"], "w", encoding="utf-8"), indent=2)
    st.cache_data.clear()

SET = load_settings()
# --- pour debugger : Tu verras instantanément quelles clés sont présentes
# ---st.write("Vérifiez vos paramètres:")
# ---st.json(SET)
# ---st.write("Clés présentes dans settings : ", list(SET.keys()))

# --- FONCTIONS DONNÉES ---
def generate_id(prefix, df, col):
    nums = [int(x.split("_")[1]) for x in df[col] if isinstance(x, str)]
    n = max(nums) if nums else 0
    return f"{prefix}_{n+1:03d}"

def load_df(file, cols):
    if os.path.exists(file):
        df = pd.read_csv(file, encoding="utf-8")
    else:
        df = pd.DataFrame(columns=cols)
    for c, v in cols.items():
        if c not in df.columns:
            df[c] = v() if callable(v) else v
    return df[list(cols.keys())]

def save_df(df, file):
    df.to_csv(file, index=False, encoding="utf-8")

# --- SCHÉMAS ---
C_COLS = {
    "ID":lambda: None,"Nom":"","Prénom":"","Genre":"","Titre":"",
    "Société":"","Secteur":SET["secteurs"][0],"Email":"","Téléphone":"",
    "Ville":"","Pays":SET["pays"][0],"Type":SET["types_contact"][0],
    "Source":SET["sources"][0],"Statut":SET.get("statuts_paiement", ["Réglé"]),
    "LinkedIn":"","Notes":"","Date_Creation":lambda: date.today().isoformat()
}
I_COLS = {
    "ID_Interaction":lambda: None,"ID":"","Date":date.today().isoformat(),
    "Canal":SET["canaux"][0],"Objet":"","Résumé":"",
    "Résultat":SET.get("resultats_inter", ["Positif"])[0][0],"Responsable":"",
    "Prochaine_Action":"","Relance":""
}
E_COLS = {
    "ID_Événement":lambda: None,"Nom_Événement":"","Type":SET["types_evenements"][0],
    "Date":date.today().isoformat(),"Durée_h":0,"Lieu":"",
    "Formateur(s)":"","Invité(s)":"","Objectif":"","Période":"Matinée","Notes":""
}
P_COLS = {
    "ID_Participation":lambda: None,"ID":"","ID_Événement":"",
    "Rôle":"Participant","Inscription":date.today().isoformat(),
    "Arrivée":"","Temps_Présent":"","Feedback":3,"Note":0,
    "Commentaire":"","Nom Participant":"","Nom Événement":""
}
PAY_COLS = {
    "ID_Paiement":lambda: None,"ID":"","ID_Événement":"",
    "Date_Paiement":date.today().isoformat(),"Montant":0.0,
    "Moyen":SET.get("statuts_paiement", ["Réglé"]),"Statut":SET.get("statuts_paiement", ["Réglé"]),
    "Référence":"","Notes":"","Relance":"","Nom Contact":"","Nom Événement":""
}
CERT_COLS = {
    "ID_Certif":lambda: None,"ID":"","Type_Certif":SET["types_contact"][0],
    "Date_Examen":date.today().isoformat(),"Résultat":"Réussi","Score":0,
    "Date_Obtention":date.today().isoformat(),"Validité":"","Renouvellement":"",
    "Notes":"","Nom Contact":""
}

# --- NAVIGATION ---
PAGES = ["Dashboard 360","Contacts","Interactions","Événements",
         "Participations","Paiements","Certifications","Paramètres"]
page = st.sidebar.selectbox("Menu", PAGES)

# --- DASHBOARD 360 ---
if page == "Dashboard 360": 
    st.title("📈 Tableau de Bord Stratégique")
    dfc=load_df(DATA["contacts"],C_COLS)
    dfi=load_df(DATA["interactions"],I_COLS)
    dfe=load_df(DATA["evenements"],E_COLS)
    dfp=load_df(DATA["participations"],P_COLS)
    dfpay=load_df(DATA["paiements"],PAY_COLS)
    dfcert=load_df(DATA["certifications"],CERT_COLS)
    # filtres année/mois
    yrs=sorted({d[:4] for d in dfc["Date_Creation"]}) or [str(date.today().year)]
    mths=["Tous"]+[f"{i:02d}" for i in range(1,13)]
    col1,col2=st.columns(2)
    yr=col1.selectbox("Année",yrs)
    mn=col2.selectbox("Mois",mths,index=0)
    def fil(df,col):
        return df[(df[col].str[:4]==yr)&((mn=="Tous")|(df[col].str[5:7]==mn))]
    dfc2,dfp2,dfpay2,dfcert2=dfl1,fil(dfp,"Inscription"),fil(dfpay,"Date_Paiement"),fil(dfcert,"Date_Obtention")
    # KPI
    c1,c2,c3,c4=st.columns(4)
    c1.metric("Prospects Actifs",len(dfc2[dfc2["Type"]=="Prospect"]))
    c1.metric("Membres IIBA",len(dfc2[dfc2["Type"]=="Membre"]))
    c2.metric("Événements",len(fil(dfe,"Date")))
    c2.metric("Participations",len(dfp2))
    benef=dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum()
    c3.metric("CA réglé",f"{benef:,.0f}")
    c3.metric("Impayés",len(dfpay2[dfpay2["Statut"]!="Réglé"]))
    c4.metric("Certifs Obtenues",len(dfcert2[dfcert2["Résultat"]=="Réussi"]))
    sc=dfp2["Feedback"].mean() if not dfp2.empty else 0
    c4.metric("Score engagement",f"{sc:.1f}")
    # export unifié
    if st.button("⬇️ Export unifié CSV"):
        uni=dfc.merge(dfi,on="ID",how="left").merge(dfp,on="ID",how="left")
        st.download_button("Télécharger",uni.to_csv(index=False),file_name="crm_union.csv")

# --- PAGE Contacts ---
elif page == "Contacts":
    st.title("👤 Contacts")
    df = load_df(DATA["contacts"], C_COLS)
    sel = st.selectbox("Sélection", [""] + df["ID"].tolist())
    rec = df[df["ID"]==sel].iloc[0] if sel else None
    with st.form("f_contacts"):
        if sel: st.text_input("ID", rec["ID"], disabled=True)
        nom = st.text_input("Nom", rec["Nom"] if rec else "")
        prenom = st.text_input("Prénom", rec["Prénom"] if rec else "")
        genre = st.selectbox("Genre", ["","Homme","Femme","Autre"],
                             index=(["","Homme","Femme","Autre"].index(rec["Genre"]) if rec else 0))
        titre = st.text_input("Titre", rec["Titre"] if rec else "")
        societe = st.text_input("Société", rec["Société"] if rec else "")
        secteur = st.selectbox("Secteur", SET["secteurs"],
                               index=(SET["secteurs"].index(rec["Secteur"]) if rec else 0))
        typec = st.selectbox("Type", SET["types_contact"],
                             index=(SET["types_contact"].index(rec["Type"]) if rec else 0))
        source = st.selectbox("Source", SET["sources"],
                              index=(SET["sources"].index(rec["Source"]) if rec else 0))
        statut = st.selectbox("Statut", SET.get("statuts_paiement", ["Réglé"]),
                              index=(SET["statuts_paiement"].index(rec["Statut"]) if rec else 0))
        email = st.text_input("Email", rec["Email"] if rec else "")
        tel = st.text_input("Téléphone", rec["Téléphone"] if rec else "")
        ville = st.text_input("Ville", rec["Ville"] if rec else "")
        pays = st.selectbox("Pays", SET["pays"],
                            index=(SET["pays"].index(rec["Pays"]) if rec else 0))
        linkedin = st.text_input("LinkedIn", rec["LinkedIn"] if rec else "")
        notes = st.text_area("Notes", rec["Notes"] if rec else "")
        dc = st.text_input("Date_Creation", rec["Date_Creation"] if rec else date.today().isoformat())
        submit = st.form_submit_button("Enregistrer")
        if submit:
            if rec is not None:
                idx = df[df["ID"]==sel].index[0]
                df.loc[idx] = [sel, nom, prenom, genre, titre, societe, secteur,
                               email, tel, ville, pays, typec, source, statut,
                               linkedin, notes, dc]
            else:
                new = {"ID":generate_id("CNT", df, "ID"),"Nom":nom,"Prénom":prenom,"Genre":genre,
                       "Titre":titre,"Société":societe,"Secteur":secteur,"Email":email,
                       "Téléphone":tel,"Ville":ville,"Pays":pays,"Type":typec,"Source":source,
                       "Statut":statut,"LinkedIn":linkedin,"Notes":notes,"Date_Creation":dc}
                df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["contacts"])
            st.success("Contact enregistré")
    if st.button("⬇️ Export Contacts CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="contacts.csv")
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Interactions ---
elif page == "Interactions":
    st.title("💬 Interactions")
    df = load_df(DATA["interactions"], I_COLS)
    dfc = load_df(DATA["contacts"], C_COLS)
    opts = [""] + dfc["ID"].tolist()
    with st.form("f_inter"):
        idc = st.selectbox("ID Contact", opts)
        date_i = st.date_input("Date", date.today())
        canal = st.selectbox("Canal", SET["canaux"])
        objet = st.text_input("Objet", "")
        resume = st.text_area("Résumé", "")
        resultat = st.selectbox("Résultat", SET.get("resultats_inter", ["Positif"])[0])
        responsable = st.text_input("Responsable", "")
        pa = st.text_area("Prochaine_Action", "")
        rel = st.date_input("Relance (opt.)", value=None)
        sub = st.form_submit_button("Enregistrer")
        if sub and idc:
            new = {"ID_Interaction":generate_id("INT",df,"ID_Interaction"),"ID":idc,
                   "Date":date_i.isoformat(),"Canal":canal,"Objet":objet,"Résumé":resume,
                   "Résultat":resultat,"Responsable":responsable,
                   "Prochaine_Action":pa,"Relance":(rel.isoformat() if rel else "")}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["interactions"])
            st.success("Interaction enregistrée")
    if st.button("⬇️ Export Interactions CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="interactions.csv")
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Événements ---
elif page == "Événements":
    st.title("📅 Événements")
    df = load_df(DATA["evenements"], E_COLS)
    with st.form("f_event"):
        nom = st.text_input("Nom Événement", "")
        typ = st.selectbox("Type", SET["types_evenements"])
        dt = st.date_input("Date", date.today())
        duree = st.number_input("Durée (h)", min_value=0.0, step=0.5)
        lieu = st.text_input("Lieu", "")
        form = st.text_area("Formateur(s)", "")
        inv = st.text_area("Invité(s)", "")
        obj = st.text_area("Objectif", "")
        per = st.selectbox("Période", ["Matinée","Après-midi","Journée"])
        notes = st.text_area("Notes", "")
        sub = st.form_submit_button("Enregistrer")
        if sub:
            new = {"ID_Événement":generate_id("EVT",df,"ID_Événement"),"Nom_Événement":nom,
                   "Type":typ,"Date":dt.isoformat(),"Durée_h":duree,"Lieu":lieu,
                   "Formateur(s)":form,"Invité(s)":inv,"Objectif":obj,"Période":per,"Notes":notes}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["evenements"])
            st.success("Événement enregistré")
    if st.button("⬇️ Export Événements CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="evenements.csv")
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Participations ---
elif page == "Participations":
    st.title("🙋 Participations")
    df = load_df(DATA["participations"], P_COLS)
    dfc = load_df(DATA["contacts"], C_COLS)
    dfe = load_df(DATA["evenements"], E_COLS)
    opts_c = [""] + dfc["ID"].tolist()
    opts_e = [""] + dfe["ID_Événement"].tolist()
    with st.form("f_part"):
        idc = st.selectbox("ID Contact", opts_c)
        ide = st.selectbox("ID Événement", opts_e)
        role = st.selectbox("Rôle", ["Participant","Organisateur","Formateur","Invité"])
        ins = st.date_input("Inscription", date.today())
        arr = st.text_input("Arrivée (hh:mm)", "")
        feedback = st.slider("Feedback", 1, 5, 3)
        note = st.number_input("Note", min_value=0, max_value=20)
        comm = st.text_area("Commentaire", "")
        sub = st.form_submit_button("Enregistrer")
        if sub and idc and ide:
            new = {"ID_Participation":generate_id("PAR",df,"ID_Participation"),
                   "ID":idc,"ID_Événement":ide,"Rôle":role,
                   "Inscription":ins.isoformat(),"Arrivée":arr,
                   "Temps_Présent":"AUTO","Feedback":feedback,"Note":note,
                   "Commentaire":comm,"Nom Participant":"","Nom Événement":""}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["participations"])
            st.success("Participation enregistrée")
    if st.button("⬇️ Export Participations CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="participations.csv")
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Paiements ---
elif page == "Paiements":
    st.title("💳 Paiements")
    df = load_df(DATA["paiements"], PAY_COLS)
    with st.form("f_pay"):
        idc = st.text_input("ID Contact", "")
        ide = st.text_input("ID Événement", "")
        dp = st.date_input("Date Paiement", date.today())
        mont = st.number_input("Montant", min_value=0.0, step=100.0)
        moy = st.selectbox("Moyen", SET["moyens_paiement"])
        stat = st.selectbox("Statut", SET.get("statuts_paiement", ["Réglé"]))
        ref = st.text_input("Référence", "")
        notes = st.text_area("Notes", "")
        rel = st.date_input("Relance (opt.)", value=None)
        sub = st.form_submit_button("Enregistrer")
        if sub and idc and ide:
            new = {"ID_Paiement":generate_id("PAY",df,"ID_Paiement"),"ID":idc,
                   "ID_Événement":ide,"Date_Paiement":dp.isoformat(),"Montant":mont,
                   "Moyen":moy,"Statut":stat,"Référence":ref,"Notes":notes,
                   "Relance":(rel.isoformat() if rel else ""),"Nom Contact":"","Nom Événement":""}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["paiements"])
            st.success("Paiement enregistré")
    if st.button("⬇️ Export Paiements CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="paiements.csv")
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Certifications ---
elif page == "Certifications":
    st.title("📜 Certifications")
    df = load_df(DATA["certifications"], CERT_COLS)
    with st.form("f_cert"):
        idc = st.text_input("ID Contact", "")
        tc = st.selectbox("Type Certif", SET["types_contact"])
        de = st.date_input("Date Examen", date.today())
        res = st.selectbox("Résultat", ["Réussi","Échoué","En attente"])
        score = st.number_input("Score", min_value=0, step=1)
        dob = st.date_input("Date Obtention", date.today())
        valid = "AUTO"
        ren = "AUTO"
        notes = st.text_area("Notes", "")
        sub = st.form_submit_button("Enregistrer")
        if sub and idc:
            new = {"ID_Certif":generate_id("CER",df,"ID_Certif"),"ID":idc,
                   "Type_Certif":tc,"Date_Examen":de.isoformat(),"Résultat":res,
                   "Score":score,"Date_Obtention":dob.isoformat(),
                   "Validité":valid,"Renouvellement":ren,"Notes":notes,"Nom Contact":""}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["certifications"])
            st.success("Certification enregistrée")
    if st.button("⬇️ Export Certifications CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="certifications.csv")
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Paramètres ---
elif page == "Paramètres":
    st.title("⚙️ Paramètres")
    s = SET
    t1, t2 = st.tabs(["Référentiels","Général"])
    with t1:
        for key in DEFAULT:
            val = "\n".join(s[key])
            s[key] = st.text_area(key, val).split("\n")
    if st.button("Sauvegarder Paramètres"):
        save_settings(s)
        st.success("Paramètres mis à jour")
