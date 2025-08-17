import streamlit as st
import pandas as pd
import os, json
from datetime import date, datetime, timedelta
import altair as alt
from st_aggrid import AgGrid, GridOptionsBuilder

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

# Ajout du style personnalisé CSS pour améliorer l’apparence des textareas et expander
# CSS pour UI moderne
st.markdown("""
<style>
textarea {background:#f7f9fa;border-radius:7px;margin-bottom:12px;}
div[data-testid="stExpander"]{background:#fffbea;border-radius:7px;padding:10px;margin-bottom:15px;}
.kpi-card{background:#e0f7fa;border-radius:7px;padding:15px;text-align:center;margin:5px;}
.header-logo {display:flex; align-items:center;}
.header-logo img{height:40px; margin-right:10px;}
</style>
""", unsafe_allow_html=True)

# --- AUTHENTIFICATION BASIQUE ---
PASSWORD = st.secrets.get("APP_PASSWORD", "")
if PASSWORD:
    pwd = st.sidebar.text_input("🔒 Mot de passe", type="password")
    if pwd != PASSWORD:
        st.error("Mot de passe incorrect")
        st.stop()

# --- FICHIERS & RÉFÉRENTIELS ---
DATA = {
    "contacts":"contacts.csv","interactions":"interactions.csv",
    "evenements":"evenements.csv","participations":"participations.csv",
    "paiements":"paiements.csv","certifications":"certifications.csv",
    "settings":"settings.json","audit":"audit.log"
}
DEFAULT = {
    "statuts_paiement":["Réglé","Partiel","Non payé"],
    "resultats_inter":["Positif","Négatif","Neutre","À relancer","À suivre","Sans suite"],
    "types_contact":["Membre","Prospect","Formateur","Partenaire"],
    "sources":["Afterwork","Formation","LinkedIn","Recommandation","Site Web","Salon","Autre"],
    "statuts_engagement":["Actif","Inactif","À relancer"],
    "secteurs":["IT","Finance","Éducation","Santé","Consulting","Autre"],
    "pays":["Cameroun","France","Canada","Belgique","Autre","Côte d’Ivoire","Sénégal"],
    "canaux":["Email","Téléphone","WhatsApp","LinkedIn","Réunion","Autre"],
    "types_evenements":["Atelier","Conférence","Formation","Webinaire","Afterwork","BA MEET UP","Groupe d’étude"],
    "moyens_paiement":["Chèque","Espèces","Virement","CB","Mobile Money","Autre"],
    "types_certif":["ECBA","CCBA","CBAP"],
    "entreprises_cibles":["Dangote","MUPECI","SALAM","Orange","MTN","Société Générale","Ecobank","UBA","BGFI","CCA"]
}

# --- PARAMÈTRES --- 
# --- LOAD / SAVE SETTINGS & AUDIT ---
def load_settings():
    if os.path.exists(DATA["settings"]):
        return json.load(open(DATA["settings"],encoding="utf-8"))
    json.dump(DEFAULT,open(DATA["settings"],"w",encoding="utf-8"),indent=2)
    return DEFAULT

def save_settings(s):
    json.dump(s,open(DATA["settings"],"w",encoding="utf-8"),indent=2)
    log_action("Mise à jour paramètres")

def log_action(msg):
    with open(DATA["audit"],"a") as f:
        f.write(f"{datetime.now().isoformat()} - {msg}\n")
    json.dump(s, open(DATA["settings"], "w", encoding="utf-8"), indent=2)
    st.cache_data.clear()


SET = load_settings()
# --- pour debugger : Tu verras instantanément quelles clés sont présentes
# ---st.write("Vérifiez vos paramètres:")
# ---st.json(SET)
# ---st.write("Clés présentes dans settings : ", list(SET.keys()))
# Après SET=load_settings()
for key in ["statuts_paiement", "resultats_inter", "types_contact", "sources", "statuts_engagement",
            "secteurs", "pays", "canaux", "types_evenements", "moyens_paiement", "types_certif"]:
    if key not in SET:
        # Insérer la valeur par défaut
        default_val = DEFAULT.get(key, ["ECBA","CCBA","CBAP"] if key=="types_certif" else ["None"])
        SET[key] = default_val if isinstance(default_val, list) else [default_val]


# --- FONCTIONS DONNÉES ---
# --- UTILITAIRES DATA ---
def generate_id(pref, df, col):
    nums = [int(x.split("_")[1]) for x in df[col] if isinstance(x,str)]
    return f"{pref}_{(max(nums) if nums else 0)+1:03d}"

def load_df(file, schema):
    df = pd.read_csv(file,encoding="utf-8") if os.path.exists(file) else pd.DataFrame(columns=schema)
    for c,default in schema.items():
        if c not in df.columns:
            df[c] = default() if callable(default) else default
    return df[list(schema.keys())]

def save_df(df,file):
    df.to_csv(file,index=False,encoding="utf-8")

# --- SCHÉMAS ---
# --- SCHÉMAS ENTITÉS ---
C_SCHEMA = {
    "ID":lambda:None,"Nom":"","Prénom":"","Genre":"","Titre":"",
    "Société":"","Top20":False,"Secteur":SET["secteurs"][0],"Email":"","Téléphone":"",
    "Ville":"","Pays":SET["pays"],"Type":SET["types_contact"],
    "Source":SET["sources"],"Statut":SET["statuts_engagement"],
    "LinkedIn":"","Notes":"", "Score":0,"Certifié":False,
    "Date_Creation":lambda:date.today().isoformat()
}
I_SCHEMA = {
    "ID_Interaction":lambda:None,"ID":"","Date":date.today().isoformat(),
    "Canal":SET["canaux"][0],"Objet":"","Résumé":"",
    "Résultat":SET["resultats_inter"],"Responsable":"",
    "Prochaine_Action":"","Relance":""
}
E_SCHEMA = {
    "ID_Événement":lambda:None,"Nom_Événement":"","Type":SET["types_evenements"][0],
    "Date":date.today().isoformat(),"Durée_h":0.0,"Lieu":"",
    "Formateur(s)":"","Invité(s)":"","Objectif":"","Période":"Matinée","Notes":"",
    "Coût_Total":0.0,"Coût_Salle":0.0,"Coût_Formateur":0.0,"Coût_Logistique":0.0,"Coût_Pub":0.0
}
P_SCHEMA = {
    "ID_Participation":lambda:None,"ID":"","ID_Événement":"",
    "Rôle":"Participant","Inscription":date.today().isoformat(),
    "Arrivée":"", "Temps_Présent":"AUTO","Feedback":3,"Note":0,
    "Commentaire":""
}
PAY_SCHEMA = {
    "ID_Paiement":lambda:None,"ID":"","ID_Événement":"",
    "Date_Paiement":date.today().isoformat(),"Montant":0.0,
    "Moyen":SET["moyens_paiement"][0],"Statut":SET["statuts_paiement"],
    "Référence":"","Notes":"","Relance":""
}
CERT_SCHEMA = {
    "ID_Certif":lambda:None,"ID":"","Type_Certif":SET["types_certif"][0],
    "Date_Examen":date.today().isoformat(),"Résultat":"Réussi","Score":0,
    "Date_Obtention":date.today().isoformat(),"Validité":"","Renouvellement":"",
    "Notes":""
}

# --- NAVIGATION ---
PAGES = ["Dashboard 360","Contacts","Interactions","Événements",
         "Participations","Paiements","Certifications","Paramètres"]
page = st.sidebar.selectbox("Menu", PAGES)

# --- DASHBOARD 360 ---
if page=="Dashboard 360":
    st.markdown('<div class="header-logo"><img src="https://iiba.org/Logo.png"/> <h1>Tableau de Bord</h1></div>', unsafe_allow_html=True)
    dfc = load_df(DATA["contacts"],C_SCHEMA)
    dfi = load_df(DATA["interactions"],I_SCHEMA)
    dfe = load_df(DATA["evenements"],E_SCHEMA)
    dfp = load_df(DATA["participations"],P_SCHEMA)
    dfpay = load_df(DATA["paiements"],PAY_SCHEMA)
    dfcert = load_df(DATA["certifications"],CERT_SCHEMA)

    # Filtres temporels
    yrs=sorted({d[:4] for d in dfc["Date_Creation"]}) or [str(date.today().year)]
    mths=["Tous"]+[f"{i:02d}" for i in range(1,13)]
    col1,col2=st.columns(2)
    yr=col1.selectbox("Année", yrs); mn=col2.selectbox("Mois", mths)

    def fil(df,col):
        df2=df[df[col].str[:4]==yr]
        return df2 if mn=="Tous" else df2[df2[col].str[5:7]==mn]

    dfc2,dfp2,dfpay2,dfcert2=fil(dfc,"Date_Creation"),fil(dfp,"Inscription"),fil(dfpay,"Date_Paiement"),fil(dfcert,"Date_Obtention")

    # Calcul scores
    dfc2["Score"] = (
        dfp2.groupby("ID").size().reindex(dfc2["ID"], fill_value=0)*1 +
        dfpay2[dfpay2["Statut"]=="Réglé"].groupby("ID").size().reindex(dfc2["ID"],fill_value=0)*2 +
        dfcert2[dfcert2["Résultat"]=="Réussi"].groupby("ID").size().reindex(dfc2["ID"],fill_value=0)*3
    )

    # KPI Cards
    cards=st.columns(4)
    cards[0].metric("Prospects convertis", len(dfc2[(dfc2["Type"]=="Prospect")&(dfc2["Statut"]=="Réglé")]))
    rate=len(dfc2[(dfc2["Type"]=="Prospect")&(dfc2["Statut"]=="Réglé")])/max(len(dfc2[dfc2["Type"]=="Prospect"]),1)
    cards[0].metric("Taux conv.",f"{rate:.1%}")
    cards[1].metric("Événements", len(fil(dfe,"Date")))
    cards[1].metric("Participations", len(dfp2))
    ca=dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum()
    cards[2].metric("CA réglé",f"{ca:,.0f} FCFA")
    imp=len(dfpay2[dfpay2["Statut"]!="Réglé"])
    cards[2].metric("Impayés",imp)
    cards[3].metric("Certifs réussies",len(dfcert2[dfcert2["Résultat"]=="Réussi"]))
    cards[3].metric("Score moy.",f"{dfc2['Score'].mean():.1f}")

    # ROI Chart
    rev=dfpay2[dfpay2["Statut"]=="Réglé"].groupby("ID_Événement")["Montant"].sum().reset_index()
    dfe2=fil(dfe,"Date").copy()
    dfe2["Recettes"]=dfe2["ID_Événement"].map(rev.set_index("ID_Événement")["Montant"])
    dfe2["Bénéfice"]=dfe2["Recettes"]-dfe2["Coût_Total"]
    chart=alt.Chart(dfe2).mark_bar().encode(x="Nom_Événement",y="Bénéfice",color="Bénéfice")
    st.altair_chart(chart,use_container_width=True)

# --- PAGE Contacts ---
elif page=="Contacts":
    st.header("👤 Contacts")
    df=load_df(DATA["contacts"],C_SCHEMA)
    # Scoring & Top20 flag
    df["Top20"]=df["Société"].isin(SET["entreprises_cibles"])
    df["Certifié"]=df["ID"].isin(load_df(DATA["certifications"],CERT_SCHEMA).query("Résultat=='Réussi'")["ID"])
    # Grille
    gb=GridOptionsBuilder.from_dataframe(df); gb.configure_default_column(sortable=True,filterable=True)
    AgGrid(df,gridOptions=gb.build(),height=300)
    # Fiche 360
    sel=st.selectbox("Sélection contact",[""]+df["ID"].tolist())
    if sel:
        rec=df[df["ID"]==sel].iloc[0]
        st.subheader(f"{rec['Nom']} {rec['Prénom']} ({rec['Score']:.0f})")
        # Interactions
        st.markdown("**Dernières interactions**")
        dfi=load_df(DATA["interactions"],I_SCHEMA)
        st.table(dfi.query("ID==@sel").tail(5)[["Date","Objet","Résultat"]])
        # Participations
        st.markdown("**Participations**")
        dfp=load_df(DATA["participations"],P_SCHEMA)
        st.table(dfp.query("ID==@sel")[["Inscription","ID_Événement"]])
        # Paiements
        st.markdown("**Paiements**")
        dfpay=load_df(DATA["paiements"],PAY_SCHEMA)
        st.table(dfpay.query("ID==@sel")[["Date_Paiement","Montant","Statut"]])
        # Certifications
        st.markdown("**Certifications**")
        dfcert=load_df(DATA["certifications"],CERT_SCHEMA)
        st.table(dfcert.query("ID==@sel")[["Type_Certif","Date_Obtention","Résultat"]])

# --- PAGE Interactions ---
elif page == "Interactions":
    st.title("💬 Interactions")
    df = load_df(DATA["interactions"], I_SCHEMA)
    dfc = load_df(DATA["contacts"], C_SCHEMA)
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
    df = load_df(DATA["evenements"], E_SCHEMA)
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
    df = load_df(DATA["participations"], P_SCHEMA)
    dfc = load_df(DATA["contacts"], C_SCHEMA)
    dfe = load_df(DATA["evenements"], E_SCHEMA)
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
    df = load_df(DATA["paiements"], PAY_SCHEMA)
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
    df = load_df(DATA["certifications"], CERT_SCHEMA)
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

elif page=="Rapports":
    st.header("📊 Rapports Stratégiques")
    # Prospects réguliers non convertis
    dfp=load_df(DATA["participations"],P_SCHEMA)
    dfpay=load_df(DATA["paiements"],PAY_SCHEMA)
    dfc=load_df(DATA["contacts"],C_SCHEMA)
    cntp=dfp.groupby("ID").size().reset_index(name="NbPart")
    prospects=dfc.query("Type=='Prospect'")
    reg=prospects.merge(cntp,on="ID").query("NbPart>=3 and ID not in @dfpay.query(\"Statut=='Réglé'\")['ID']")
    st.subheader("Prospects réguliers non convertis")
    st.table(reg[["ID","Nom","NbPart"]])
    # Top20 entreprises
    top20=dfc[dfc["Top20"]]
    st.subheader("Contacts Top20 entreprises")
    st.metric("Total Top20",len(top20))
    # Relances urgentes
    st.subheader("Relances urgentes")
    dfi=load_df(DATA["interactions"],I_SCHEMA)
    today=date.today().isoformat()
    urg=dfi[dfi["Relance"]<today]
    st.table(urg[["ID_Interaction","ID","Relance"]])

# --- PAGE Paramètres ---
elif page=="Paramètres":
    st.header("⚙️ Paramètres")
    col1,col2=st.columns(2)
    with col1:
        sp="\n".join(SET["statuts_paiement"])
        SET["statuts_paiement"]=st.text_area("statuts_paiement",sp).split("\n")
        ri="\n".join(SET["resultats_inter"])
        SET["resultats_inter"]=st.text_area("resultats_inter",ri).split("\n")
        tc="\n".join(SET["types_contact"])
        SET["types_contact"]=st.text_area("types_contact",tc).split("\n")
        sc="\n".join(SET["sources"])
        SET["sources"]=st.text_area("sources",sc).split("\n")
    with col2:
        se="\n".join(SET["statuts_engagement"])
        SET["statuts_engagement"]=st.text_area("statuts_engagement",se).split("\n")
        sec="\n".join(SET["secteurs"])
        SET["secteurs"]=st.text_area("secteurs",sec).split("\n")
        py="\n".join(SET["pays"])
        SET["pays"]=st.text_area("pays",py).split("\n")
        cc="\n".join(SET["entreprises_cibles"])
        SET["entreprises_cibles"]=st.text_area("entreprises_cibles",cc).split("\n")
    if st.button("💾 Sauvegarder"):
        save_settings(SET)
        st.success("Paramètres mis à jour")
