import streamlit as st
import pandas as pd
import os, json
from datetime import date, datetime, timedelta
import altair as alt
from st_aggrid import AgGrid, GridOptionsBuilder

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

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

SET = load_settings()

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
PAGES=["Dashboard 360","Contacts","Interactions","Événements",
       "Participations","Paiements","Certifications","Rapports","Paramètres"]
page = st.sidebar.selectbox("Menu", PAGES)

# --- PAGE Dashboard 360 ---
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

# --- PAGE Rapports ---
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
