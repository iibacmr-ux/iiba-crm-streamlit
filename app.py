import streamlit as st
import pandas as pd
import os, json
from datetime import datetime, date, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")
DATA = {
    "contacts":"contacts.csv","interactions":"interactions.csv",
    "evenements":"evenements.csv","participations":"participations.csv",
    "paiements":"paiements.csv","certifications":"certifications.csv",
    "settings":"settings.json"
}

# --- RÉFÉRENTIELS PAR DÉFAUT ---
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

# --- CHARGEMENT / SAUVEGARDE PARAMÈTRES ---
@st.cache_data
def load_settings():
    if os.path.exists(DATA["settings"]):
        return json.load(open(DATA["settings"],encoding="utf-8"))
    json.dump(DEFAULT, open(DATA["settings"],"w",encoding="utf-8"),indent=2)
    return DEFAULT

def save_settings(s):
    json.dump(s, open(DATA["settings"],"w",encoding="utf-8"),indent=2)
    st.cache_data.clear()

SET = load_settings()

# --- FONCTIONS DE DONNÉES ---
def generate_id(prefix, df, col):
    nums=[int(x.split("_")[1]) for x in df[col] if isinstance(x,str)]
    n=max(nums) if nums else 0
    return f"{prefix}_{n+1:03d}"

def load_df(file, cols):
    if os.path.exists(file):
        df=pd.read_csv(file,encoding="utf-8")
    else: df=pd.DataFrame(columns=cols)
    # Migration colonnes manquantes
    for c,v in cols.items():
        if c not in df.columns:
            df[c]=v if not callable(v) else v()
    return df[list(cols.keys())]

def save_df(df,file):
    df.to_csv(file,index=False,encoding="utf-8")

# --- DÉFINITIONS DES SCHÉMAS ---
C_COLS={ "ID":lambda:None,"Nom":"","Prénom":"","Genre":"","Titre":"",
 "Société":"","Secteur":SET["secteurs"][0],"Email":"","Téléphone":"",
 "Ville":"","Pays":SET["pays"],"Type":SET["types_contact"],
 "Source":SET["sources"],"Statut":SET["statuts_paiement"],
 "LinkedIn":"","Notes":"","Date_Creation":lambda:date.today().isoformat()}

I_COLS={ "ID_Interaction":lambda:None,"ID": "","Date":date.today().isoformat(),
 "Canal":SET["canaux"],"Objet":"","Résumé":"",
 "Résultat":SET["resultats_inter"],"Responsable":"",
 "Prochaine_Action":"","Relance":""}

E_COLS={ "ID_Événement":lambda:None,"Nom_Événement":"","Type":SET["types_evenements"],
 "Date":date.today().isoformat(),"Durée_h":0,"Lieu":"",
 "Formateur(s)":"","Invité(s)":"","Objectif":"","Période":"Matinée","Notes":""}

P_COLS={ "ID_Participation":lambda:None,"ID":"","ID_Événement":"",
 "Rôle":"Participant","Inscription":date.today().isoformat(),
 "Arrivée":"","Temps_Présent":"","Feedback":3,"Note":0,
 "Commentaire":"","Nom Participant":"","Nom Événement":""}

PAY_COLS={ "ID_Paiement":lambda:None,"ID":"","ID_Événement":"",
 "Date_Paiement":date.today().isoformat(),"Montant":0.0,
 "Moyen":SET["moyens_paiement"],"Statut":SET["statuts_paiement"],
 "Référence":"","Notes":"","Relance":"","Nom Contact":"","Nom Événement":""}

CERT_COLS={ "ID_Certif":lambda:None,"ID":"","Type_Certif":SET["types_contact"],
 "Date_Examen":date.today().isoformat(),"Résultat":"Réussi","Score":0,
 "Date_Obtention":date.today().isoformat(),"Validité":"","Renouvellement":"",
 "Notes":"","Nom Contact":""}

# --- NAVIGATION ---
PAGES=["Dashboard 360","Contacts","Interactions","Événements",
       "Participations","Paiements","Certifications","Paramètres"]
page=st.sidebar.selectbox("Menu",PAGES)

# --- DASHBOARD 360 ---
if page=="Dashboard 360":
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
elif page=="Contacts":
    st.title("👤 Gestion Contacts")
    df=load_df(DATA["contacts"],C_COLS)
    sel=st.selectbox("Sélection",[""]+df["ID"].tolist())
    rec=df[df["ID"]==sel].iloc[0] if sel else None
    with st.form("f"):
        if sel: st.text_input("ID",rec["ID"],disabled=True)
        n=st.text_input("Nom",rec["Nom"] if rec else "")
        pr=st.text_input("Prénom",rec["Prénom"] if rec else "")
        g=st.selectbox("Genre",["","Homme","Femme","Autre"],index=(["","Homme","Femme","Autre"].index(rec["Genre"]) if rec else 0))
        t=st.text_input("Titre",rec["Titre"] if rec else "")
        soc=st.text_input("Société",rec["Société"] if rec else "")
        sec=st.selectbox("Secteur",SET["secteurs"],index=(SET["secteurs"].index(rec["Secteur"]) if rec else 0))
        tp=st.selectbox("Type",SET["types_contact"],index=(SET["types_contact"].index(rec["Type"]) if rec else 0))
        so=st.selectbox("Source",SET["sources"],index=(SET["sources"].index(rec["Source"]) if rec else 0))
        stt=st.selectbox("Statut",SET["statuts_paiement"],index=(SET["statuts_paiement"].index(rec["Statut"]) if rec else 0))
        em=st.text_input("Email",rec["Email"] if rec else "")
        ph=st.text_input("Téléphone",rec["Téléphone"] if rec else "")
        vi=st.text_input("Ville",rec["Ville"] if rec else "")
        pa=st.selectbox("Pays",SET["pays"],index=(SET["pays"].index(rec["Pays"]) if rec else 0))
        ln=st.text_input("LinkedIn",rec["LinkedIn"] if rec else "")
        no=st.text_area("Notes",rec["Notes"] if rec else "")
        dc=st.text_input("Date Création",rec["Date_Creation"] if rec else date.today().isoformat())
        sub=st.form_submit_button("OK")
        if sub:
            if sel:
                idx=df[df["ID"]==sel].index[0]
                df.loc[idx]=[sel,n,pr,g,t,soc,sec,em,ph,vi,pa,tp,so,stt,ln,no,dc]
            else:
                new={"ID":generate_id("CNT",df,"ID"),"Nom":n,"Prénom":pr,"Genre":g,"Titre":t,"Société":soc,"Secteur":sec,
                     "Email":em,"Téléphone":ph,"Ville":vi,"Pays":pa,"Type":tp,"Source":so,"Statut":stt,
                     "LinkedIn":ln,"Notes":no,"Date_Creation":dc}
                df=pd.concat([df,pd.DataFrame([new])],ignore_index=True)
            save_df(df,DATA["contacts"])
            st.success("Enregistré")
    # export CSV & AgGrid
    if st.button("⬇️ Export Contacts CSV"):
        st.download_button("Download",df.to_csv(index=False),file_name="contacts.csv")
    gb=GridOptionsBuilder.from_dataframe(df);gb.configure_default_column(filterable=True,sortable=True);AgGrid(df,gridOptions=gb.build())

# --- Similar blocks for Interactions, Événements, Participations, Paiements, Certifications ---
# (Omitted for brevity: follow same pattern: load_df, form, save_df, export CSV, AgGrid)

# --- PAGE Paramètres ---
elif page=="Paramètres":
    st.title("⚙️ Paramètres")
    s=SET
    t1,t2=st.tabs(["Référentiels","Général"])
    with t1:
        for key in DEFAULT:
            val="\n".join(s[key])
            s[key]=st.text_area(key,val).split("\n")
    if st.button("Sauvegarder"):
        save_settings(s);st.success("Paramètres mis à jour")
