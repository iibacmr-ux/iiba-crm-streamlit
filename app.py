import streamlit as st
import pandas as pd
import os, json
from datetime import datetime, date, timedelta
import altair as alt
from st_aggrid import AgGrid, GridOptionsBuilder

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

# CSS pour style
st.markdown("""
<style>
textarea {background:#f7f9fa;border-radius:7px;margin-bottom:12px;}
div[data-testid="stExpander"]{background:#fffbea;border-radius:7px;padding:10px;margin-bottom:15px;}
.kpi-card{background:#e0f7fa;border-radius:7px;padding:15px;text-align:center;margin:5px;}
</style>
""", unsafe_allow_html=True)

# --- FICHIERS & RÉFÉRENTIELS ---
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
  "secteurs":["IT","Finance","Éducation","Santé","Consulting","Autre"],
  "pays":["Cameroun","France","Canada","Belgique","Autre","Côte d’Ivoire","Sénégal"],
  "canaux":["Email","Téléphone","WhatsApp","LinkedIn","Réunion","Autre"],
  "types_evenements":["Atelier","Conférence","Formation","Webinaire","Afterwork","BA MEET UP","Groupe d’étude"],
  "moyens_paiement":["Chèque","Espèces","Virement","CB","Mobile Money","Autre"],
  "types_certif":["ECBA","CCBA","CBAP"]
}

# --- LOAD / SAVE SETTINGS ---
def load_settings():
    if os.path.exists(DATA["settings"]):
        return json.load(open(DATA["settings"], encoding="utf-8"))
    json.dump(DEFAULT, open(DATA["settings"], "w", encoding="utf-8"), indent=2)
    return DEFAULT

def save_settings(s):
    json.dump(s, open(DATA["settings"], "w", encoding="utf-8"), indent=2)

SET = load_settings()

# --- UTILITAIRES DATA ---
def generate_id(pref, df, col):
    nums = [int(x.split("_")[1]) for x in df[col] if isinstance(x, str)]
    return f"{pref}_{(max(nums) if nums else 0)+1:03d}"

def load_df(file, schema):
    df = pd.read_csv(file, encoding="utf-8") if os.path.exists(file) else pd.DataFrame(columns=schema)
    for c, default in schema.items():
        if c not in df.columns:
            df[c] = default() if callable(default) else default
    return df[list(schema.keys())]

def save_df(df, file):
    df.to_csv(file, index=False, encoding="utf-8")

# --- SCHÉMAS ENTITÉS ---
C_SCHEMA = {
  "ID":lambda:None, "Nom":"", "Prénom":"", "Genre":"", "Titre":"",
  "Société":"", "Secteur":SET["secteurs"], "Email":"", "Téléphone":"",
  "Ville":"", "Pays":SET["pays"], "Type":SET["types_contact"],
  "Source":SET["sources"], "Statut":SET["statuts_engagement"],
  "LinkedIn":"", "Notes":"", "Date_Creation":lambda:date.today().isoformat()
}

I_SCHEMA = {
  "ID_Interaction":lambda:None, "ID":"", "Date":date.today().isoformat(),
  "Canal":SET["canaux"], "Objet":"", "Résumé":"",
  "Résultat":SET["resultats_inter"], "Responsable":"",
  "Prochaine_Action":"", "Relance":""
}

E_SCHEMA = {
  "ID_Événement":lambda:None, "Nom_Événement":"", "Type":SET["types_evenements"],
  "Date":date.today().isoformat(), "Durée_h":0.0, "Lieu":"",
  "Formateur(s)":"", "Invité(s)":"", "Objectif":"", "Période":"Matinée","Notes":"",
  "Coût_Total":0.0, "Coût_Salle":0.0, "Coût_Formateur":0.0, "Coût_Logistique":0.0, "Coût_Pub":0.0
}

P_SCHEMA = {
  "ID_Participation":lambda:None, "ID":"", "ID_Événement":"",
  "Rôle":"Participant", "Inscription":date.today().isoformat(),
  "Arrivée":"", "Temps_Présent":"AUTO", "Feedback":3, "Note":0,
  "Commentaire":"", "Nom Participant":"", "Nom Événement":""
}

PAY_SCHEMA = {
  "ID_Paiement":lambda:None, "ID":"", "ID_Événement":"",
  "Date_Paiement":date.today().isoformat(), "Montant":0.0,
  "Moyen":SET["moyens_paiement"], "Statut":SET["statuts_paiement"],
  "Référence":"", "Notes":"", "Relance":"", "Nom Contact":"", "Nom Événement":""
}

CERT_SCHEMA = {
  "ID_Certif":lambda:None, "ID":"", "Type_Certif":SET["types_certif"],
  "Date_Examen":date.today().isoformat(), "Résultat":"Réussi", "Score":0,
  "Date_Obtention":date.today().isoformat(), "Validité":"", "Renouvellement":"",
  "Notes":"", "Nom Contact":""
}

# --- NAVIGATION ---
PAGES = ["Dashboard 360","Contacts","Interactions","Événements",
         "Participations","Paiements","Certifications","Paramètres"]
page = st.sidebar.selectbox("Menu", PAGES)

# --- PAGE Dashboard 360 ---
if page == "Dashboard 360":
    st.title("📈 Tableau de Bord Stratégique")
    # Chargement data
    dfc = load_df(DATA["contacts"], C_SCHEMA)
    dfi = load_df(DATA["interactions"], I_SCHEMA)
    dfe = load_df(DATA["evenements"], E_SCHEMA)
    dfp = load_df(DATA["participations"], P_SCHEMA)
    dfpay = load_df(DATA["paiements"], PAY_SCHEMA)
    dfcert = load_df(DATA["certifications"], CERT_SCHEMA)
    # Filtres temporels
    yrs = sorted({d[:4] for d in dfc["Date_Creation"]}) or [str(date.today().year)]
    mths = ["Tous"]+[f"{i:02d}" for i in range(1,13)]
    c1, c2 = st.columns(2)
    yr = c1.selectbox("Année", yrs)
    mn = c2.selectbox("Mois", mths)
    def fil(df, col):
        df2 = df[df[col].str[:4]==yr]
        return df2 if mn=="Tous" else df2[df2[col].str[5:7]==mn]
    dfc2 = fil(dfc, "Date_Creation")
    dfp2 = fil(dfp, "Inscription")
    dfpay2 = fil(dfpay, "Date_Paiement")
    dfcert2 = fil(dfcert, "Date_Obtention")
    # KPI cards
    cards = st.columns(4)
    cards[0].metric("Prospects convertis", len(dfc2[(dfc2["Type"]=="Prospect")&(dfc2["Statut"]=="Réglé")]))
    rate = (len(dfc2[(dfc2["Type"]=="Prospect")&(dfc2["Statut"]=="Réglé")])/max(len(dfc2[dfc2["Type"]=="Prospect"]),1))
    cards.metric("Taux conv.", f"{rate:.1%}")
    cards[1].metric("Événements", len(fil(dfe, "Date")))
    cards[1].metric("Participations", len(dfp2))
    ca = dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum()
    cards[2].metric("CA réglé", f"{ca:,.0f} FCFA")
    cards[2].metric("Impayés", len(dfpay2[dfpay2["Statut"]!="Réglé"]))
    cards[3].metric("Certifs réussies", len(dfcert2[dfcert2["Résultat"]=="Réussi"]))
    cards[3].metric("Score moy.", f"{dfp2['Feedback'].mean() if not dfp2.empty else 0:.1f}")
    # Bénéfice et ROI par événement
    rev = dfpay2[dfpay2["Statut"]=="Réglé"].groupby("ID_Événement")["Montant"].sum().reset_index()
    dfe2 = fil(dfe, "Date").copy()
    dfe2["Recettes"] = dfe2["ID_Événement"].map(dict(zip(rev["ID_Événement"], rev["Montant"])))
    dfe2["Bénéfice"] = dfe2["Recettes"] - dfe2["Coût_Total"]
    chart = alt.Chart(dfe2).mark_bar().encode(
        x="Nom_Événement", y="Bénéfice", color="Bénéfice"
    ).properties(width=700)
    st.altair_chart(chart, use_container_width=True)
    # Top 5 bénéfice
    top5 = dfe2.nlargest(5, "Bénéfice")[["Nom_Événement","Bénéfice"]]
    st.table(top5)
    # Relances urgentes
    today = date.today().isoformat()
    nextw = (date.today()+timedelta(days=7)).isoformat()
    urgent = dfi[dfi["Relance"]<today]
    soon = dfi[(dfi["Relance"]>=today)&(dfi["Relance"]<=nextw)]
    if not urgent.empty:
        st.warning("🔥 Relances en retard")
        st.table(urgent[["ID_Interaction","ID","Relance"]])
    if not soon.empty:
        st.info("⏳ Relances à venir")
        st.table(soon[["ID_Interaction","ID","Relance"]])

# --- PAGE Contacts ---
elif page=="Contacts":
    st.header("👤 Contacts")
    df = load_df(DATA["contacts"], C_SCHEMA)
    sel = st.text_input("Recherche ID", "")
    df_f = df[df["ID"].str.contains(sel)] if sel else df
    # Grille interactive
    gb = GridOptionsBuilder.from_dataframe(df_f)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df_f, gridOptions=gb.build(), height=300)
    # Fiche 360
    sel2 = st.selectbox("Sélectionner un contact", [""]+df["ID"].tolist())
    if sel2:
        rec = df[df["ID"]==sel2].iloc[0]
        st.subheader("Fiche 360")
        st.markdown(f"**{rec['Nom']} {rec['Prénom']}** – {rec['Société']}")
        st.write("Dernières interactions :")
        df_int = load_df(DATA["interactions"], I_SCHEMA)
        st.table(df_int[df_int["ID"]==sel2].tail(5)[["Date","Objet","Résultat"]])
        st.write("Dernières participations :")
        df_par = load_df(DATA["participations"], P_SCHEMA)
        st.table(df_par[df_par["ID"]==sel2].tail(5)[["Inscription","ID_Événement"]])

# --- PAGE Interactions ---
elif page=="Interactions":
    st.header("💬 Interactions")
    df = load_df(DATA["interactions"], I_SCHEMA)
    gb = GridOptionsBuilder.from_dataframe(df); gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build(), height=300)
    with st.expander("➕ Nouvelle interaction"):
        with st.form("f_int"):
            idc = st.selectbox("ID Contact", [""]+load_df(DATA["contacts"],C_SCHEMA)["ID"].tolist())
            date_i = st.date_input("Date", date.today())
            canal = st.selectbox("Canal", SET["canaux"])
            obj = st.text_input("Objet")
            res = st.selectbox("Résultat", SET["resultats_inter"])
            resp = st.text_input("Responsable")
            pa = st.text_area("Prochaine action")
            rel = st.date_input("Relance (opt.)", value=None)
            sub = st.form_submit_button("Enregistrer")
            if sub and idc:
                new = {"ID_Interaction":generate_id("INT",df,"ID_Interaction"),"ID":idc,
                       "Date":date_i.isoformat(),"Canal":canal,"Objet":obj,"Résumé":pa,
                       "Résultat":res,"Responsable":resp,"Prochaine_Action":pa,
                       "Relance":rel.isoformat() if rel else ""}
                df = pd.concat([df,pd.DataFrame([new])],ignore_index=True)
                save_df(df,DATA["interactions"])
                st.success("Interaction créée")

# --- PAGE Événements ---
elif page=="Événements":
    st.header("📅 Événements")
    df = load_df(DATA["evenements"], E_SCHEMA)
    gb = GridOptionsBuilder.from_dataframe(df); gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build(), height=300)
    with st.expander("➕ Nouvel événement"):
        with st.form("f_evt"):
            nom = st.text_input("Nom")
            typ = st.selectbox("Type", SET["types_evenements"])
            dt = st.date_input("Date")
            dur = st.number_input("Durée (h)",0.0,step=0.5)
            lieu = st.text_input("Lieu")
            form = st.text_area("Formateur(s)")
            inv = st.text_area("Invité(s)")
            obj = st.text_area("Objectif")
            per = st.selectbox("Période",["Matinée","Après-midi","Journée"])
            cout_tot = st.number_input("Coût total",0.0)
            sub = st.form_submit_button("Enregistrer")
            if sub:
                new = {"ID_Événement":generate_id("EVT",df,"ID_Événement"),
                       "Nom_Événement":nom,"Type":typ,"Date":dt.isoformat(),
                       "Durée_h":dur,"Lieu":lieu,"Formateur(s)":form,
                       "Invité(s)":inv,"Objectif":obj,"Période":per,
                       "Notes":"","Coût_Total":cout_tot,"Coût_Salle":0.0,
                       "Coût_Formateur":0.0,"Coût_Logistique":0.0,"Coût_Pub":0.0}
                df = pd.concat([df,pd.DataFrame([new])],ignore_index=True)
                save_df(df,DATA["evenements"])
                st.success("Événement créé")

# --- PAGE Participations ---
elif page=="Participations":
    st.header("🙋 Participations")
    df = load_df(DATA["participations"], P_SCHEMA)
    gb = GridOptionsBuilder.from_dataframe(df); gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build(), height=300)
    with st.expander("➕ Nouvelle participation"):
        with st.form("f_par"):
            idc = st.selectbox("ID Contact", [""]+load_df(DATA["contacts"],C_SCHEMA)["ID"].tolist())
            ide = st.selectbox("ID Événement", [""]+load_df(DATA["evenements"],E_SCHEMA)["ID_Événement"].tolist())
            ins = st.date_input("Inscription")
            arr = st.text_input("Arrivée (hh:mm)")
            fb = st.slider("Feedback",1,5,3)
            note = st.number_input("Note",0,20)
            sub = st.form_submit_button("Enregistrer")
            if sub and idc and ide:
                new = {"ID_Participation":generate_id("PAR",df,"ID_Participation"),
                       "ID":idc,"ID_Événement":ide,"Rôle":"Participant",
                       "Inscription":ins.isoformat(),"Arrivée":arr,
                       "Temps_Présent":"AUTO","Feedback":fb,"Note":note,
                       "Commentaire":"","Nom Participant":"","Nom Événement":""}
                df = pd.concat([df,pd.DataFrame([new])],ignore_index=True)
                save_df(df,DATA["participations"])
                st.success("Participation ajoutée")

# --- PAGE Paiements ---
elif page=="Paiements":
    st.header("💳 Paiements")
    df = load_df(DATA["paiements"], PAY_SCHEMA)
    gb = GridOptionsBuilder.from_dataframe(df); gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build(), height=300)
    with st.expander("➕ Nouveau paiement"):
        with st.form("f_pay"):
            idc = st.text_input("ID Contact")
            ide = st.text_input("ID Événement")
            dp = st.date_input("Date Paiement")
            mont = st.number_input("Montant",0.0)
            moy = st.selectbox("Moyen", SET["moyens_paiement"])
            stat = st.selectbox("Statut", SET["statuts_paiement"])
            sub = st.form_submit_button("Enregistrer")
            if sub:
                new = {"ID_Paiement":generate_id("PAY",df,"ID_Paiement"),
                       "ID":idc,"ID_Événement":ide,"Date_Paiement":dp.isoformat(),
                       "Montant":mont,"Moyen":moy,"Statut":stat,
                       "Référence":"","Notes":"","Relance":"" , "Nom Contact":"","Nom Événement":""}
                df = pd.concat([df,pd.DataFrame([new])], ignore_index=True)
                save_df(df,DATA["paiements"])
                st.success("Paiement enregistré")

# --- PAGE Certifications ---
elif page=="Certifications":
    st.header("📜 Certifications")
    df = load_df(DATA["certifications"], CERT_SCHEMA)
    gb = GridOptionsBuilder.from_dataframe(df); gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build(), height=300)
    with st.expander("➕ Nouvelle certification"):
        with st.form("f_cert"):
            idc = st.text_input("ID Contact")
            tc = st.selectbox("Type Certif", SET["types_certif"])
            de = st.date_input("Date Examen")
            res = st.selectbox("Résultat", ["Réussi","Échoué","En attente"])
            score = st.number_input("Score",0)
            do = st.date_input("Date Obtention")
            sub = st.form_submit_button("Enregistrer")
            if sub:
                new = {"ID_Certif":generate_id("CER",df,"ID_Certif"),
                       "ID":idc,"Type_Certif":tc,"Date_Examen":de.isoformat(),
                       "Résultat":res,"Score":score,"Date_Obtention":do.isoformat(),
                       "Validité":"","Renouvellement":"","Notes":"","Nom Contact":""}
                df = pd.concat([df,pd.DataFrame([new])], ignore_index=True)
                save_df(df,DATA["certifications"])
                st.success("Certification ajoutée")

# --- PAGE Paramètres ---
elif page=="Paramètres":
    st.header("⚙️ Paramètres")
    st.markdown("**Référentiels dynamiques**")
    col1,col2 = st.columns(2)
    with col1:
        with st.expander("Statuts de paiement"):
            sp = "\n".join(SET["statuts_paiement"])
            statuts_paiement = st.text_area("statuts_paiement", sp)
        with st.expander("Résultats d'interaction"):
            ri = "\n".join(SET["resultats_inter"])
            resultats_inter = st.text_area("resultats_inter", ri)
        with st.expander("Types de contact"):
            tc = "\n".join(SET["types_contact"])
            types_contact = st.text_area("types_contact", tc)
        with st.expander("Sources"):
            so = "\n".join(SET["sources"])
            sources = st.text_area("sources", so)
    with col2:
        with st.expander("Statuts d'engagement"):
            se = "\n".join(SET["statuts_engagement"])
            statuts_engagement = st.text_area("statuts_engagement", se)
        with st.expander("Secteurs"):
            sc = "\n".join(SET["secteurs"])
            secteurs = st.text_area("secteurs", sc)
        with st.expander("Pays"):
            py = "\n".join(SET["pays"])
            pays = st.text_area("pays", py)
        with st.expander("Canaux"):
            ca = "\n".join(SET["canaux"])
            canaux = st.text_area("canaux", ca)
        with st.expander("Types d'événements"):
            te = "\n".join(SET["types_evenements"])
            types_evenements = st.text_area("types_evenements", te)
        with st.expander("Moyens de paiement"):
            mp = "\n".join(SET["moyens_paiement"])
            moyens_paiement = st.text_area("moyens_paiement", mp)
        with st.expander("Types Certif"):
            ct = "\n".join(SET["types_certif"])
            types_certif = st.text_area("types_certif", ct)
    if st.button("💾 Sauvegarder Paramètres"):
        SET["statuts_paiement"]    = statuts_paiement.split("\n")
        SET["resultats_inter"]     = resultats_inter.split("\n")
        SET["types_contact"]       = types_contact.split("\n")
        SET["sources"]             = sources.split("\n")
        SET["statuts_engagement"]  = statuts_engagement.split("\n")
        SET["secteurs"]            = secteurs.split("\n")
        SET["pays"]                = pays.split("\n")
        SET["canaux"]              = canaux.split("\n")
        SET["types_evenements"]    = types_evenements.split("\n")
        SET["moyens_paiement"]     = moyens_paiement.split("\n")
        SET["types_certif"]        = types_certif.split("\n")
        save_settings(SET)
        st.success("✅ Paramètres mis à jour")
