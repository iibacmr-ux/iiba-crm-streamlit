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
if hasattr(st, 'secrets') and "APP_PASSWORD" in st.secrets:
    pwd = st.sidebar.text_input("🔒 Mot de passe", type="password")
    if pwd != st.secrets["APP_PASSWORD"]:
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
    "pays":["Cameroun","France","Canada","Belgique","Autre","Côte d'Ivoire","Sénégal"],
    "canaux":["Email","Téléphone","WhatsApp","LinkedIn","Réunion","Autre"],
    "types_evenements":["Atelier","Conférence","Formation","Webinaire","Afterwork","BA MEET UP","Groupe d'étude"],
    "moyens_paiement":["Chèque","Espèces","Virement","CB","Mobile Money","Autre"],
    "types_certif":["ECBA","CCBA","CBAP"],
    "entreprises_cibles":["Dangote","MUPECI","SALAM","Orange","MTN","Société Générale","Ecobank","UBA","BGFI","CCA"]
}

# --- LOAD / SAVE SETTINGS & AUDIT ---
def load_settings():
    if os.path.exists(DATA["settings"]):
        settings = json.load(open(DATA["settings"],encoding="utf-8"))
        # Assurer que toutes les clés DEFAULT existent
        for k, v in DEFAULT.items():
            if k not in settings:
                settings[k] = v
        return settings
    json.dump(DEFAULT,open(DATA["settings"],"w",encoding="utf-8"),indent=2)
    return DEFAULT

def save_settings(s):
    json.dump(s,open(DATA["settings"],"w",encoding="utf-8"),indent=2)
    log_action("Mise à jour paramètres")

def log_action(msg):
    try:
        with open(DATA["audit"],"a",encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} - {msg}\n")
    except:
        pass

SET = load_settings()

# --- UTILITAIRES DATA ---
def generate_id(pref, df, col):
    nums = [int(x.split("_")[1]) for x in df[col] if isinstance(x,str) and "_" in x]
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
    "Société":"","Top20":False,"Secteur":SET["secteurs"][0] if SET["secteurs"] else "IT","Email":"","Téléphone":"",
    "Ville":"","Pays":SET["pays"] if SET["pays"] else "Cameroun","Type":SET["types_contact"] if SET["types_contact"] else "Prospect",
    "Source":SET["sources"] if SET["sources"] else "Autre","Statut":SET["statuts_engagement"] if SET["statuts_engagement"] else "Inactif",
    "LinkedIn":"","Notes":"", "Score":0,"Certifié":False,
    "Date_Creation":lambda:date.today().isoformat()
}

I_SCHEMA = {
    "ID_Interaction":lambda:None,"ID":"","Date":date.today().isoformat(),
    "Canal":SET["canaux"][0] if SET["canaux"] else "Email","Objet":"","Résumé":"",
    "Résultat":SET["resultats_inter"] if SET["resultats_inter"] else "Positif","Responsable":"",
    "Prochaine_Action":"","Relance":""
}

E_SCHEMA = {
    "ID_Événement":lambda:None,"Nom_Événement":"","Type":SET["types_evenements"][0] if SET["types_evenements"] else "Atelier",
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
    "Moyen":SET["moyens_paiement"][0] if SET["moyens_paiement"] else "Espèces","Statut":SET["statuts_paiement"] if SET["statuts_paiement"] else "Non payé",
    "Référence":"","Notes":"","Relance":""
}

CERT_SCHEMA = {
    "ID_Certif":lambda:None,"ID":"","Type_Certif":SET["types_certif"][0] if SET["types_certif"] else "ECBA",
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
    st.markdown('<h1>📈 Tableau de Bord Stratégique</h1>', unsafe_allow_html=True)
    
    dfc = load_df(DATA["contacts"],C_SCHEMA)
    dfi = load_df(DATA["interactions"],I_SCHEMA)
    dfe = load_df(DATA["evenements"],E_SCHEMA)
    dfp = load_df(DATA["participations"],P_SCHEMA)
    dfpay = load_df(DATA["paiements"],PAY_SCHEMA)
    dfcert = load_df(DATA["certifications"],CERT_SCHEMA)

    # Filtres temporels
    yrs=sorted({str(d)[:4] for d in dfc["Date_Creation"]}) or [str(date.today().year)]
    mths=["Tous"]+[f"{i:02d}" for i in range(1,13)]
    col1,col2=st.columns(2)
    yr=col1.selectbox("Année", yrs)
    mn=col2.selectbox("Mois", mths)

    def fil(df,col):
        df2=df[df[col].str[:4]==yr]
        return df2 if mn=="Tous" else df2[df2[col].str[5:7]==mn]

    dfc2,dfp2,dfpay2,dfcert2=fil(dfc,"Date_Creation"),fil(dfp,"Inscription"),fil(dfpay,"Date_Paiement"),fil(dfcert,"Date_Obtention")

    # KPI Cards
    cards=st.columns(4)
    prospects = dfc2[dfc2["Type"]=="Prospect"]
    convertis = prospects[prospects["Statut"]=="Actif"]
    cards[0].metric("Prospects convertis", len(convertis))
    rate=len(convertis)/max(len(prospects),1)
    cards.metric("Taux conv.",f"{rate:.1%}")
    
    cards[1].metric("Événements", len(fil(dfe,"Date")))
    cards[1].metric("Participations", len(dfp2))
    
    ca=dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum()
    cards[2].metric("CA réglé",f"{ca:,.0f} FCFA")
    imp=len(dfpay2[dfpay2["Statut"]!="Réglé"])
    cards[2].metric("Impayés",imp)
    
    cards[3].metric("Certifs réussies",len(dfcert2[dfcert2["Résultat"]=="Réussi"]))
    score_moy = dfp2["Feedback"].mean() if len(dfp2) > 0 else 0
    cards[3].metric("Score moy.",f"{score_moy:.1f}")

    # ROI Chart
    if not dfe.empty and not dfpay.empty:
        rev=dfpay2[dfpay2["Statut"]=="Réglé"].groupby("ID_Événement")["Montant"].sum().reset_index()
        dfe2=fil(dfe,"Date").copy()
        dfe2["Recettes"]=dfe2["ID_Événement"].map(dict(zip(rev["ID_Événement"],rev["Montant"]))).fillna(0)
        dfe2["Bénéfice"]=dfe2["Recettes"]-dfe2["Coût_Total"]
        
        if not dfe2.empty:
            chart=alt.Chart(dfe2.head(10)).mark_bar().encode(
                x=alt.X("Nom_Événement:N", title="Événement"),
                y=alt.Y("Bénéfice:Q", title="Bénéfice (FCFA)"),
                color=alt.Color("Bénéfice:Q", scale=alt.Scale(scheme="viridis"))
            ).properties(width=600, height=300)
            st.altair_chart(chart,use_container_width=True)

    # Relances urgentes
    today=date.today().isoformat()
    urgent=dfi[(dfi["Relance"] != "") & (dfi["Relance"] < today)]
    if not urgent.empty:
        st.warning("🔥 Relances urgentes")
        st.dataframe(urgent[["ID","Objet","Relance"]].head(5))

# --- PAGE Contacts ---
elif page=="Contacts":
    st.header("👤 Contacts")
    df=load_df(DATA["contacts"],C_SCHEMA)
    
    # Marquer Top20 et Certifiés
    df["Top20"]=df["Société"].isin(SET.get("entreprises_cibles",[]))
    dfcert=load_df(DATA["certifications"],CERT_SCHEMA)
    certifies=dfcert[dfcert["Résultat"]=="Réussi"]["ID"].unique()
    df["Certifié"]=df["ID"].isin(certifies)
    
    # Calcul scoring
    dfp=load_df(DATA["participations"],P_SCHEMA)
    dfpay=load_df(DATA["paiements"],PAY_SCHEMA)
    part_counts = dfp.groupby("ID").size()
    pay_counts = dfpay[dfpay["Statut"]=="Réglé"].groupby("ID").size()
    cert_counts = dfcert[dfcert["Résultat"]=="Réussi"].groupby("ID").size()
    
    df["Score"] = (
        df["ID"].map(part_counts).fillna(0)*1 +
        df["ID"].map(pay_counts).fillna(0)*2 +
        df["ID"].map(cert_counts).fillna(0)*3
    )
    
    # Grille
    gb=GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True,filterable=True)
    AgGrid(df,gridOptions=gb.build(),height=400)
    
    # Export
    if st.button("⬇️ Export Contacts"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="contacts.csv")

    # Fiche 360
    sel=st.selectbox("Sélection contact",[""]+df["ID"].tolist())
    if sel:
        rec=df[df["ID"]==sel].iloc[0]
        st.subheader(f"📋 {rec['Nom']} {rec['Prénom']} (Score: {rec['Score']:.0f})")
        
        col1,col2=st.columns(2)
        with col1:
            st.markdown("**Dernières interactions**")
            dfi=load_df(DATA["interactions"],I_SCHEMA)
            inter=dfi[dfi["ID"]==sel].tail(3)
            if not inter.empty:
                st.dataframe(inter[["Date","Objet","Résultat"]])
            else:
                st.info("Aucune interaction")
                
            st.markdown("**Participations**")
            part=dfp[dfp["ID"]==sel]
            if not part.empty:
                st.dataframe(part[["Inscription","ID_Événement","Feedback"]])
            else:
                st.info("Aucune participation")
        
        with col2:
            st.markdown("**Paiements**")
            pay=dfpay[dfpay["ID"]==sel]
            if not pay.empty:
                st.dataframe(pay[["Date_Paiement","Montant","Statut"]])
            else:
                st.info("Aucun paiement")
                
            st.markdown("**Certifications**")
            cert=dfcert[dfcert["ID"]==sel]
            if not cert.empty:
                st.dataframe(cert[["Type_Certif","Date_Obtention","Résultat"]])
            else:
                st.info("Aucune certification")

# --- PAGE Interactions ---
elif page=="Interactions":
    st.header("💬 Interactions")
    df=load_df(DATA["interactions"],I_SCHEMA)
    
    gb=GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True,filterable=True)
    AgGrid(df,gridOptions=gb.build(),height=300)
    
    with st.expander("➕ Nouvelle interaction"):
        with st.form("f_int"):
            dfc=load_df(DATA["contacts"],C_SCHEMA)
            idc=st.selectbox("ID Contact",[""]+dfc["ID"].tolist())
            date_i=st.date_input("Date",date.today())
            canal=st.selectbox("Canal",SET["canaux"])
            obj=st.text_input("Objet")
            res=st.text_area("Résumé")
            resultat=st.selectbox("Résultat",SET["resultats_inter"])
            resp=st.text_input("Responsable")
            pa=st.text_area("Prochaine action")
            rel=st.date_input("Relance (opt.)",value=None)
            sub=st.form_submit_button("Enregistrer")
            
            if sub and idc:
                new={
                    "ID_Interaction":generate_id("INT",df,"ID_Interaction"),
                    "ID":idc,"Date":date_i.isoformat(),"Canal":canal,"Objet":obj,
                    "Résumé":res,"Résultat":resultat,"Responsable":resp,
                    "Prochaine_Action":pa,"Relance":rel.isoformat() if rel else ""
                }
                df=pd.concat([df,pd.DataFrame([new])],ignore_index=True)
                save_df(df,DATA["interactions"])
                log_action(f"Nouvelle interaction: {idc}")
                st.success("Interaction créée")
                st.experimental_rerun()

# --- PAGE Événements ---
elif page=="Événements":
    st.header("📅 Événements")
    df=load_df(DATA["evenements"],E_SCHEMA)
    
    gb=GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True,filterable=True)
    AgGrid(df,gridOptions=gb.build(),height=300)
    
    with st.expander("➕ Nouvel événement"):
        with st.form("f_evt"):
            nom=st.text_input("Nom")
            typ=st.selectbox("Type",SET["types_evenements"])
            dt=st.date_input("Date")
            dur=st.number_input("Durée (h)",0.0,step=0.5)
            lieu=st.text_input("Lieu")
            form=st.text_area("Formateur(s)")
            inv=st.text_area("Invité(s)")
            obj=st.text_area("Objectif")
            per=st.selectbox("Période",["Matinée","Après-midi","Journée"])
            
            st.markdown("**Coûts**")
            cout_tot=st.number_input("Coût total",0.0)
            cout_salle=st.number_input("Coût salle",0.0)
            cout_form=st.number_input("Coût formateur",0.0)
            cout_log=st.number_input("Coût logistique",0.0)
            cout_pub=st.number_input("Coût publicité",0.0)
            
            sub=st.form_submit_button("Enregistrer")
            
            if sub and nom:
                new={
                    "ID_Événement":generate_id("EVT",df,"ID_Événement"),
                    "Nom_Événement":nom,"Type":typ,"Date":dt.isoformat(),
                    "Durée_h":dur,"Lieu":lieu,"Formateur(s)":form,
                    "Invité(s)":inv,"Objectif":obj,"Période":per,"Notes":"",
                    "Coût_Total":cout_tot,"Coût_Salle":cout_salle,
                    "Coût_Formateur":cout_form,"Coût_Logistique":cout_log,"Coût_Pub":cout_pub
                }
                df=pd.concat([df,pd.DataFrame([new])],ignore_index=True)
                save_df(df,DATA["evenements"])
                log_action(f"Nouvel événement: {nom}")
                st.success("Événement créé")
                st.experimental_rerun()

# --- PAGE Participations ---
elif page=="Participations":
    st.header("🙋 Participations")
    df=load_df(DATA["participations"],P_SCHEMA)
    
    gb=GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True,filterable=True)
    AgGrid(df,gridOptions=gb.build(),height=300)
    
    with st.expander("➕ Nouvelle participation"):
        with st.form("f_par"):
            dfc=load_df(DATA["contacts"],C_SCHEMA)
            dfe=load_df(DATA["evenements"],E_SCHEMA)
            idc=st.selectbox("ID Contact",[""]+dfc["ID"].tolist())
            ide=st.selectbox("ID Événement",[""]+dfe["ID_Événement"].tolist())
            role=st.selectbox("Rôle",["Participant","Organisateur","Formateur","Invité"])
            ins=st.date_input("Inscription")
            arr=st.text_input("Arrivée (hh:mm)")
            fb=st.slider("Feedback",1,5,3)
            note=st.number_input("Note",0,20,0)
            comm=st.text_area("Commentaire")
            sub=st.form_submit_button("Enregistrer")
            
            if sub and idc and ide:
                new={
                    "ID_Participation":generate_id("PAR",df,"ID_Participation"),
                    "ID":idc,"ID_Événement":ide,"Rôle":role,
                    "Inscription":ins.isoformat(),"Arrivée":arr,
                    "Temps_Présent":"AUTO","Feedback":fb,"Note":note,"Commentaire":comm
                }
                df=pd.concat([df,pd.DataFrame([new])],ignore_index=True)
                save_df(df,DATA["participations"])
                log_action(f"Nouvelle participation: {idc} -> {ide}")
                st.success("Participation ajoutée")
                st.experimental_rerun()

# --- PAGE Paiements ---
elif page=="Paiements":
    st.header("💳 Paiements")
    df=load_df(DATA["paiements"],PAY_SCHEMA)
    
    gb=GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True,filterable=True)
    AgGrid(df,gridOptions=gb.build(),height=300)
    
    with st.expander("➕ Nouveau paiement"):
        with st.form("f_pay"):
            idc=st.text_input("ID Contact")
            ide=st.text_input("ID Événement")
            dp=st.date_input("Date Paiement")
            mont=st.number_input("Montant",0.0)
            moy=st.selectbox("Moyen",SET["moyens_paiement"])
            stat=st.selectbox("Statut",SET["statuts_paiement"])
            ref=st.text_input("Référence")
            notes=st.text_area("Notes")
            rel=st.date_input("Relance (opt.)",value=None)
            sub=st.form_submit_button("Enregistrer")
            
            if sub and idc:
                new={
                    "ID_Paiement":generate_id("PAY",df,"ID_Paiement"),
                    "ID":idc,"ID_Événement":ide,"Date_Paiement":dp.isoformat(),
                    "Montant":mont,"Moyen":moy,"Statut":stat,
                    "Référence":ref,"Notes":notes,
                    "Relance":rel.isoformat() if rel else ""
                }
                df=pd.concat([df,pd.DataFrame([new])],ignore_index=True)
                save_df(df,DATA["paiements"])
                log_action(f"Nouveau paiement: {mont} FCFA de {idc}")
                st.success("Paiement enregistré")
                st.experimental_rerun()

# --- PAGE Certifications ---
elif page=="Certifications":
    st.header("📜 Certifications")
    df=load_df(DATA["certifications"],CERT_SCHEMA)
    
    gb=GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True,filterable=True)
    AgGrid(df,gridOptions=gb.build(),height=300)
    
    with st.expander("➕ Nouvelle certification"):
        with st.form("f_cert"):
            idc=st.text_input("ID Contact")
            tc=st.selectbox("Type Certif",SET["types_certif"])
            de=st.date_input("Date Examen")
            res=st.selectbox("Résultat",["Réussi","Échoué","En attente"])
            score=st.number_input("Score",0)
            do=st.date_input("Date Obtention")
            valid=st.text_input("Validité")
            ren=st.text_input("Renouvellement")
            notes=st.text_area("Notes")
            sub=st.form_submit_button("Enregistrer")
            
            if sub and idc:
                new={
                    "ID_Certif":generate_id("CER",df,"ID_Certif"),
                    "ID":idc,"Type_Certif":tc,"Date_Examen":de.isoformat(),
                    "Résultat":res,"Score":score,"Date_Obtention":do.isoformat(),
                    "Validité":valid,"Renouvellement":ren,"Notes":notes
                }
                df=pd.concat([df,pd.DataFrame([new])],ignore_index=True)
                save_df(df,DATA["certifications"])
                log_action(f"Nouvelle certification: {tc} pour {idc}")
                st.success("Certification ajoutée")
                st.experimental_rerun()

# --- PAGE Rapports ---
elif page=="Rapports":
    st.header("📊 Rapports Stratégiques")
    
    dfc=load_df(DATA["contacts"],C_SCHEMA)
    dfp=load_df(DATA["participations"],P_SCHEMA)
    dfpay=load_df(DATA["paiements"],PAY_SCHEMA)
    dfe=load_df(DATA["evenements"],E_SCHEMA)
    dfcert=load_df(DATA["certifications"],CERT_SCHEMA)
    
    tab1,tab2,tab3,tab4=st.tabs(["Prospects Non Convertis","Top 20 Entreprises","ROI Événements","KPIs IIBA"])
    
    with tab1:
        st.subheader("🔥 Prospects réguliers non convertis")
        # Prospects avec ≥3 participations mais 0 paiement
        part_counts=dfp.groupby("ID").size().reset_index(name="NbPart")
        paid_contacts=dfpay[dfpay["Statut"]=="Réglé"]["ID"].unique()
        prospects=dfc[dfc["Type"]=="Prospect"]
        reg_non_conv=prospects.merge(part_counts,on="ID",how="left")
        reg_non_conv["NbPart"]=reg_non_conv["NbPart"].fillna(0)
        hot_prospects=reg_non_conv[(reg_non_conv["NbPart"]>=3) & (~reg_non_conv["ID"].isin(paid_contacts))]
        
        if not hot_prospects.empty:
            st.dataframe(hot_prospects[["ID","Nom","Prénom","Société","NbPart","Score"]].sort_values("NbPart",ascending=False))
            st.info(f"{len(hot_prospects)} prospects chauds à relancer prioritairement")
        else:
            st.success("Aucun prospect chaud non converti!")
    
    with tab2:
        st.subheader("⭐ Top 20 Entreprises GECAM")
        top20=dfc[dfc["Société"].isin(SET.get("entreprises_cibles",[]))]
        if not top20.empty:
            summary=top20.groupby(["Société","Type"]).size().reset_index(name="Nombre")
            st.dataframe(summary.pivot(index="Société",columns="Type",values="Nombre").fillna(0))
            
            conv_rate=len(top20[top20["Type"]=="Membre"])/max(len(top20[top20["Type"]=="Prospect"]),1)
            st.metric("Taux conversion Top20",f"{conv_rate:.1%}")
        else:
            st.warning("Aucun contact dans les entreprises Top 20")
    
    with tab3:
        st.subheader("💰 ROI par Événement")
        if not dfe.empty:
            # Calculer recettes et bénéfices
            rev=dfpay[dfpay["Statut"]=="Réglé"].groupby("ID_Événement")["Montant"].sum().reset_index()
            roi_data=dfe.merge(rev,on="ID_Événement",how="left")
            roi_data["Montant"]=roi_data["Montant"].fillna(0)
            roi_data["Bénéfice"]=roi_data["Montant"]-roi_data["Coût_Total"]
            roi_data["ROI%"]=(roi_data["Bénéfice"]/roi_data["Coût_Total"].replace(0,1)*100).round(1)
            
            st.dataframe(roi_data[["Nom_Événement","Type","Montant","Coût_Total","Bénéfice","ROI%"]].sort_values("Bénéfice",ascending=False))
            
            # Top 5 bénéfices
            top5=roi_data.nlargest(5,"Bénéfice")
            st.bar_chart(top5.set_index("Nom_Événement")["Bénéfice"])
    
    with tab4:
        st.subheader("📈 KPIs Mission IIBA")
        
        col1,col2=st.columns(2)
        with col1:
            st.metric("Total Membres",len(dfc[dfc["Type"]=="Membre"]))
            st.metric("Total Prospects",len(dfc[dfc["Type"]=="Prospect"]))
            st.metric("Événements organisés",len(dfe))
            st.metric("Participations totales",len(dfp))
        
        with col2:
            st.metric("Certifications réussies",len(dfcert[dfcert["Résultat"]=="Réussi"]))
            ca_total=dfpay[dfpay["Statut"]=="Réglé"]["Montant"].sum()
            st.metric("CA Total",f"{ca_total:,.0f} FCFA")
            taux_cert=len(dfcert[dfcert["Résultat"]=="Réussi"])/max(len(dfcert),1)
            st.metric("Taux réussite certifs",f"{taux_cert:.1%}")
            satisfaction=dfp["Feedback"].mean() if len(dfp)>0 else 0
            st.metric("Satisfaction moyenne",f"{satisfaction:.1f}/5")

# --- PAGE Paramètres ---
elif page=="Paramètres":
    st.header("⚙️ Paramètres")
    st.markdown("**Référentiels dynamiques**")
    
    col1,col2=st.columns(2)
    with col1:
        with st.expander("💰 Statuts de paiement"):
            sp="\n".join(SET["statuts_paiement"])
            statuts_paiement=st.text_area("statuts_paiement",sp)
            
        with st.expander("📨 Résultats d'interaction"):
            ri="\n".join(SET["resultats_inter"])
            resultats_inter=st.text_area("resultats_inter",ri)
            
        with st.expander("🧑‍💼 Types de contact"):
            tc="\n".join(SET["types_contact"])
            types_contact=st.text_area("types_contact",tc)
            
        with st.expander("📋 Sources"):
            sc="\n".join(SET["sources"])
            sources=st.text_area("sources",sc)
            
        with st.expander("📜 Types de certifications"):
            cert="\n".join(SET["types_certif"])
            types_certif=st.text_area("types_certif",cert)
    
    with col2:
        with st.expander("🔄 Statuts d'engagement"):
            se="\n".join(SET["statuts_engagement"])
            statuts_engagement=st.text_area("statuts_engagement",se)
            
        with st.expander("🏢 Secteurs"):
            sec="\n".join(SET["secteurs"])
            secteurs=st.text_area("secteurs",sec)
            
        with st.expander("🌍 Pays"):
            py="\n".join(SET["pays"])
            pays=st.text_area("pays",py)
            
        with st.expander("📞 Canaux"):
            can="\n".join(SET["canaux"])
            canaux=st.text_area("canaux",can)
            
        with st.expander("⭐ Top 20 Entreprises GECAM"):
            ent="\n".join(SET["entreprises_cibles"])
            entreprises_cibles=st.text_area("entreprises_cibles",ent)
    
    if st.button("💾 Sauvegarder Paramètres"):
        SET["statuts_paiement"]=statuts_paiement.split("\n")
        SET["resultats_inter"]=resultats_inter.split("\n")
        SET["types_contact"]=types_contact.split("\n")
        SET["sources"]=sources.split("\n")
        SET["types_certif"]=types_certif.split("\n")
        SET["statuts_engagement"]=statuts_engagement.split("\n")
        SET["secteurs"]=secteurs.split("\n")
        SET["pays"]=pays.split("\n")
        SET["canaux"]=canaux.split("\n")
        SET["entreprises_cibles"]=entreprises_cibles.split("\n")
        save_settings(SET)
        st.success("✅ Paramètres mis à jour")
        st.experimental_rerun()
