# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM Streamlit (monofichier)
Version : CRM + Events + Rapports + Admin/Migration (Global & Multi-onglets)
Améliorations :
- Sélection contact robuste : sélecteur maître + auto-sync AgGrid + bouton de secours
- Grilles paginées (AgGrid), filtres, export Excel, logs migrations, reset DB, purge ID
"""

from datetime import datetime, date
from pathlib import Path
import io, json, re, unicodedata

import numpy as np
import pandas as pd
import streamlit as st

# AgGrid
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# ------------------------------------------------------------------
# Fichiers & schémas
# ------------------------------------------------------------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "inter": DATA_DIR / "interactions.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "settings": DATA_DIR / "settings.json",
    "logs": DATA_DIR / "migration_logs.jsonl",
}

C_COLS = ["ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
          "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"]
I_COLS = ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"]
E_COLS = ["ID_Événement","Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
          "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]
P_COLS = ["ID_Participation","ID","ID_Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"]
PAY_COLS = ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence","Notes","Relance"]
CERT_COLS = ["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"]

ALL_SCHEMAS = {
    "contacts": C_COLS, "interactions": I_COLS, "evenements": E_COLS,
    "participations": P_COLS, "paiements": PAY_COLS, "certifications": CERT_COLS,
}
TABLE_ID_COL = {"contacts":"ID","interactions":"ID_Interaction","evenements":"ID_Événement",
                "participations":"ID_Participation","paiements":"ID_Paiement","certifications":"ID_Certif"}

DEFAULT = {
    "genres":["Homme","Femme","Autre"],
    "secteurs":["Banque","Télécom","IT","Éducation","Santé","ONG","Industrie","Public","Autre"],
    "types_contact":["Membre","Prospect","Formateur","Partenaire"],
    "sources":["Afterwork","Formation","LinkedIn","Recommandation","Site Web","Salon","Autre"],
    "statuts_engagement":["Actif","Inactif","À relancer"],
    "canaux":["Appel","Email","WhatsApp","Zoom","Présentiel","Autre"],
    "villes":["Douala","Yaoundé","Limbe","Bafoussam","Garoua","Autres"],
    "pays":["Cameroun","Côte d'Ivoire","Sénégal","France","Canada","Autres"],
    "types_evenements":["Formation","Groupe d'étude","BA MEET UP","Webinaire","Conférence","Certification"],
    "lieux":["Présentiel","Zoom","Hybride"],
    "resultats_inter":["Positif","Négatif","À suivre","Sans suite"],
    "statuts_paiement":["Réglé","Partiel","Non payé"],
    "moyens_paiement":["Mobile Money","Virement","CB","Cash"],
    "types_certif":["ECBA","CCBA","CBAP","PBA"],
    "entreprises_cibles":["Dangote","MUPECI","SALAM","SUNU IARD","ENEO","PAD","PAK"],
}

def load_settings():
    if PATHS["settings"].exists():
        try: d = json.loads(PATHS["settings"].read_text(encoding="utf-8"))
        except Exception: d = DEFAULT.copy()
    else: d = DEFAULT.copy()
    for k,v in DEFAULT.items():
        if k not in d or not isinstance(d[k],list): d[k]=v
    return d

def save_settings(d:dict):
    PATHS["settings"].write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

SET = load_settings()

# ------------------------------------------------------------------
# Utilitaires
# ------------------------------------------------------------------
def ensure_df(path:Path, cols:list)->pd.DataFrame:
    if path.exists():
        try: df = pd.read_csv(path, dtype=str, encoding="utf-8")
        except Exception: df = pd.DataFrame(columns=cols)
    else: df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns: df[c]=""
    return df[cols]

def save_df(df:pd.DataFrame, path:Path): df.to_csv(path, index=False, encoding="utf-8")

def parse_date(s:str):
    if not s or str(s).strip()=="" or str(s).lower()=="nan": return None
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%Y/%m/%d"):
        try: return datetime.strptime(str(s), fmt).date()
        except: pass
    try: return pd.to_datetime(s).date()
    except: return None

def email_ok(s:str)->bool:
    if not s or str(s).strip()=="" or str(s).lower()=="nan": return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(s).strip()))

def phone_ok(s:str)->bool:
    if not s or str(s).strip()=="" or str(s).lower()=="nan": return True
    s2 = re.sub(r"[ \.\-\(\)]","",str(s)).replace("+","")
    return s2.isdigit() and len(s2)>=8

def generate_id(prefix:str, df:pd.DataFrame, id_col:str, width:int=3)->str:
    if df.empty or id_col not in df.columns: return f"{prefix}_{str(1).zfill(width)}"
    patt = re.compile(rf"^{re.escape(prefix)}_(\d+)$"); mx=0
    for x in df[id_col].dropna().astype(str):
        m=patt.match(x.strip()); 
        if m:
            try: mx=max(mx,int(m.group(1)))
            except: pass
    return f"{prefix}_{str(mx+1).zfill(width)}"

def log_event(kind:str, payload:dict):
    rec={"ts":datetime.now().isoformat(),"kind":kind,**payload}
    with PATHS["logs"].open("a", encoding="utf-8") as f: f.write(json.dumps(rec, ensure_ascii=False)+"\n")

def dedupe_contacts(df:pd.DataFrame):
    df=df.copy(); rejects=[]; seen=set(); keep=[]
    def norm(s): return str(s).strip().lower()
    for _,r in df.iterrows():
        if not email_ok(r.get("Email","")):
            rr=r.to_dict(); rr["_Raison"]="Email invalide"; rejects.append(rr); continue
        if not phone_ok(r.get("Téléphone","")):
            rr=r.to_dict(); rr["_Raison"]="Téléphone invalide"; rejects.append(rr); continue
        if r.get("Email",""): key=("email",norm(r["Email"]))
        elif r.get("Téléphone",""): key=("tel",norm(r["Téléphone"]))
        else: key=("nps",(norm(r.get("Nom","")),norm(r.get("Prénom","")),norm(r.get("Société",""))))
        if key in seen:
            rr=r.to_dict(); rr["_Raison"]="Doublon détecté (fichier)"; rejects.append(rr); continue
        seen.add(key); keep.append(r)
    return pd.DataFrame(keep, columns=C_COLS), pd.DataFrame(rejects)

def strip_accents(s): 
    import unicodedata as _u
    return ''.join(c for c in _u.normalize('NFD', str(s)) if _u.category(c)!='Mn')

# ------------------------------------------------------------------
# Charger données
# ------------------------------------------------------------------
df_contacts = ensure_df(PATHS["contacts"], C_COLS)
df_inter    = ensure_df(PATHS["inter"], I_COLS)
df_events   = ensure_df(PATHS["events"], E_COLS)
df_parts    = ensure_df(PATHS["parts"], P_COLS)
df_pay      = ensure_df(PATHS["pay"], PAY_COLS)
df_cert     = ensure_df(PATHS["cert"], CERT_COLS)
if not df_contacts.empty:
    df_contacts["Top20"] = df_contacts["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

# ------------------------------------------------------------------
# Sidebar
# ------------------------------------------------------------------
st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à", ["CRM (Grille centrale)","Événements","Rapports","Admin"], index=0)
this_year = datetime.now().year
annee = st.sidebar.selectbox("Année", ["Toutes"]+[str(this_year-1),str(this_year),str(this_year+1)], index=1)
mois   = st.sidebar.selectbox("Mois", ["Tous"]+[f"{m:02d}" for m in range(1,13)], index=0)

# ------------------------------------------------------------------
# Aides analytiques
# ------------------------------------------------------------------
def filtered_tables_for_period(year_sel:str, month_sel:str):
    def in_period(d:pd.Series)->pd.Series:
        p=d.map(parse_date); m=p.notna()
        if year_sel!="Toutes": y=int(year_sel); m=m & p.map(lambda x: x and x.year==y)
        if month_sel!="Tous": mm=int(month_sel); m=m & p.map(lambda x: x and x.month==mm)
        return m.fillna(False)
    dfe2 = df_events[in_period(df_events["Date"])].copy() if not df_events.empty else df_events.copy()
    dfp2 = df_parts.copy()
    if not df_events.empty and not df_parts.empty:
        evd = df_events.set_index("ID_Événement")["Date"].map(parse_date)
        dfp2["_d"] = dfp2["ID_Événement"].map(evd)
        if year_sel!="Toutes": dfp2 = dfp2[dfp2["_d"].map(lambda x: x and x.year==int(year_sel))]
        if month_sel!="Tous": dfp2 = dfp2[dfp2["_d"].map(lambda x: x and x.month==int(month_sel))]
    dfpay2 = df_pay[in_period(df_pay["Date_Paiement"])].copy() if not df_pay.empty else df_pay.copy()
    dfcert2 = df_cert[in_period(df_cert["Date_Obtention"]) | in_period(df_cert["Date_Examen"])].copy() if not df_cert.empty else df_cert.copy()
    return dfe2, dfp2, dfpay2, dfcert2

# ------------------------------------------------------------------
# PAGE CRM
# ------------------------------------------------------------------
if page=="CRM (Grille centrale)":
    st.title("👥 CRM — Grille centrale (Contacts)")
    colf1,colf2,colf3,colf4 = st.columns([2,1,1,1])
    q = colf1.text_input("Recherche (nom, société, email)…","")
    page_size = colf2.selectbox("Taille de page", [20,50,100,200], index=0)
    type_filtre = colf3.selectbox("Type", ["Tous"]+SET["types_contact"])
    top20_only = colf4.checkbox("Top-20 uniquement", value=False)

    dfc = df_contacts.copy()
    if q:
        qs=q.lower()
        dfc=dfc[dfc.apply(lambda r: qs in str(r["Nom"]).lower() or qs in str(r["Prénom"]).lower()
                          or qs in str(r["Société"]).lower() or qs in str(r["Email"]).lower(), axis=1)]
    if type_filtre!="Tous": dfc=dfc[dfc["Type"]==type_filtre]
    if top20_only: dfc=dfc[dfc["Top20"]==True]

    # ---- Sélecteur maître
    def _label_contact(row):
        return f"{row['ID']} — {row['Prénom']} {row['Nom']} — {row['Société']}"
    options = [] if dfc.empty else dfc.apply(_label_contact, axis=1).tolist()
    id_map = {} if dfc.empty else dict(zip(options, dfc["ID"]))
    colsel, _ = st.columns([3,1])
    sel_label = colsel.selectbox("Contact sélectionné (sélecteur maître)", [""]+options, index=0, key="select_contact_label")
    if sel_label:
        st.session_state["selected_contact_id"] = id_map[sel_label]

    # ---- Grille paginée + auto-sync
    sel_id = st.session_state.get("selected_contact_id")
    table_cols = ["ID","Nom","Prénom","Société","Type","Statut","Email","Téléphone","Ville","Pays","Top20"]
    if HAS_AGGRID and not dfc.empty:
        gob = GridOptionsBuilder.from_dataframe(dfc[table_cols])
        gob.configure_selection("single", use_checkbox=True)
        gob.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size)
        gob.configure_side_bar()
        grid = AgGrid(
            dfc[table_cols],
            gridOptions=gob.build(),
            height=520,
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            allow_unsafe_jscode=True
        )
        # Bouton de secours
        if st.button("✅ Utiliser la sélection de la grille"):
            if grid.get("selected_rows"):
                st.session_state["selected_contact_id"] = grid["selected_rows"][0]["ID"]
                st.rerun()
            else:
                st.warning("Aucune ligne sélectionnée.")
        # Auto-sync + re-run si la sélection change
        if grid and grid.get("selected_rows"):
            new_sel = grid["selected_rows"][0]["ID"]
            if new_sel and new_sel != st.session_state.get("_grid_sel_id"):
                st.session_state["_grid_sel_id"] = new_sel
                st.session_state["selected_contact_id"] = new_sel
                st.rerun()
    else:
        st.info("Installez `streamlit-aggrid` pour la pagination/sélection avancée.")
        st.dataframe(dfc[table_cols], use_container_width=True)

    st.markdown("---")
    cL,cR = st.columns([1,2])
    with cL:
        st.subheader("Fiche Contact")
        sel_id = st.session_state.get("selected_contact_id")
        if sel_id:
            c = df_contacts[df_contacts["ID"]==sel_id]
            if not c.empty:
                d=c.iloc[0].to_dict()
                st.markdown(f"**{d.get('Prénom','')} {d.get('Nom','')}** — {d.get('Titre','')} chez **{d.get('Société','')}**")
                st.write(f"{d.get('Email','')} • {d.get('Téléphone','')} • {d.get('LinkedIn','')}")
                st.write(f"{d.get('Ville','')}, {d.get('Pays','')} — {d.get('Secteur','')}")
                st.write(f"Type: **{d.get('Type','')}** | Statut: **{d.get('Statut','')}** | Score: **{d.get('Score_Engagement','')}** | Top20: **{d.get('Top20','')}**")
            else:
                st.warning("ID introuvable (rafraîchissez la page).")
        else:
            st.info("Sélectionnez un contact via la grille ou le sélecteur maître.")

    with cR:
        st.subheader("Actions liées au contact sélectionné")
        sel_id = st.session_state.get("selected_contact_id")
        if not sel_id:
            st.info("Sélectionnez un contact pour créer une interaction, participation, paiement ou certification.")
        else:
            tabs = st.tabs(["➕ Interaction","➕ Participation","➕ Paiement","➕ Certification","📑 Vue 360°"])
            with tabs[0]:
                with st.form("add_inter"):
                    c1,c2,c3 = st.columns(3)
                    dti=c1.date_input("Date", value=date.today())
                    canal=c2.selectbox("Canal", SET["canaux"])
                    resp=c3.selectbox("Responsable", ["Aymard","Alix","Autre"])
                    obj=st.text_input("Objet")
                    resu=st.selectbox("Résultat", SET["resultats_inter"])
                    resume=st.text_area("Résumé")
                    add_rel=st.checkbox("Planifier une relance ?"); rel=st.date_input("Relance", value=date.today()) if add_rel else None
                    ok=st.form_submit_button("💾 Enregistrer l'interaction")
                    if ok:
                        nid=generate_id("INT", df_inter, "ID_Interaction")
                        row={"ID_Interaction":nid,"ID":sel_id,"Date":dti.isoformat(),"Canal":canal,"Objet":obj,"Résumé":resume,"Résultat":resu,"Prochaine_Action":"","Relance":(rel.isoformat() if rel else ""),"Responsable":resp}
                        globals()["df_inter"]=pd.concat([df_inter, pd.DataFrame([row])], ignore_index=True); save_df(df_inter, PATHS["inter"]); st.success(f"Interaction enregistrée ({nid}).")
            with tabs[1]:
                with st.form("add_part"):
                    if df_events.empty: st.warning("Créez d'abord un événement.")
                    else:
                        ide=st.selectbox("Événement", df_events["ID_Événement"].tolist())
                        role=st.selectbox("Rôle", ["Participant","Animateur","Invité"])
                        fb=st.selectbox("Feedback", ["Très satisfait","Satisfait","Moyen","Insatisfait"])
                        note=st.number_input("Note (1-5)", min_value=1,max_value=5,value=5)
                        ok=st.form_submit_button("💾 Enregistrer la participation")
                        if ok:
                            nid=generate_id("PAR", df_parts, "ID_Participation")
                            row={"ID_Participation":nid,"ID":sel_id,"ID_Événement":ide,"Rôle":role,"Inscription":"","Arrivée":"","Temps_Present":"","Feedback":fb,"Note":str(note),"Commentaire":""}
                            globals()["df_parts"]=pd.concat([df_parts, pd.DataFrame([row])], ignore_index=True); save_df(df_parts, PATHS["parts"]); st.success(f"Participation ajoutée ({nid}).")
            with tabs[2]:
                with st.form("add_pay"):
                    if df_events.empty: st.warning("Créez d'abord un événement.")
                    else:
                        ide=st.selectbox("Événement", df_events["ID_Événement"].tolist())
                        dtp=st.date_input("Date paiement", value=date.today())
                        montant=st.number_input("Montant (FCFA)", min_value=0, step=1000)
                        moyen=st.selectbox("Moyen", SET["moyens_paiement"])
                        statut=st.selectbox("Statut", SET["statuts_paiement"])
                        ref=st.text_input("Référence")
                        ok=st.form_submit_button("💾 Enregistrer le paiement")
                        if ok:
                            nid=generate_id("PAY", df_pay, "ID_Paiement")
                            row={"ID_Paiement":nid,"ID":sel_id,"ID_Événement":ide,"Date_Paiement":dtp.isoformat(),"Montant":str(montant),"Moyen":moyen,"Statut":statut,"Référence":ref,"Notes":"","Relance":""}
                            globals()["df_pay"]=pd.concat([df_pay, pd.DataFrame([row])], ignore_index=True); save_df(df_pay, PATHS["pay"]); st.success(f"Paiement enregistré ({nid}).")
            with tabs[3]:
                with st.form("add_cert"):
                    tc=st.selectbox("Type Certification", SET["types_certif"])
                    dte=st.date_input("Date Examen", value=date.today())
                    res=st.selectbox("Résultat", ["Réussi","Échoué","En cours","Reporté"])
                    sc=st.number_input("Score", min_value=0, max_value=100, value=0)
                    has_dto=st.checkbox("Renseigner une date d'obtention ?"); dto=st.date_input("Date Obtention", value=date.today()) if has_dto else None
                    ok=st.form_submit_button("💾 Enregistrer la certification")
                    if ok:
                        nid=generate_id("CER", df_cert, "ID_Certif")
                        row={"ID_Certif":nid,"ID":sel_id,"Type_Certif":tc,"Date_Examen":dte.isoformat(),"Résultat":res,"Score":str(sc),"Date_Obtention":(dto.isoformat() if dto else ""), "Validité":"","Renouvellement":"","Notes":""}
                        globals()["df_cert"]=pd.concat([df_cert, pd.DataFrame([row])], ignore_index=True); save_df(df_cert, PATHS["cert"]); st.success(f"Certification ajoutée ({nid}).")
            with tabs[4]:
                st.markdown("#### Vue 360°")
                if not df_inter.empty:
                    st.write("**Interactions**"); st.dataframe(df_inter[df_inter["ID"]==sel_id][["Date","Canal","Objet","Résultat","Relance","Responsable"]], use_container_width=True)
                if not df_parts.empty:
                    st.write("**Participations**"); dfp=df_parts[df_parts["ID"]==sel_id].copy()
                    if not df_events.empty:
                        ev_names=df_events.set_index("ID_Événement")["Nom_Événement"]; dfp["Événement"]=dfp["ID_Événement"].map(ev_names)
                    st.dataframe(dfp[["Événement","Rôle","Feedback","Note"]], use_container_width=True)
                if not df_pay.empty:
                    st.write("**Paiements**"); st.dataframe(df_pay[df_pay["ID"]==sel_id][["ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence"]], use_container_width=True)
                if not df_cert.empty:
                    st.write("**Certifications**"); st.dataframe(df_cert[df_cert["ID"]==sel_id][["Type_Certif","Date_Examen","Résultat","Score","Date_Obtention"]], use_container_width=True)

# ------------------------------------------------------------------
# PAGE ÉVÉNEMENTS
# ------------------------------------------------------------------
elif page=="Événements":
    st.title("📅 Événements")
    filt = st.text_input("Filtre rapide (nom, type, lieu, notes…)", "")
    page_size_evt = st.selectbox("Taille de page", [20,50,100,200], index=0, key="pg_evt")
    df_show=df_events.copy()
    if filt:
        t=filt.lower()
        df_show=df_show[df_show.apply(lambda r: any(t in str(r[c]).lower() for c in ["Nom_Événement","Type","Lieu","Notes"]), axis=1)]
    if HAS_AGGRID:
        gob=GridOptionsBuilder.from_dataframe(df_show)
        gob.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size_evt)
        gob.configure_default_column(filter=True, sortable=True, resizable=True)
        gob.configure_side_bar()
        AgGrid(df_show, gridOptions=gob.build(), height=520, update_mode=GridUpdateMode.NO_UPDATE, allow_unsafe_jscode=True)
    else:
        st.dataframe(df_show, use_container_width=True)

# ------------------------------------------------------------------
# PAGE RAPPORTS
# ------------------------------------------------------------------
elif page=="Rapports":
    st.title("📑 Rapports & Export")
    dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
    dfc2 = df_contacts.copy()
    total_contacts=len(dfc2)
    prospects_actifs=len(dfc2[(dfc2["Type"]=="Prospect") & (dfc2["Statut"]=="Actif")])
    membres=len(dfc2[dfc2["Type"]=="Membre"])
    ca_regle=impayes=0.0
    if not dfpay2.empty:
        dfpay2["Montant"]=pd.to_numeric(dfpay2["Montant"], errors="coerce").fillna(0.0)
        ca_regle=float(dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum())
        impayes=float(dfpay2[dfpay2["Statut"]!="Réglé"]["Montant"].sum())
    c1,c2,c3,c4=st.columns(4)
    c1.metric("👥 Contacts", total_contacts)
    c2.metric("🧲 Prospects actifs", prospects_actifs)
    c3.metric("🏆 Membres", membres)
    c4.metric("💰 CA réglé", f"{int(ca_regle):,} FCFA".replace(","," "))

    st.markdown("---"); st.markdown("### Export Excel (multi-onglets)")
    buf=io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_contacts.to_excel(w, index=False, sheet_name="contacts")
        df_inter.to_excel(w, index=False, sheet_name="interactions")
        df_events.to_excel(w, index=False, sheet_name="evenements")
        df_parts.to_excel(w, index=False, sheet_name="participations")
        df_pay.to_excel(w, index=False, sheet_name="paiements")
        df_cert.to_excel(w, index=False, sheet_name="certifications")
    st.download_button("⬇️ Télécharger la base (Excel, 6 feuilles)", buf.getvalue(),
                       file_name="IIBA_base.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ------------------------------------------------------------------
# PAGE ADMIN — Paramètres / Migration / Reset / Purge
# ------------------------------------------------------------------
elif page=="Admin":
    st.title("⚙️ Admin — Paramètres, Migration & Maintenance")

    st.markdown("#### Paramètres (listes déroulantes)")
    with st.form("set_form"):
        c1,c2,c3=st.columns(3)
        genres=c1.text_area("Genres","\n".join(SET["genres"]))
        types_contact=c2.text_area("Types de contact","\n".join(SET["types_contact"]))
        statuts_eng=c3.text_area("Statuts d'engagement","\n".join(SET["statuts_engagement"]))
        s1,s2,s3=st.columns(3)
        secteurs=s1.text_area("Secteurs","\n".join(SET["secteurs"]))
        pays=s2.text_area("Pays","\n".join(SET["pays"]))
        villes=s3.text_area("Villes","\n".join(SET["villes"]))
        s4,s5,s6=st.columns(3)
        sources=s4.text_area("Sources","\n".join(SET["sources"]))
        canaux=s5.text_area("Canaux","\n".join(SET["canaux"]))
        resint=s6.text_area("Résultats interaction","\n".join(SET["resultats_inter"]))
        e1,e2,e3=st.columns(3)
        types_evt=e1.text_area("Types événements","\n".join(SET["types_evenements"]))
        lieux=e2.text_area("Lieux","\n".join(SET["lieux"]))
        moyens=e3.text_area("Moyens paiement","\n".join(SET["moyens_paiement"]))
        e4,e5=st.columns(2)
        statpay=e4.text_area("Statuts paiement","\n".join(SET["statuts_paiement"]))
        tcert=e5.text_area("Types certification","\n".join(SET["types_certif"]))
        top20=st.text_area("Entreprises cibles (Top-20 / GECAM)","\n".join(SET["entreprises_cibles"]))
        ok=st.form_submit_button("💾 Enregistrer")
        if ok:
            SET.update({
                "genres":[x.strip() for x in genres.splitlines() if x.strip()],
                "types_contact":[x.strip() for x in types_contact.splitlines() if x.strip()],
                "statuts_engagement":[x.strip() for x in statuts_eng.splitlines() if x.strip()],
                "secteurs":[x.strip() for x in secteurs.splitlines() if x.strip()],
                "pays":[x.strip() for x in pays.splitlines() if x.strip()],
                "villes":[x.strip() for x in villes.splitlines() if x.strip()],
                "sources":[x.strip() for x in sources.splitlines() if x.strip()],
                "canaux":[x.strip() for x in canaux.splitlines() if x.strip()],
                "resultats_inter":[x.strip() for x in resint.splitlines() if x.strip()],
                "types_evenements":[x.strip() for x in types_evt.splitlines() if x.strip()],
                "lieux":[x.strip() for x in lieux.splitlines() if x.strip()],
                "moyens_paiement":[x.strip() for x in moyens.splitlines() if x.strip()],
                "statuts_paiement":[x.strip() for x in statpay.splitlines() if x.strip()],
                "types_certif":[x.strip() for x in tcert.splitlines() if x.strip()],
                "entreprises_cibles":[x.strip() for x in top20.splitlines() if x.strip()],
            }); save_settings(SET); st.success("Paramètres enregistrés.")

    st.markdown("---")
    st.header("📦 Migration — Global & Multi-onglets (logs & collisions d'ID)")
    mode_mig = st.radio("Mode de migration", ["Import Excel global (.xlsx)","Import Excel multi-onglets (.xlsx)","Import CSV global","Par table (CSV)"], horizontal=True)

    # --------- Import Excel Global
    if mode_mig=="Import Excel global (.xlsx)":
        up = st.file_uploader("Fichier Excel global (.xlsx)", type=["xlsx"], key="xlsx_up")
        st.caption("La feuille doit contenir la colonne **__TABLE__**.")
        if st.button("Importer l'Excel global") and up is not None:
            log={"timestamp":datetime.now().isoformat(),"import_type":"excel_global","counts":{},"errors":[],"collisions":{}}
            try:
                xls=pd.ExcelFile(up); sheet="Global" if "Global" in xls.sheet_names else xls.sheet_names[0]
                gdf=pd.read_excel(xls, sheet_name=sheet, dtype=str)
                if "__TABLE__" not in gdf.columns: raise ValueError("Colonne '__TABLE__' manquante.")
                gcols=["__TABLE__"]+sorted(set(sum(ALL_SCHEMAS.values(),[])))
                for c in gcols:
                    if c not in gdf.columns: gdf[c]=""
                # Contacts
                sub_c=gdf[gdf["__TABLE__"]=="contacts"].copy(); sub_c=sub_c[C_COLS].fillna("")
                sub_c["Top20"]=sub_c["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])
                valid_c, rejects_c = dedupe_contacts(sub_c)
                base=df_contacts.copy(); collisions=[]
                if "ID" in valid_c.columns and not valid_c.empty:
                    incoming=set(x for x in valid_c["ID"].astype(str) if x and x.lower()!="nan")
                    existing=set(base["ID"].astype(str))
                    collisions=sorted(list(incoming & existing))
                    if collisions: base=base[~base["ID"].isin(collisions)]
                patt=re.compile(r"^CNT_(\d+)$"); base_max=0
                for x in base["ID"].dropna().astype(str):
                    m=patt.match(x.strip())
                    if m:
                        try: base_max=max(base_max,int(m.group(1)))
                        except: pass
                next_id=base_max+1; new_rows=[]
                for _,r in valid_c.iterrows():
                    rid=r["ID"]
                    if not isinstance(rid,str) or rid.strip()=="" or rid.strip().lower()=="nan":
                        rid=f"CNT_{str(next_id).zfill(3)}"; next_id+=1
                    rr=r.to_dict(); rr["ID"]=rid; new_rows.append(rr)
                globals()["df_contacts"]=pd.concat([base, pd.DataFrame(new_rows, columns=C_COLS)], ignore_index=True); save_df(df_contacts, PATHS["contacts"])
                log["counts"]["contacts"]=len(new_rows); log["collisions"]["contacts"]=collisions
                if isinstance(rejects_c, pd.DataFrame) and not rejects_c.empty:
                    st.warning(f"Lignes contacts rejetées : {len(rejects_c)}"); st.dataframe(rejects_c, use_container_width=True)

                # Autres tables
                def save_subset(tbl, cols, path, prefix):
                    sub=gdf[gdf["__TABLE__"]==tbl].copy(); sub=sub[cols].fillna("")
                    id_col=cols[0]; base_df=ensure_df(path, cols)
                    incoming=set(x for x in sub[id_col].astype(str) if x and x.lower()!="nan")
                    existing=set(base_df[id_col].astype(str))
                    coll=sorted(list(incoming & existing))
                    if coll: base_df=base_df[~base_df[id_col].isin(coll)]
                    patt=re.compile(rf"^{prefix}_(\d+)$"); base_max=0
                    for x in base_df[id_col].dropna().astype(str):
                        m=patt.match(x.strip())
                        if m:
                            try: base_max=max(base_max,int(m.group(1)))
                            except: pass
                    gen=base_max+1; new_rows=[]
                    for _,r in sub.iterrows():
                        cur=r[id_col]
                        if not isinstance(cur,str) or cur.strip()=="" or cur.strip().lower()=="nan":
                            cur=f"{prefix}_{str(gen).zfill(3)}"; gen+=1
                        rr=r.to_dict(); rr[id_col]=cur; new_rows.append(rr)
                    out=pd.concat([base_df, pd.DataFrame(new_rows, columns=cols)], ignore_index=True); save_df(out, path)
                    log["counts"][tbl]=len(new_rows); log["collisions"][tbl]=coll
                save_subset("interactions", I_COLS, PATHS["inter"], "INT")
                save_subset("evenements", E_COLS, PATHS["events"], "EVT")
                save_subset("participations", P_COLS, PATHS["parts"], "PAR")
                save_subset("paiements", PAY_COLS, PATHS["pay"], "PAY")
                save_subset("certifications", CERT_COLS, PATHS["cert"], "CER")
                st.success("Import Excel global terminé."); st.json(log); log_event("import_excel_global", log)
            except Exception as e:
                st.error(f"Erreur d'import Excel global : {e}"); log_event("error_import_excel_global", {"error":str(e)})

        # Modèle global
        buf=io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            gcols=["__TABLE__"]+sorted(set(sum(ALL_SCHEMAS.values(),[])))
            pd.DataFrame(columns=gcols).to_excel(w, index=False, sheet_name="Global")
        st.download_button("⬇️ Modèle Global (xlsx)", buf.getvalue(), file_name="IIBA_global_template.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # --------- Import Excel Multi-onglets
    elif mode_mig=="Import Excel multi-onglets (.xlsx)":
        up = st.file_uploader("Classeur Excel (6 feuilles : contacts, interactions, evenements, participations, paiements, certifications)", type=["xlsx"], key="xlsx_multi")
        if st.button("Importer l'Excel multi-onglets") and up is not None:
            log={"timestamp":datetime.now().isoformat(),"import_type":"excel_multisheets","counts":{},"errors":[],"collisions":{}}
            try:
                xls=pd.ExcelFile(up)
                def norm(s): return strip_accents(str(s)).lower().strip()
                sheets={norm(n):n for n in xls.sheet_names}
                aliases={"contacts":["contacts","contact"],
                         "interactions":["interactions","interaction"],
                         "evenements":["evenements","événements","events"],
                         "participations":["participations","participation"],
                         "paiements":["paiements","paiement","payments"],
                         "certifications":["certifications","certification"]}
                found={}
                for tbl, names in aliases.items():
                    for n in names:
                        k=norm(n)
                        if k in sheets: found[tbl]=sheets[k]; break

                # Contacts
                if "contacts" in found:
                    gdf=pd.read_excel(xls, sheet_name=found["contacts"], dtype=str).fillna("")
                    for c in C_COLS:
                        if c not in gdf.columns: gdf[c]=""
                    sub_c=gdf[C_COLS].fillna("")
                    sub_c["Top20"]=sub_c["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])
                    valid_c, rejects_c = dedupe_contacts(sub_c)
                    base=df_contacts.copy(); collisions=[]
                    if "ID" in valid_c.columns and not valid_c.empty:
                        incoming=set(x for x in valid_c["ID"].astype(str) if x and x.lower()!="nan")
                        existing=set(base["ID"].astype(str)); collisions=sorted(list(incoming & existing))
                        if collisions: base=base[~base["ID"].isin(collisions)]
                    patt=re.compile(r"^CNT_(\d+)$"); base_max=0
                    for x in base["ID"].dropna().astype(str):
                        m=patt.match(x.strip())
                        if m:
                            try: base_max=max(base_max,int(m.group(1)))
                            except: pass
                    next_id=base_max+1; new_rows=[]
                    for _,r in valid_c.iterrows():
                        rid=r["ID"]
                        if not isinstance(rid,str) or rid.strip()=="" or rid.strip().lower()=="nan":
                            rid=f"CNT_{str(next_id).zfill(3)}"; next_id+=1
                        rr=r.to_dict(); rr["ID"]=rid; new_rows.append(rr)
                    globals()["df_contacts"]=pd.concat([base, pd.DataFrame(new_rows, columns=C_COLS)], ignore_index=True); save_df(df_contacts, PATHS["contacts"])
                    log["counts"]["contacts"]=len(new_rows); log["collisions"]["contacts"]=collisions
                    if 'rejects_c' in locals() and not rejects_c.empty:
                        st.warning(f"Lignes contacts rejetées : {len(rejects_c)}"); st.dataframe(rejects_c, use_container_width=True)

                def save_sheet(tbl, cols, path, prefix):
                    if tbl not in found: return 0, []
                    sdf=pd.read_excel(xls, sheet_name=found[tbl], dtype=str).fillna("")
                    for c in cols:
                        if c not in sdf.columns: sdf[c]=""
                    sdf=sdf[cols]
                    id_col=cols[0]; base_df=ensure_df(path, cols)
                    incoming=set(x for x in sdf[id_col].astype(str) if x and x.lower()!="nan")
                    existing=set(base_df[id_col].astype(str))
                    coll=sorted(list(incoming & existing))
                    if coll: base_df=base_df[~base_df[id_col].isin(coll)]
                    patt=re.compile(rf"^{prefix}_(\d+)$"); base_max=0
                    for x in base_df[id_col].dropna().astype(str):
                        m=patt.match(x.strip())
                        if m:
                            try: base_max=max(base_max,int(m.group(1)))
                            except: pass
                    gen=base_max+1; new_rows=[]
                    for _,r in sdf.iterrows():
                        cur=r[id_col]
                        if not isinstance(cur,str) or cur.strip()=="" or cur.strip().lower()=="nan":
                            cur=f"{prefix}_{str(gen).zfill(3)}"; gen+=1
                        rr=r.to_dict(); rr[id_col]=cur; new_rows.append(rr)
                    out=pd.concat([base_df, pd.DataFrame(new_rows, columns=cols)], ignore_index=True); save_df(out, path)
                    return len(new_rows), coll
                for spec in [("interactions",I_COLS,PATHS["inter"],"INT"),
                             ("evenements",E_COLS,PATHS["events"],"EVT"),
                             ("participations",P_COLS,PATHS["parts"],"PAR"),
                             ("paiements",PAY_COLS,PATHS["pay"],"PAY"),
                             ("certifications",CERT_COLS,PATHS["cert"],"CER")]:
                    cnt, coll = save_sheet(*spec); log["counts"][spec[0]]=cnt; log["collisions"][spec[0]]=coll

                st.success("Import Excel multi-onglets terminé."); st.json(log); log_event("import_excel_multisheets", log)
            except Exception as e:
                st.error(f"Erreur d'import multi-onglets : {e}"); log_event("error_import_excel_multisheets", {"error":str(e)})

        # Modèle multi-onglets
        bufm=io.BytesIO()
        with pd.ExcelWriter(bufm, engine="openpyxl") as w:
            pd.DataFrame(columns=C_COLS).to_excel(w, index=False, sheet_name="contacts")
            pd.DataFrame(columns=I_COLS).to_excel(w, index=False, sheet_name="interactions")
            pd.DataFrame(columns=E_COLS).to_excel(w, index=False, sheet_name="evenements")
            pd.DataFrame(columns=P_COLS).to_excel(w, index=False, sheet_name="participations")
            pd.DataFrame(columns=PAY_COLS).to_excel(w, index=False, sheet_name="paiements")
            pd.DataFrame(columns=CERT_COLS).to_excel(w, index=False, sheet_name="certifications")
        st.download_button("⬇️ Modèle Multi-onglets (xlsx)", bufm.getvalue(), file_name="IIBA_multisheets_template.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # --------- Import CSV Global + Export Global
    elif mode_mig=="Import CSV global":
        up=st.file_uploader("CSV global (colonne __TABLE__)", type=["csv"], key="g_up")
        if st.button("Importer le CSV global") and up is not None:
            try:
                gdf=pd.read_csv(up, dtype=str, encoding="utf-8")
                if "__TABLE__" not in gdf.columns: raise ValueError("Colonne __TABLE__ manquante.")
                def save_subset(tbl, cols, path, prefix):
                    sub=gdf[gdf["__TABLE__"]==tbl].copy()
                    for c in cols:
                        if c not in sub.columns: sub[c]=""
                    sub=sub[cols].fillna("")
                    id_col=cols[0]; base_df=ensure_df(path, cols)
                    incoming=set(x for x in sub[id_col].astype(str) if x and x.lower()!="nan")
                    existing=set(base_df[id_col].astype(str)); coll=sorted(list(incoming & existing))
                    if coll: base_df=base_df[~base_df[id_col].isin(coll)]
                    patt=re.compile(rf"^{prefix}_(\d+)$"); base_max=0
                    for x in base_df[id_col].dropna().astype(str):
                        m=patt.match(x.strip())
                        if m:
                            try: base_max=max(base_max,int(m.group(1)))
                            except: pass
                    gen=base_max+1; ids=[]
                    for _,r in sub.iterrows():
                        rid=r[id_col]
                        if not isinstance(rid,str) or rid.strip()=="" or rid.strip().lower()=="nan":
                            ids.append(f"{prefix}_{str(gen).zfill(3)}"); gen+=1
                        else: ids.append(rid.strip())
                    sub[id_col]=ids; out=pd.concat([base_df, sub], ignore_index=True); save_df(out, path)
                save_subset("contacts", C_COLS, PATHS["contacts"], "CNT")
                save_subset("interactions", I_COLS, PATHS["inter"], "INT")
                save_subset("evenements", E_COLS, PATHS["events"], "EVT")
                save_subset("participations", P_COLS, PATHS["parts"], "PAR")
                save_subset("paiements", PAY_COLS, PATHS["pay"], "PAY")
                save_subset("certifications", CERT_COLS, PATHS["cert"], "CER")
                st.success("Import CSV global terminé.")
            except Exception as e:
                st.error(f"Erreur d'import CSV global : {e}")
        # Export global (Excel)
        gcols=["__TABLE__"]+sorted(set(sum(ALL_SCHEMAS.values(),[])))
        rows=[]
        for tbl, df in [("contacts",df_contacts),("interactions",df_inter),("evenements",df_events),("participations",df_parts),("paiements",df_pay),("certifications",df_cert)]:
            d=df.copy().fillna(""); d["__TABLE__"]=tbl
            for c in gcols:
                if c not in d.columns: d[c]=""
            rows.append(d[gcols])
        gexport=pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=gcols)
        bufg=io.BytesIO()
        with pd.ExcelWriter(bufg, engine="openpyxl") as w: gexport.to_excel(w, index=False, sheet_name="Global")
        st.download_button("⬇️ Export Excel (Global)", bufg.getvalue(), file_name="IIBA_export_global.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # --------- Par table (CSV)
    else:
        cimp, cexp = st.columns(2)
        with cimp:
            st.subheader("Importer (par table)")
            up_kind=st.selectbox("Table", list(ALL_SCHEMAS.keys()))
            up=st.file_uploader("CSV", type=["csv"])
            if st.button("Importer", key="imp_table") and up is not None:
                df_new=pd.read_csv(up, dtype=str, encoding="utf-8")
                cols=ALL_SCHEMAS[up_kind]
                for c in cols:
                    if c not in df_new.columns: df_new[c]=""
                df_new=df_new[cols]; save_df(df_new, PATHS["contacts" if up_kind=="contacts" else "inter" if up_kind=="interactions" else "events" if up_kind=="evenements" else "parts" if up_kind=="participations" else "pay" if up_kind=="paiements" else "cert"])
                st.success("Import terminé.")
        with cexp:
            st.subheader("Exporter (par table)")
            kind=st.selectbox("Table à exporter", list(ALL_SCHEMAS.keys()))
            if st.button("Exporter", key="exp_table"):
                dfx=(df_contacts if kind=="contacts" else df_inter if kind=="interactions" else df_events if kind=="evenements" else df_parts if kind=="participations" else df_pay if kind=="paiements" else df_cert)
                st.download_button("⬇️ Télécharger CSV", dfx.to_csv(index=False).encode("utf-8"), file_name=f"{kind}.csv", mime="text/csv")

    st.markdown("---"); st.header("🧹 Maintenance — Réinitialiser & Purger")
    with st.expander("Réinitialiser la base (⚠️ supprime tous les CSV)", expanded=False):
        confirm=st.checkbox("Je confirme la suppression de toutes les tables CSV.")
        if st.button("🗑️ Réinitialiser la base", disabled=not confirm):
            try:
                for k,p in PATHS.items():
                    if k in ("settings","logs"): continue
                    if p.exists(): p.unlink(missing_ok=True)
                globals()["df_contacts"]=ensure_df(PATHS["contacts"], C_COLS); save_df(df_contacts, PATHS["contacts"])
                globals()["df_inter"]=ensure_df(PATHS["inter"], I_COLS); save_df(df_inter, PATHS["inter"])
                globals()["df_events"]=ensure_df(PATHS["events"], E_COLS); save_df(df_events, PATHS["events"])
                globals()["df_parts"]=ensure_df(PATHS["parts"], P_COLS); save_df(df_parts, PATHS["parts"])
                globals()["df_pay"]=ensure_df(PATHS["pay"], PAY_COLS); save_df(df_pay, PATHS["pay"])
                globals()["df_cert"]=ensure_df(PATHS["cert"], CERT_COLS); save_df(df_cert, PATHS["cert"])
                st.success("Base réinitialisée."); log_event("reset_db", {"status":"ok"})
            except Exception as e:
                st.error(f"Échec de la réinitialisation : {e}"); log_event("error_reset_db", {"error":str(e)})

    with st.expander("Purger un ID (supprime 1 enregistrement)", expanded=False):
        tbl=st.selectbox("Table", list(ALL_SCHEMAS.keys()), key="purge_tbl")
        id_col=TABLE_ID_COL[tbl]; target_id=st.text_input(f"ID à supprimer ({id_col})", key="purge_id")
        if st.button("🧽 Purger l'ID"):
            if not target_id.strip(): st.error("Veuillez saisir un ID.")
            else:
                try:
                    path=PATHS["contacts" if tbl=="contacts" else "inter" if tbl=="interactions" else "events" if tbl=="evenements" else "parts" if tbl=="participations" else "pay" if tbl=="paiements" else "cert"]
                    dfx=ensure_df(path, ALL_SCHEMAS[tbl]); before=len(dfx); dfx=dfx[dfx[id_col]!=target_id.strip()]; save_df(dfx, path)
                    if tbl=="contacts": globals()["df_contacts"]=dfx
                    elif tbl=="interactions": globals()["df_inter"]=dfx
                    elif tbl=="evenements": globals()["df_events"]=dfx
                    elif tbl=="participations": globals()["df_parts"]=dfx
                    elif tbl=="paiements": globals()["df_pay"]=dfx
                    else: globals()["df_cert"]=dfx
                    st.success(f"{before-len(dfx)} ligne(s) supprimée(s)."); log_event("purge_id", {"table":tbl,"id":target_id,"deleted":before-len(dfx)})
                except Exception as e:
                    st.error(f"Échec de la purge : {e}"); log_event("error_purge_id", {"table":tbl,"id":target_id,"error":str(e)})

st.sidebar.markdown("---"); st.sidebar.caption("© IIBA Cameroun — CRM monofichier")
