import re
import pandas as pd
from pathlib import Path

SRC_DB = Path("DB IIBA.xlsx")                   # source “brute”
EXPORT_IN = Path("IIBA_export_20250822_2331.xlsx")  # export existant à enrichir
EXPORT_OUT = Path("IIBA_export_enriched_v2.xlsx")

# --- utilitaires ---

def _norm_str(s):
    s = (str(s or "")).strip()
    return re.sub(r"\s+", " ", s)

def _norm_phone(s):
    s = re.sub(r"[^\d+]", "", str(s or ""))
    return s

def _norm_company(s):
    s = _norm_str(s).lower()
    if not s: 
        return ""
    # quelques alias simples
    aliases = {
        "univ.": "université",
        "university": "université",
        "institut": "institut",
        "ecole": "école",
        "iiba cameroon": "iiba cameroun",
        "sunuiard": "sunu iard",
        "eneo cameroon": "eneo",
    }
    for k,v in aliases.items():
        s = s.replace(k, v)
    return s

def _first_non_empty(*vals):
    for v in vals:
        if str(v or "").strip():
            return v
    return ""

def _merge_unique(values, sep="; "):
    uniq = []
    for v in values:
        for part in str(v or "").split(sep):
            vv = _norm_str(part)
            if vv and vv not in uniq:
                uniq.append(vv)
    return sep.join(uniq)

def _collect_emails(df):
    # prend toutes les colonnes qui contiennent "mail" insensible à la casse
    email_cols = [c for c in df.columns if "mail" in c.lower()]
    if "Email" not in email_cols and "Email" in df.columns:
        email_cols.append("Email")
    return email_cols

# --- chargement ---

xl_src = pd.ExcelFile(SRC_DB)
xl_exp = pd.ExcelFile(EXPORT_IN)

def load_sheet(xl, name, cols):
    if name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=name, dtype=str).fillna("")
    else:
        df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols].copy()

C_COLS   = ["ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
            "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"]
I_COLS   = ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"]
E_COLS   = ["ID_Événement","Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
            "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]
P_COLS   = ["ID_Participation","ID","ID_Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"]
PAY_COLS = ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence","Notes","Relance"]
CERT_COLS= ["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"]

contacts = load_sheet(xl_exp, "contacts", C_COLS)
inter    = load_sheet(xl_exp, "interactions", I_COLS)
events   = load_sheet(xl_exp, "evenements", E_COLS)
parts    = load_sheet(xl_exp, "participations", P_COLS)
pays     = load_sheet(xl_exp, "paiements", PAY_COLS)
certs    = load_sheet(xl_exp, "certifications", CERT_COLS)

# =========================
# 1) CONTACTS : “Entreprise fix” + dédoublonnage + emails multiples
# =========================

# 1a) chercher des colonnes “entreprise” dans le SRC pour alimenter Société si vide
src_contacts_guess = None
for sn in xl_src.sheet_names:
    df = pd.read_excel(xl_src, sheet_name=sn, dtype=str).fillna("")
    cols = [c.lower() for c in df.columns]
    if any(k in cols for k in ["nom", "prénom", "prenom", "email", "téléphone", "telephone"]):
        src_contacts_guess = df
        break

if src_contacts_guess is not None:
    # map des synonymes vers Société
    company_synonyms = [c for c in src_contacts_guess.columns
                        if any(w in c.lower() for w in ["soci", "entreprise", "company", "organisation", "école", "ecole", "institut"])]
else:
    company_synonyms = []

# emails : concaténer emails multiples dans “Email” (séparés par ; )
email_cols_src = _collect_emails(src_contacts_guess) if src_contacts_guess is not None else []
if email_cols_src and "Email" not in contacts.columns:
    contacts["Email"] = ""

def enrich_company_and_emails(row):
    # si société vide, essayer depuis SRC en matchant email/nom/tel
    soc = row["Société"]
    if not str(soc).strip() and src_contacts_guess is not None:
        # recherche par email si possible
        em = str(row["Email"]).strip().lower()
        hit = None
        if em:
            for c in email_cols_src:
                found = src_contacts_guess[src_contacts_guess[c].astype(str).str.lower()==em]
                if not found.empty:
                    hit = found.iloc[0]
                    break
        # sinon tentative par nom + téléphone
        if hit is None:
            nm, pr, tel = row["Nom"], row["Prénom"], _norm_phone(row["Téléphone"])
            tmp = src_contacts_guess.copy()
            tmp["_nm"]  = tmp.filter(regex="nom", axis=1).apply(lambda s: _norm_str(s).lower(), axis=1) if any("nom" in c.lower() for c in tmp.columns) else ""
            tmp["_pr"]  = tmp.filter(regex="pr[ée]nom", axis=1).apply(lambda s: _norm_str(s).lower(), axis=1) if any("pr" in c.lower() for c in tmp.columns) else ""
            tmp["_tel"] = tmp.filter(regex="t[ée]l", axis=1).apply(lambda s: _norm_phone(s), axis=1) if any("t" in c.lower() for c in tmp.columns) else ""
            cand = tmp[(tmp["_nm"]==_norm_str(nm).lower()) & (tmp["_pr"]==_norm_str(pr).lower())]
            if tel:
                cand = cand[cand["_tel"]==tel]
            if not cand.empty:
                hit = cand.iloc[0]
        if hit is not None and company_synonyms:
            for c in company_synonyms:
                v = str(hit.get(c,"")).strip()
                if v:
                    soc = v
                    break
    # emails multiples
    emails = [row.get("Email","")]
    if src_contacts_guess is not None and email_cols_src:
        # si on a une correspondance sur nom/prénom/tel, concaténer les emails trouvés
        nm, pr, tel = row["Nom"], row["Prénom"], _norm_phone(row["Téléphone"])
        tmp = src_contacts_guess.copy()
        tmp["_nm"]  = tmp.filter(regex="nom", axis=1).apply(lambda s: _norm_str(s).lower(), axis=1) if any("nom" in c.lower() for c in tmp.columns) else ""
        tmp["_pr"]  = tmp.filter(regex="pr[ée]nom", axis=1).apply(lambda s: _norm_str(s).lower(), axis=1) if any("pr" in c.lower() for c in tmp.columns) else ""
        tmp["_tel"] = tmp.filter(regex="t[ée]l", axis=1).apply(lambda s: _norm_phone(s), axis=1) if any("t" in c.lower() for c in tmp.columns) else ""
        cand = tmp[(tmp["_nm"]==_norm_str(nm).lower()) & (tmp["_pr"]==_norm_str(pr).lower())]
        if tel:
            cand = cand[cand["_tel"]==tel]
        if not cand.empty:
            for c in email_cols_src:
                emails.extend(cand[c].tolist())
    return pd.Series({"Société": soc, "Email": _merge_unique(emails)})

contacts[["Société","Email"]] = contacts.apply(enrich_company_and_emails, axis=1)

# Dédoublonnage contacts : clé = email (si non vide) sinon téléphone, sinon (Nom+Prénom+Société normalisée)
def contact_key(row):
    em = str(row["Email"]).lower().strip()
    if em:
        return ("email", em)
    tel = _norm_phone(row["Téléphone"])
    if tel:
        return ("tel", tel)
    return ("nps", (_norm_str(row["Nom"]).lower(), _norm_str(row["Prénom"]).lower(), _norm_company(row["Société"])))

grp = {}
agg_rows = []
for _, r in contacts.iterrows():
    k = contact_key(r)
    if k not in grp:
        grp[k] = r.copy()
    else:
        # fusion
        base = grp[k]
        base["Email"] = _merge_unique([base["Email"], r["Email"]])
        base["Téléphone"] = _merge_unique([base["Téléphone"], r["Téléphone"]])
        base["Société"] = _first_non_empty(base["Société"], r["Société"])
        for col in ["Secteur","LinkedIn","Ville","Pays","Type","Source","Statut","Notes","Titre"]:
            base[col] = _first_non_empty(base.get(col,""), r.get(col,""))
        # score/date/top20: garder le premier non vide
        base["Score_Engagement"] = _first_non_empty(base.get("Score_Engagement",""), r.get("Score_Engagement",""))
        base["Date_Creation"]    = _first_non_empty(base.get("Date_Creation",""), r.get("Date_Creation",""))
        base["Top20"]            = _first_non_empty(base.get("Top20",""), r.get("Top20",""))
        grp[k] = base

contacts_dedup = pd.DataFrame(grp.values(), columns=C_COLS)

# =========================
# 2) CERTIFICATIONS : ID ligne unique + ID de type stable
# =========================

# mapping type -> ID de type (stable et partagé)
TYPE2ID = {"CBAP":"CER_001", "ECBA":"CER_002", "CCBA":"CER_003"}

certs = certs.copy()
certs["Type_Certif"] = certs["Type_Certif"].str.upper().str.strip().replace({
    "CBAP":"CBAP","ECBA":"ECBA","CCBA":"CCBA"
})
certs["Type_Certif_ID"] = certs["Type_Certif"].map(TYPE2ID).fillna("CER_999")  # inconnu => CER_999

# Corriger les cas où le même ID_Certif était réutilisé pour des types différents
# (on réassigne un ID_Certif unique par ligne si collision incohérente)
def make_row_id(i):
    return f"CER_{str(i+1).zfill(4)}"

# si des ID_Certif sont vides ou incohérents, on les réattribue proprement
if certs.empty:
    certs_fixed = certs
else:
    certs_fixed = certs.copy()
    # détecter collisions “même ID_Certif mais types différents”
    bad = certs_fixed.groupby("ID_Certif")["Type_Certif"].nunique()
    bad_ids = set(bad[bad>1].index)
    need_reassign = certs_fixed["ID_Certif"].eq("") | certs_fixed["ID_Certif"].str.lower().eq("nan") | certs_fixed["ID_Certif"].isin(bad_ids)
    # réassigne uniquement pour ces lignes
    idxs = certs_fixed[need_reassign].index.tolist()
    for j, ridx in enumerate(idxs):
        certs_fixed.at[ridx, "ID_Certif"] = make_row_id(j)

# =========================
# 3) Dédup simple sur les autres onglets (éviter doublons stricts)
# =========================

def dedup_by_keys(df, keys):
    if df.empty:
        return df
    keys = [k for k in keys if k in df.columns]
    if not keys:
        return df.drop_duplicates().reset_index(drop=True)
    return df.sort_values(keys).drop_duplicates(subset=keys, keep="first").reset_index(drop=True)

events_dedup = dedup_by_keys(events, ["Nom_Événement","Date","Lieu"])
inter_dedup  = dedup_by_keys(inter, ["ID","Date","Objet"])
parts_dedup  = dedup_by_keys(parts, ["ID","ID_Événement"])
pays_dedup   = dedup_by_keys(pays, ["ID","ID_Événement","Montant","Date_Paiement"])
certs_dedup  = dedup_by_keys(certs_fixed, ["ID","Type_Certif","Date_Examen","Date_Obtention","Score","Résultat"])

# =========================
# 4) Sauvegarde
# =========================

with pd.ExcelWriter(EXPORT_OUT, engine="openpyxl") as w:
    contacts_dedup.to_excel(w, sheet_name="contacts", index=False)
    inter_dedup.to_excel(w, sheet_name="interactions", index=False)
    events_dedup.to_excel(w, sheet_name="evenements", index=False)
    parts_dedup.to_excel(w, sheet_name="participations", index=False)
    pays_dedup.to_excel(w, sheet_name="paiements", index=False)
    certs_dedup.to_excel(w, sheet_name="certifications", index=False)

print("✅ Écrit :", EXPORT_OUT)
