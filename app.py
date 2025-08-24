
# Script pour créer un nouvel utilisateur admin avec mot de passe "123456"
import pandas as pd
import bcrypt
from pathlib import Path
from datetime import datetime

def create_new_admin():
    """
    Crée un nouvel utilisateur admin avec mot de passe "123456"
    """
    # Chemin vers le fichier des utilisateurs
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    USERS_PATH = DATA_DIR / "users.csv"
    
    print("🔧 Création d'un nouvel utilisateur admin...")
    
    # Colonnes nécessaires
    USER_COLS = ["user_id", "full_name", "role", "active", "pwd_hash", "must_change_pw", "created_at", "updated_at"]
    
    try:
        # Essayer de lire le fichier existant
        if USERS_PATH.exists():
            df_users = pd.read_csv(USERS_PATH, dtype=str).fillna("")
            print(f"✅ Fichier users.csv trouvé avec {len(df_users)} utilisateurs")
        else:
            df_users = pd.DataFrame(columns=USER_COLS)
            print("📝 Création d'un nouveau fichier users.csv")
        
        # Normaliser les colonnes
        if not df_users.empty:
            df_users.columns = [c.strip().lower() for c in df_users.columns]
            # Assurer que toutes les colonnes existent
            for col in USER_COLS:
                if col not in df_users.columns:
                    df_users[col] = ""
        else:
            df_users = pd.DataFrame(columns=USER_COLS)
        
        # Créer le nouvel admin
        new_password = "123456"
        pwd_hash = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        
        # Données du nouvel admin
        new_admin = {
            "user_id": "admin2@iiba.cm",
            "full_name": "Admin2 IIBA Cameroun",
            "role": "admin",
            "active": "1",  # Actif
            "pwd_hash": pwd_hash,
            "must_change_pw": "0",  # Pas besoin de changer
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        
        # Vérifier si l'utilisateur existe déjà
        if not df_users.empty:
            existing = df_users["user_id"].str.lower() == "admin2@iiba.cm"
            if existing.any():
                print("⚠️ admin2@iiba.cm existe déjà, mise à jour...")
                df_users.loc[existing.idxmax()] = new_admin
            else:
                # Ajouter le nouvel utilisateur
                df_users = pd.concat([df_users, pd.DataFrame([new_admin])], ignore_index=True)
        else:
            df_users = pd.DataFrame([new_admin])
        
        # Sauvegarder
        df_users.to_csv(USERS_PATH, index=False, encoding="utf-8")
        
        print("\n" + "="*60)
        print("✅ NOUVEL ADMIN CRÉÉ AVEC SUCCÈS!")
        print("="*60)
        print("Email/Login: admin2@iiba.cm")
        print("Mot de passe: 123456")
        print("Statut: ACTIF ✅")
        print("Rôle: admin")
        print("="*60)
        print("\n🚀 Vous pouvez maintenant vous connecter!")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

def reset_admin_password():
    """
    Remet le mot de passe de admin@iiba.cm à "123456"
    """
    DATA_DIR = Path("./data")
    USERS_PATH = DATA_DIR / "users.csv"
    
    if not USERS_PATH.exists():
        print("❌ Fichier users.csv introuvable")
        return False
    
    try:
        df_users = pd.read_csv(USERS_PATH, dtype=str).fillna("")
        df_users.columns = [c.strip().lower() for c in df_users.columns]
        
        # Chercher admin@iiba.cm
        admin_mask = df_users["user_id"].str.lower() == "admin@iiba.cm"
        
        if admin_mask.any():
            # Réinitialiser le mot de passe
            new_password = "123456"
            pwd_hash = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
            
            df_users.loc[admin_mask, "pwd_hash"] = pwd_hash
            df_users.loc[admin_mask, "active"] = "1"
            df_users.loc[admin_mask, "must_change_pw"] = "0"
            df_users.loc[admin_mask, "updated_at"] = datetime.now().isoformat(timespec="seconds")
            
            df_users.to_csv(USERS_PATH, index=False, encoding="utf-8")
            
            print("✅ Mot de passe de admin@iiba.cm réinitialisé!")
            print("Nouveau mot de passe: 123456")
            return True
        else:
            print("❌ admin@iiba.cm non trouvé")
            return False
            
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

def reset_all_users():
    """
    Supprime tous les utilisateurs et recrée admin@iiba.cm
    """
    DATA_DIR = Path("./data")
    USERS_PATH = DATA_DIR / "users.csv"
    
    try:
        # Supprimer le fichier existant
        if USERS_PATH.exists():
            USERS_PATH.unlink()
            print("🗑️ Ancien fichier users.csv supprimé")
        
        # Créer un nouveau fichier avec admin
        USER_COLS = ["user_id", "full_name", "role", "active", "pwd_hash", "must_change_pw", "created_at", "updated_at"]
        
        new_password = "123456"
        pwd_hash = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        
        admin_data = {
            "user_id": "admin@iiba.cm",
            "full_name": "Admin IIBA Cameroun",
            "role": "admin",
            "active": "1",
            "pwd_hash": pwd_hash,
            "must_change_pw": "0",
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        
        df_new = pd.DataFrame([admin_data])
        df_new.to_csv(USERS_PATH, index=False, encoding="utf-8")
        
        print("✅ Fichier users.csv recréé!")
        print("Email: admin@iiba.cm")
        print("Mot de passe: 123456")
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

if __name__ == "__main__":
    print("🔓 DÉBLOCAGE COMPTE ADMIN - IIBA Cameroun")
    print("="*50)
    
    print("\n1️⃣ Tentative de création d'un nouvel admin...")
    if create_new_admin():
        print("\n✅ SUCCÈS! Vous pouvez vous connecter avec admin2@iiba.cm / 123456")
    else:
        print("\n2️⃣ Tentative de réinitialisation du mot de passe admin@iiba.cm...")
        if reset_admin_password():
            print("\n✅ SUCCÈS! Vous pouvez vous connecter avec admin@iiba.cm / 123456")
        else:
            print("\n3️⃣ Réinitialisation complète des utilisateurs...")
            if reset_all_users():
                print("\n✅ SUCCÈS! Vous pouvez vous connecter avec admin@iiba.cm / 123456")
            else:
                print("\n❌ ÉCHEC TOTAL - Contactez le support technique")
    
    print("\n" + "="*50)
    print("🎯 RÉSUMÉ DES COMPTES DISPONIBLES:")
    print("• admin@iiba.cm / 123456")
    print("• admin2@iiba.cm / 123456")
    print("="*50)
