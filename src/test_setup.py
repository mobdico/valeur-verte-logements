#!/usr/bin/env python3
"""
Script de test de la configuration du projet
"""

import sys
from pathlib import Path

# Ajouter le répertoire src au path
src_path = Path(__file__).parent
sys.path.insert(0, str(src_path))

def test_imports():
    """Test des imports des modules principaux"""
    try:
        import config
        print("✅ Module config importé avec succès")
        
        # Test de la configuration
        config_data = config.get_config()
        print(f"✅ Configuration chargée : {len(config_data)} sections")
        
        return True
    except ImportError as e:
        print(f"❌ Erreur d'import : {e}")
        return False
    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")
        return False

def test_directories():
    """Test de la création des répertoires"""
    try:
        from config import ensure_directories
        
        # Créer les répertoires
        ensure_directories()
        print("✅ Répertoires créés avec succès")
        
        # Vérifier qu'ils existent
        from config import DATA_DIR, LOGS_DIR
        assert DATA_DIR.exists(), f"Le répertoire {DATA_DIR} n'existe pas"
        assert LOGS_DIR.exists(), f"Le répertoire {LOGS_DIR} n'existe pas"
        
        print("✅ Vérification des répertoires réussie")
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de la création des répertoires : {e}")
        return False

def main():
    """Fonction principale de test"""
    print("🧪 Test de la configuration du projet Valeur Verte des Logements")
    print("=" * 60)
    
    tests = [
        ("Test des imports", test_imports),
        ("Test des répertoires", test_directories)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n🔍 {test_name}...")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Erreur lors du test : {e}")
            results.append((test_name, False))
    
    # Résumé des tests
    print("\n📊 Résumé des tests :")
    print("=" * 40)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    print(f"\n🎯 Résultat global : {passed}/{total} tests réussis")
    
    if passed == total:
        print("🎉 Tous les tests sont passés ! Le projet est prêt.")
        return 0
    else:
        print("⚠️  Certains tests ont échoué. Vérifiez la configuration.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
