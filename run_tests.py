#!/usr/bin/env python3
"""
Script pour exécuter les tests du projet ETL World Cup.
Utilise pytest pour découvrir et exécuter tous les tests.
"""

import subprocess
import sys
import os

def run_tests():
    """Exécute tous les tests avec pytest."""
    print("Exécution des tests ETL World Cup...")
    print("=" * 50)

    # Commande pytest
    cmd = [
        sys.executable, "-m", "pytest",
        "data/Tests/",  # Répertoire des tests
        "-v",  # Verbose
        "--tb=short",  # Format d'erreur court
        "--cov=extract",  # Coverage pour extract.py
        "--cov=transform",  # Coverage pour transform.py
        "--cov=load",  # Coverage pour load.py
        "--cov-report=term-missing"  # Rapport de coverage
    ]

    try:
        result = subprocess.run(cmd, cwd=os.getcwd())
        return result.returncode == 0
    except FileNotFoundError:
        print("Erreur: pytest n'est pas installé. Installez-le avec: pip install pytest pytest-cov")
        return False

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)