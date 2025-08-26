@echo off
cd /d "Renseignez le chemin du dossier acceuillant les scripts ici"
REM Active l'environnement Conda
CALL C:\ProgramData\anaconda3\Scripts\activate.bat "<= chemin pour activer l'environnement python (supprimer ce qu'il y a entre parenthèses" C:\Users\LouisPC\.conda\envs\sig_vc_test "<= chemin de l'environnement python"
python pipeline.py --update
pause
