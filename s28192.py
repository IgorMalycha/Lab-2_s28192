import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import StandardScaler
import gspread
from google.oauth2.service_account import Credentials
import sys
import os

# # --- KONFIGURACJA LOGGERA ---
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler("log.txt"),
#         logging.StreamHandler(sys.stdout)
#     ]
# )
# logger = logging.getLogger(__name__)

# --- FUNKCJA DO POBRANIA DANYCH Z GOOGLE SHEETS ---
def get_data_from_sheets():
    # logger.info("Odczytywanie danych z Google Sheets...")
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    sheet_id = os.getenv("SHEET_ID")

    if not creds_json or not sheet_id:
        # logger.warning("Brak danych uwierzytelniających lub ID arkusza — wczytuję z CSV.")
        return pd.read_csv("data.csv")

    creds_dict = eval(creds_json)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    # logger.info(f"Pobrano {len(df)} rekordów z Google Sheets.")
    return df

# --- CZYSZCZENIE I STANDARYZACJA ---
def clean_and_standardize(df: pd.DataFrame):
    total_cells = df.size
    missing_before = df.isna().sum().sum()

    # logger.info("Rozpoczynam czyszczenie danych...")

    # Usuwanie wierszy z więcej niż 3 brakami
    before_rows = len(df)
    df = df.dropna(thresh=len(df.columns) - 3)
    after_rows = len(df)
    removed_rows = before_rows - after_rows

    # Uzupełnianie braków (dla kolumn numerycznych)
    for col in df.select_dtypes(include=[np.number]).columns:
        df[col].fillna(df[col].median(), inplace=True)

    # Dla kolumn tekstowych
    for col in df.select_dtypes(include=[object]).columns:
        df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else "Brak danych", inplace=True)

    missing_after = df.isna().sum().sum()
    filled = missing_before - missing_after

    # Standaryzacja danych numerycznych
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    scaler = StandardScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

    changed_percent = (filled / total_cells) * 100
    removed_percent = (removed_rows / before_rows) * 100

    # logger.info(f"Uzupełniono {changed_percent:.2f}% danych, usunięto {removed_percent:.2f}% wierszy.")
    return df, changed_percent, removed_percent

# --- GENEROWANIE RAPORTU ---
def generate_report(changed_percent, removed_percent):
    with open("report.txt", "w", encoding="utf-8") as f:
        f.write("RAPORT Z CZYSZCZENIA DANYCH\n")
        f.write("=============================\n")
        f.write(f"Uzupełniono: {changed_percent:.2f}% danych\n")
        f.write(f"Usunięto: {removed_percent:.2f}% danych\n")
    # logger.info("Raport został zapisany w pliku report.txt")

if __name__ == "__main__":
    # --- AUTOMATYCZNE URUCHOMIENIE generator_danych.py ---
    student_number = int(os.getenv("STUDENT_NUMBER", "28192"))
    os.system(f"python generator_danych.py -s {student_number}")
    df = pd.read_csv(f"data_student_{student_number}.csv")
    df.to_csv("data.csv", index=False)

    # --- PRZETWARZANIE ---
    df, changed, removed = clean_and_standardize(df)
    generate_report(changed, removed)
    df.to_csv("cleaned_data.csv", index=False)
    # logger.info("Proces zakończony pomyślnie ✅")
