from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "generated")
FHIR_DIR = os.path.join(BASE_DIR, "data", "sample_fhir")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FHIR_DIR, exist_ok=True)


def generate_patients(n: int = 1000) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "patient_id": f"P{i:06d}",
            "given_name": fake.first_name(),
            "family_name": fake.last_name(),
            "gender": random.choice(["male", "female"]),
            "birth_date": fake.date_of_birth(minimum_age=1, maximum_age=95).isoformat(),
            "city": fake.city(),
            "source_system": random.choice(["ehr_a", "ehr_b", "registration_csv"]),
        })
    return pd.DataFrame(rows)


def generate_encounters(patients_df: pd.DataFrame, n: int = 3000) -> pd.DataFrame:
    rows = []
    patient_ids = patients_df["patient_id"].tolist()
    for i in range(1, n + 1):
        admit_time = fake.date_time_between(start_date="-180d", end_date="now")
        discharge_time = admit_time + timedelta(hours=random.randint(2, 120))
        rows.append({
            "encounter_id": f"E{i:07d}",
            "patient_id": random.choice(patient_ids),
            "provider_id": f"DR{random.randint(1, 200):04d}",
            "encounter_type": random.choice(["inpatient", "outpatient", "emergency"]),
            "admit_time": admit_time.isoformat(),
            "discharge_time": discharge_time.isoformat(),
            "hospital_unit": random.choice(["ICU", "Ward-A", "Ward-B", "ER", "Lab"]),
        })
    return pd.DataFrame(rows)


def generate_observations(encounters_df: pd.DataFrame, n: int = 10000) -> pd.DataFrame:
    rows = []
    encounter_records = encounters_df.to_dict("records")
    observation_codes = [
        ("heart_rate", "bpm"),
        ("spo2", "%"),
        ("temperature", "C"),
        ("resp_rate", "breaths/min"),
    ]

    for i in range(1, n + 1):
        enc = random.choice(encounter_records)
        code, unit = random.choice(observation_codes)
        if code == "heart_rate":
            value = random.randint(50, 140)
        elif code == "spo2":
            value = random.randint(85, 100)
        elif code == "temperature":
            value = round(random.uniform(35.5, 40.0), 1)
        else:
            value = random.randint(10, 30)

        rows.append({
            "observation_id": f"O{i:08d}",
            "patient_id": enc["patient_id"],
            "encounter_id": enc["encounter_id"],
            "observation_code": code,
            "observation_value": value,
            "unit": unit,
            "effective_time": fake.date_time_between(start_date="-180d", end_date="now").isoformat(),
            "abnormal_flag": value > 120 if code == "heart_rate" else False,
        })
    return pd.DataFrame(rows)


def write_fhir_patients(patients_df: pd.DataFrame, limit: int = 50) -> None:
    for _, row in patients_df.head(limit).iterrows():
        resource = {
            "resourceType": "Patient",
            "id": row["patient_id"],
            "name": [{"given": [row["given_name"]], "family": row["family_name"]}],
            "gender": row["gender"],
            "birthDate": row["birth_date"],
            "address": [{"city": row["city"]}],
        }
        with open(os.path.join(FHIR_DIR, f"{row['patient_id']}.json"), "w", encoding="utf-8") as f:
            json.dump(resource, f, indent=2)


def main() -> None:
    patients = generate_patients()
    encounters = generate_encounters(patients)
    observations = generate_observations(encounters)

    patients.to_csv(os.path.join(OUTPUT_DIR, "patients.csv"), index=False)
    encounters.to_csv(os.path.join(OUTPUT_DIR, "encounters.csv"), index=False)
    observations.to_csv(os.path.join(OUTPUT_DIR, "observations.csv"), index=False)

    write_fhir_patients(patients)

    print("Synthetic healthcare data generated successfully.")


if __name__ == "__main__":
    main()