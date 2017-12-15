import csv
from observatorio_laboral.offer.offer import Offer

Offer.ConnectToDatabase("l4", "l4_offers")

with open("eco.csv") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        id = row['Id']
        year = int(row['Año'])
        month = int(row['Mes'])
        source = row['Fuente']

        features = {}
        features['Job Title'] = row['Job Title']
        features['Description'] = row['Description']
        features['Qualifications'] = row['Qualifications']

        areas = {"EI":row['EI'],
                "F": row['F'],
                "TE": row['TE'],
                "OI": row['OI'],
                "MC": row['MC'],
                "P": row['P'],
                "EM": row['EM']}

        features['Areas'] = ",".join([area for area, mark in areas.items() if mark == "x" or mark == "X"])

        offer = Offer(source, year, month, id, features, careers=set(['ECONOMÍA']))
        offer.Insert()




