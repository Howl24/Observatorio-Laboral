from observatorio_laboral.dictionary.dictionary import Dictionary
from observatorio_laboral.dictionary.term import Term

dictionary = Dictionary("Diccionario_Economía")
print(dictionary.configurations)


import csv
with open("dic.csv") as csvfile:
    reader = csv.DictReader(csvfile)

    marks = set()
    for row in reader:
        mark = row['Economía']
        if mark == 's':
            term = Term("Diccionario_Economía", row['Concepto'],
                        row['Concepto'], True, 1)
            dictionary.add_term(term)

    dictionary.save()
