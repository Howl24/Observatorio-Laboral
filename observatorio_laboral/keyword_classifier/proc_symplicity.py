SOURCE = "new_btpucp"

languages_category = "languages"                # Nombre de la categoria
languages_proc_code = 2                         # Codigo de procesamiento
languages_fields = ['Qualifications',           # Campos de la convocatoria
                    'Description',              # a utilizar en el procesamiento
                    'Job Title',
                    ]

software_category = "software"
software_proc_code = 2
software_fields = ['Qualifications',
                   'Description',
                   'Job Title',
                   ]

degrees_category = "degrees"
degrees_proc_code = 3
degrees_fields = ['Degree Level',
                  'Qualifications',
                  'Job Title',
                  ]


positions_category = "positions"
positions_proc_code = 3
positions_fields = ['Position Level',
                    'Job Title',
                    ]

configurations = {
        languages_category: (languages_proc_code, languages_fields),
        software_category: (software_proc_code, software_fields),
        degrees_category: (degrees_proc_code, degrees_fields),
        positions_category: (positions_proc_code, positions_fields),
        }

{



