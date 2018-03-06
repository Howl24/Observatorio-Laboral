SOURCE = "example"                 # nombre de la fuente

category_name = "positions"        # nombre de la category

processing_code = 2                # 1 -> Return field value 
                                   # 2 -> Return all the representative words
                                   # 3 -> Return the first representative word

fields = ['Position Level',        # Offer fields list where to find a 
          'Job Title',             # representative word in order.
          ]
                                
        

# Dictionary containing all the configurations by name.
configurations = {
        category_name: (processing_code, fields),
        }
