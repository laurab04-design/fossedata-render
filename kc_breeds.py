# kc_breeds.py

KC_BREEDS = [
    # Hound Group
    "Afghan Hound", "Basenji", "Basset Fauve de Bretagne",
    "Grand Basset Griffon Vendeen", "Petit Basset Griffon Vendeen", "Basset Hound",
    "Beagle", "Bloodhound", "Borzoi", "Cirneco dell'Etna", "Dachshund",
    "Dachshund (Long Haired)", "Dachshund (Miniature Long Haired)",
    "Dachshund (Smooth Haired)", "Dachshund (Miniature Smooth Haired)",
    "Dachshund (Wire Haired)", "Dachshund (Miniature Wire Haired)",
    "Scottish Deerhound", "Finnish Spitz", "Greyhound", "Hamiltonstovare",
    "Harrier", "Ibizan Hound", "Irish Wolfhound", "Norwegian Elkhound",
    "Otterhound", "Pharaoh Hound", "Portuguese Podengo", "Rhodesian Ridgeback",
    "Saluki", "Sloughi", "Whippet",

    # Gundog Group
    "Brittany", "Bracco Italiano", "German Shorthaired Pointer",
    "German Longhaired Pointer", "German Wirehaired Pointer", "Gordon Setter",
    "Hungarian Vizsla", "Hungarian Wirehaired Vizsla", "Italian Spinone",
    "Irish Red and White Setter", "Irish Setter", "English Setter", "Pointer",
    "Weimaraner", "Large Munsterlander", "Small Munsterlander", "Lagotto Romagnolo",
    "Kooikerhondje", "Spanish Water Dog", "Labrador Retriever",
    "Flat Coated Retriever", "Curly Coated Retriever", "Chesapeake Bay Retriever",
    "Nova Scotia Duck Tolling Retriever", "English Springer Spaniel",
    "Welsh Springer Spaniel", "Cocker Spaniel", "Clumber Spaniel", "Field Spaniel",
    "Irish Water Spaniel", "Sussex Spaniel", "American Cocker Spaniel",
    "Slovakian Rough Haired Pointer", "Braque d'Auvergne",

    # Terrier Group
    "Airedale Terrier", "American Hairless Terrier", "Australian Terrier",
    "Bedlington Terrier", "Border Terrier", "Bull Terrier", "Miniature Bull Terrier",
    "Cairn Terrier", "Cesky Terrier", "Dandie Dinmont Terrier", "Smooth Fox Terrier",
    "Wire Fox Terrier", "Glen of Imaal Terrier", "Irish Terrier", "Jack Russell Terrier",
    "Kerry Blue Terrier", "Lakeland Terrier", "Manchester Terrier", "Norfolk Terrier",
    "Norwich Terrier", "Parson Russell Terrier", "Scottish Terrier", "Sealyham Terrier",
    "Skye Terrier", "Soft Coated Wheaten Terrier", "Staffordshire Bull Terrier",
    "Welsh Terrier", "West Highland White Terrier",

    # Utility Group
    "Akita", "Japanese Akita Inu", "Boston Terrier", "Bulldog", "Chow Chow",
    "Dalmatian", "French Bulldog", "German Spitz", "Japanese Spitz", "Keeshond",
    "Lhasa Apso", "Schipperke", "Schnauzer", "Miniature Schnauzer", "Shar Pei",
    "Shih Tzu", "Tibetan Spaniel", "Tibetan Terrier", "Xoloitzcuintle", "Xoloitzcuintli",
    "Mexican Hairless", "Poodle", "Standard Poodle", "Miniature Poodle", "Toy Poodle",
    "Shiba Inu", "Japanese Shiba Inu", "Canaan Dog", "Eurasier",

    # Pastoral Group
    "Anatolian Shepherd Dog", "Kangal Shepherd Dog", "Australian Cattle Dog",
    "Australian Shepherd", "Bearded Collie", "Belgian Shepherd Dog", "Belgian Malinois",
    "Belgian Tervueren", "Belgian Groenendael", "Belgian Laekenois", "Border Collie",
    "Briard", "Catalan Sheepdog", "Rough Collie", "Smooth Collie", "German Shepherd Dog",
    "Komondor", "Kuvasz", "Finnish Lapphund", "Icelandic Sheepdog", "Norwegian Buhund",
    "Old English Sheepdog", "Polish Lowland Sheepdog", "Pyrenean Mountain Dog",
    "Shetland Sheepdog", "Swedish Vallhund", "Cardigan Welsh Corgi", "Pembroke Welsh Corgi",
    "Samoyed", "White Swiss Shepherd Dog",

    # Working Group
    "Alaskan Malamute", "Bernese Mountain Dog", "Bouvier des Flandres", "Boxer",
    "Bullmastiff", "Canadian Eskimo Dog", "Cane Corso", "Dobermann", "Dogue de Bordeaux",
    "Estrela Mountain Dog", "Great Dane", "Greater Swiss Mountain Dog", "Greenland Dog",
    "Hovawart", "Leonberger", "Mastiff", "Neapolitan Mastiff", "Newfoundland",
    "Portuguese Water Dog", "Rottweiler", "Russian Black Terrier", "St. Bernard",
    "Siberian Husky", "Tibetan Mastiff", "Giant Schnauzer", "German Pinscher",

    # Toy Group 
    "Affenpinscher", "Bichon Frise", "Bolognese", "Cavalier King Charles Spaniel",
    "Chihuahua", "English Toy Terrier", "Griffon Bruxellois", "Havanese",
    "Italian Greyhound", "Japanese Chin", "King Charles Spaniel", "Lowchen",
    "Maltese", "Miniature Pinscher", "Papillon", "Pekingese", "Pomeranian", "Pug",
    "Russian Toy", "Yorkshire Terrier", "Chinese Crested", "Coton de Tulear",

    # Broad catchalls
    "terrier", "hound", "toy", "spaniel", "bulldog", "collie",
    "spitz", "sheepdog", "pastoral", "working", "corgi", "pointer", "setter", "bichon", "basset", "heeler","limited", "beauceron"
]

# Deduplicate and sort for sanity
KC_BREEDS = sorted(set(KC_BREEDS), key=str.casefold)
