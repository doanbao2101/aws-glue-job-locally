import re
from collections import defaultdict

def detect_categories(sheet_names):
    categories = defaultdict(list)

    for s in sheet_names:
        match = re.match(r"^([A-Za-z]+)?\s*(FY\d+)$", s.strip(), re.IGNORECASE)
        if match:
            prefix, fy = match.groups()
            if prefix:
                cat = prefix.upper()  
            else:
                cat = "FY"
            categories[cat].append(s)
        else:
            categories["OTHERS"].append(s)

    return dict(categories)


# Example usage
sheet_names = ["Hart FY20"]
categories = detect_categories(sheet_names)
keys = list(categories.keys())
print(keys)
