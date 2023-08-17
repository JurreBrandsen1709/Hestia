import random
import json

# Create a dict with 1000 keys and 1000 random values between 800- and 1200
data = {i: random.randint(800, 1200) for i in range(4000)}

# save the dict to a json file.
with open('w3_priority.json', 'w') as f:
    json.dump(data, f)