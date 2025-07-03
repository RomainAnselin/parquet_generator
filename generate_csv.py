import pandas as pd
import string
import random

# Function to generate random string of 20 alphanumeric characters
def generate_random_string(length=20):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

# Generate data
n_rows = 1_000_000
data = {
    'id': range(1, n_rows + 1),
    'column1': [generate_random_string() for _ in range(n_rows)],
    'date': ['0001-01-01'] * n_rows
}

# Create DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv('output.csv', index=False)

print("CSV file has been generated successfully!") 