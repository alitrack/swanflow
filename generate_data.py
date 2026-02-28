import csv
import random

depts = ['IT', 'Sales', 'HR', 'Finance']
with open('users.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'name', 'age', 'salary', 'dept'])
    for i in range(1, 101):
        writer.writerow([
            i, 
            f'user_{i}', 
            random.randint(18, 70), 
            random.randint(3000, 15000), 
            random.choice(depts)
        ])
print("Created users.csv with 100 rows using standard csv module.")
