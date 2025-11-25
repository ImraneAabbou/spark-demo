import csv
import random
from datetime import datetime, timedelta

# Create sample orders
orders_data = []
statuses = ['Shipped', 'Processing', 'On Hold', 'Cancelled']
payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cash']
start_date = datetime(2024, 1, 1)

# Generate 5000 orders
for i in range(1, 5001):
    order_date = start_date + timedelta(days=random.randint(0, 330))
    customer_id = random.randint(1, 1000)
    amount = round(random.uniform(100, 10000), 2)
    status = random.choice(statuses)
    payment = random.choice(payment_methods)
    
    orders_data.append([
        i,  # orderNumber
        order_date.strftime('%Y-%m-%d'),  # orderDate
        order_date.strftime('%Y-%m-%d'),  # requiredDate (same for simplicity)
        status,  # status
        customer_id,  # customerNumber
        amount,  # amount
        payment  # paymentMethod
    ])

# Write to CSV
output_path = 'spark-data/ecommerce/orders.csv'
with open(output_path, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'orderNumber', 'orderDate', 'requiredDate', 'status',
        'customerNumber', 'amount', 'paymentMethod'
    ])
    writer.writerows(orders_data)

print("Orders data created successfully!")
print(f"Created {len(orders_data)} order records")

