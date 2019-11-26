import csv

with open('../test_data5.csv', newline='') as f:
	csv_reader = csv.DictReader(f)
	line_count = 0
	unique_linked_accounts = []

	for row in csv_reader:
		if line_count == 0:
			print(f'{" ".join(row)}')
			line_count += 1
		print(f'\t{row["TimePeriod"]} {row["LinkedAccount"]} {row["Service"]} {row["Amount"]} {row["Unit"]} {row["Estimated"]}')
		line_count += 1


		if row['LinkedAccount'] not in unique_linked_accounts:
			unique_linked_accounts.append(row['LinkedAccount'])

			with open(f'test_with_names/{row["LinkedAccountName"]}_{row["LinkedAccount"]}.csv', mode='w+') as account_file:
				fieldnames = ['TimePeriod', 'LinkedAccount', 'LinkedAccountName', 'Service', 'Amount', 'Unit', 'Estimated'] #set column headers
				writer = csv.DictWriter(account_file, fieldnames=fieldnames)
				writer.writeheader()

				writer.writerow({
					'TimePeriod': row["TimePeriod"],
					'LinkedAccount': row["LinkedAccount"],
					'LinkedAccountName' : row["LinkedAccountName"],
					'Service': row["Service"],
					'Amount': row["Amount"],
					'Unit': row["Unit"],
					'Estimated': row["Estimated"]
					})
		else:
			with open(f'test_with_names/{row["LinkedAccountName"]}_{row["LinkedAccount"]}.csv', mode='a') as account_file:
				writer = csv.DictWriter(account_file, fieldnames=fieldnames)
		
				writer.writerow({
					'TimePeriod': row["TimePeriod"],
					'LinkedAccount': row["LinkedAccount"],
					'LinkedAccountName' : row["LinkedAccountName"],
					'Service': row["Service"],
					'Amount': row["Amount"],
					'Unit': row["Unit"],
					'Estimated': row["Estimated"]
					})


	print(f'Total lines: {line_count}')
	print(unique_linked_accounts)
	print(len(unique_linked_accounts))

# we need everything to be transfered to the new csv BUT
# we only need to check if key is unique
# write inside read?
# TimePeriod	LinkedAccount	Service	Amount	Unit	Estimated
# use pandas instead????

#if id is same as before, add row to current file (or if in list)
#else, make a new file