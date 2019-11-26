import boto3, csv
from pprint import pprint
from datetime import datetime, timedelta


def get_cost_and_usage_data(start, end):

	ce_obj = boto3.client('ce')
	data = ce_obj.get_cost_and_usage(TimePeriod={'Start':'2019-10-01', 'End':'2019-11-01'},
									 Granularity='MONTHLY', #MONTHLY, DAILY, HOURLY
									 Metrics=['BLENDED_COST', 'UNBLENDED_COST', 'AMORTIZED_COST'], #, 'NET_AMORTIZED_COST', 'NET_UNBLENDED_COST', 'USAGE_QUANTITY', 'NORMALIZED_USAGE_AMOUNT'
									 GroupBy=[{'Type':'DIMENSION', 'Key':'LINKED_ACCOUNT'},{'Type':'DIMENSION', 'Key':'SERVICE'}] #use LINKED_ACCOUNT to get key of account
									 )	#returns a dictionary

	return data

def export_to_csv(data, date_now):
	try:
		with open('test_data5.csv', 'w+') as csvfile: #write into a csv file, create a new one if none exists
			fieldnames = ['TimePeriod', 'LinkedAccount', 'LinkedAccountName', 'Service', 'Amount', 'Unit', 'Estimated'] #set column headers
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
			writer.writeheader()

			for results_by_time in data['ResultsByTime']: #parse data object returned by aws ce api
				for group in results_by_time['Groups']: 
						amount = group['Metrics']['BlendedCost']['Amount'] #get amount
						unit = group['Metrics']['BlendedCost']['Unit']     #get unit(USD)
						
						writer.writerow({'TimePeriod': results_by_time['TimePeriod']['Start'],  #write into csv file
										'LinkedAccount': group['Keys'][0],
										'LinkedAccountName': get_name(group['Keys'][0]),
										'Service': group['Keys'][1],
										'Amount': amount,
										'Unit': unit,
										'Estimated': results_by_time['Estimated']})
	except IOError as e:
		print("({})".format(e))

	return None

names = {}
def get_name(account_id):

	if account_id in names:
		return names[account_id]
	else:
		org_obj = boto3.client('organizations')
		response = org_obj.describe_account(AccountId=account_id)
		names[account_id] = response['Account']['Name']
	print(names)
	return names[account_id]


if __name__ == '__main__':
	date_now = datetime.utcnow()
	start = (date_now-timedelta(days=10)).strftime('%Y-%m-%d')
	end = date_now.strftime('%Y-%m-%d')

	data = get_cost_and_usage_data(start, end)
	export_to_csv(data, date_now)
	print('done.')
