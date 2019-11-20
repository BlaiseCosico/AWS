import boto3, csv, json, logging
from pprint import pprint
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

def get_cost_and_usage_data(start, end):

	ce_obj = boto3.client('ce')
	data = ce_obj.get_cost_and_usage(TimePeriod={'Start':start, 'End':end},
									 Granularity='DAILY', #MONTHY, DAILY, HOURLY
									 Metrics=['BLENDED_COST','UsageQuantity'],
									 GroupBy=[{'Type':'DIMENSION', 'Key':'SERVICE'}, {'Type':'DIMENSION', 'Key':'LINKED_ACCOUNT'}] #use LINKED_ACCOUNT to get key of account
									 )	#returns a dictionary

	return data

def export_to_csv(data, date_now):

	filename = f'tmp/bill_{date_now.strftime("%Y-%m-%d")}.csv'
	
	try:
		with open(filename, 'w+') as csvfile: #write into a csv file, create a new one if none exists
			fieldnames = ['TimePeriod', 'LinkedAccount', 'Service', 'Amount', 'Unit', 'Estimated'] #set column headers
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
			writer.writeheader()

			for results_by_time in data['ResultsByTime']: #parse data object returned by aws ce api
				for group in results_by_time['Groups']: 
						amount = group['Metrics']['BlendedCost']['Amount'] #get amount
						unit = group['Metrics']['BlendedCost']['Unit']     #get unit(USD)
						
						writer.writerow({'TimePeriod': results_by_time['TimePeriod']['Start'],  #write into csv file
										'LinkedAccount': group['Keys'][1],
										'Service': group['Keys'][0],
										'Amount': amount,
										'Unit': unit,
										'Estimated': results_by_time['Estimated']})
	except IOError as e:
		print("({})".format(e))


	return filename

def upload_to_s3(file_name, bucket, object_name=None):
	if object_name is None:
		object_name = file_name

	s3_obj = boto3.client('s3')
	try:
		s3_obj.upload_file(file_name, bucket, object_name)
	except ClientError as e:
			logging.error(e)
			return False
	return True

def lambda_handler(event, context):
	date_now = datetime.utcnow()
	start = (date_now-timedelta(days=10)).strftime('%Y-%m-%d')
	end = date_now.strftime('%Y-%m-%d')

	data = get_cost_and_usage_data(start, end)
	file = export_to_csv(data, date_now)
	upload_to_s3(file, 'csvcebucket')


	return {
		'statusCode': 200,
	}



