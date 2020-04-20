import boto3
import time

s3 = boto3.resource('s3')

userInput = ''
while userInput != "exit":
	userInput = str(input('Do you wish to [search] or [download] or [exit]? '))
	print("you entered " + userInput)
	
	if userInput == "search":
		userInput = str(input('All containers [all], containter [con], object [obj]? '))
		if userInput == "all":
			# Print out all obj names
			# www.stackoverflow.com/a/30249553
			startTime = time.time()
			for bucket in s3.buckets.all():
				print(bucket.name)
				for file in bucket.objects.all():
					print("    "+file.key)
			endTime = time.time()
			print("All containers result took " + str(endTime - startTime) + " seconds")

		elif userInput == "con":
			# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html?highlight=s3#S3.Bucket
			bucketInput = str(input("Enter bucket/container name: "))
			try:
				startTime = time.time()
				bucketInput = s3.Bucket(bucketInput)
				for file in bucketInput.objects.all():
					print(file.key)
				endTime = time.time()
				print("Single container result took " + str(endTime - startTime) + " seconds")
			except:
				print("Failed to find bucket/container")
		elif userInput == "obj":
			#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.objects
			objectInput = str(input("Enter object name: "))
			startTime = time.time()
			for bucket in s3.buckets.all():
				for file in bucket.objects.filter(Prefix=objectInput):
					print("Container: " + bucket.name + " File: " + file.key)
			endTime = time.time()
			print("Find object result took " + str(endTime - startTime) + " seconds")
		else:
			print("Unknown command")
				
	elif userInput == "download":
		# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
		objectInput = str(input("Enter exact object name: "))
		count = 0
		startTime = time.time()
		for bucket in s3.buckets.all():
			for file in bucket.objects.filter(Prefix=objectInput):
				print(bucket.name, file.key)
				count+=1
				bucketName = bucket.name
				objName = file.key
		if count == 1:
			try:
				sus3 = boto3.client('s3')
				sus3.download_file(bucketName, objName, objName)
				print("downloaded " + objName + " to current directory")

			except:
				print("failed to download") 
		else:
			print("Input returned either 0 or too many results")
		endTime = time.time()
		print("Download took " + str(endTime - startTime) + " seconds")

	else:
		if userInput != "exit":
			print("Unknown command")
