import os, uuid
import time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try:
	
	# Retrieve the connection string for use with the application. 
	connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

	# Create the BlobServiceClient object which will be used to create a container client
	blob_service_client = BlobServiceClient.from_connection_string(connect_str)

	userInput = ''
	while userInput != 'exit':
		userInput = str(input("Do you wish to [search] or [download] or [exit]? "))
		print("you entered : " + userInput)
		
		if userInput == 'search':
			userInput = str(input("All containers [all], container [con], object [obj]? "))
			
			if userInput == "all":
				# Create a unique name for the container
				container_names = ["cis1300","cis3110","cis4010"]
				startTime = time.time()
				for container_name in container_names:
					# get the container
					# https://pypi.org/project/azure-storage-blob/
					container_client = blob_service_client.get_container_client(container_name)
					print(container_name)
					# List the blobs in the container
					blob_list = container_client.list_blobs()
					for blob in blob_list:
						print("\t" + blob.name)
				endTime = time.time()
				print("Time to query all containers: " + str(endTime - startTime) + " seconds")
			elif userInput == "con":
				bucketInput = str(input("Enter bucket/container name: "))
				startTime = time.time()
				container_client = blob_service_client.get_container_client(bucketInput)
				blob_list = container_client.list_blobs()
				for blob in blob_list:
					print("\t" + blob.name)
				endTime = time.time()
				print("Time to get single container: " + str(endTime - startTime) + " seconds")
			
			elif userInput == "obj":
				objectInput = str(input("Enter object name: "))
				container_names = ["cis1300","cis3110","cis4010"]
				startTime = time.time()
				for container_name in container_names:
					container_client = blob_service_client.get_container_client(container_name)
					blob_list = container_client.list_blobs()
					for blob in blob_list:
						if objectInput in blob.name:
							print("Container: " + container_name + " File: " + blob.name)
				endTime = time.time()
				print("Time to query single object: " + str(endTime - startTime) + " seconds")

		elif userInput == 'download':
			objectInput = str(input("Enter exact object name: "))
			count = 0
			container_names = ["cis1300","cis3110","cis4010"]
			startTime = time.time()
			for container_name in container_names:
				container_client = blob_service_client.get_container_client(container_name)
				blob_list = container_client.list_blobs()
				for blob in blob_list:
					if objectInput in blob.name:
						local_file_name = blob.name
						blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
						download_file_path = os.path.join(os.getcwd(), local_file_name)
						print("\nDownloading blob to \n\t" + download_file_path)
						with open(download_file_path, "wb") as download_file:
							download_file.write(blob_client.download_blob().readall())
			endTime = time.time()
			print("time to download: " + str(endTime - startTime) + " seconds")

		else:
			if userInput != 'exit':
				print("wrong input")
except Exception as ex:
	print('Exception:')
	print(ex)
