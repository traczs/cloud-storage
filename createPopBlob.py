import time
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try:

	# Retrieve the connection string for use with the application. 
	connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

	# Create the BlobServiceClient object which will be used to create a container client
	blob_service_client = BlobServiceClient.from_connection_string(connect_str)

	######CIS1300 container
	
	# Create a unique name for the container
	container_name = "cis1300"
	# Create the container
	startTime = time.time()
	container_client = blob_service_client.create_container(container_name)
	endTime = time.time()
	print("Time it takes to make " + container_name + ": " + str(endTime - startTime) + " seconds")

	startTime = time.time()
	# Create a file in local Documents directory to upload and download
	local_path = "./data"
	local_file_name = "1300Assignment1.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)

	# Create a blob client using the local file name as the name for the blob
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

	# Upload the created file
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)
		
	local_file_name = "1300Assignment2.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)

	local_file_name = "1300Assignment3.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)	
		
	local_file_name = "1300Assignment4.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)	
	
	endTime = time.time()
	print("Time it took to upload data to " + container_name + ": " + str(endTime - startTime) + " seconds") 
	
	######cis3110 container
	# Create a unique name for the container
	container_name = "cis3110"
	# Create the container
	container_client = blob_service_client.create_container(container_name)

	# Create a file in local Documents directory to upload and download
	local_path = "./data"
	local_file_name = "3110Lecture1.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)

	# Create a blob client using the local file name as the name for the blob
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

	# Upload the created file
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)
		
	local_file_name = "3110Lecture2.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)

	local_file_name = "3110Lecture3.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)
		
	local_file_name = "3110Assignment1.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)
		
	#####CIS4010 container
	# Create a unique name for the container
	container_name = "cis4010"
	# Create the container
	container_client = blob_service_client.create_container(container_name)

	# Create a file in local Documents directory to upload and download
	local_path = "./data"
	local_file_name = "4010Lecture1.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)

	# Create a blob client using the local file name as the name for the blob
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

	# Upload the created file
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)
		
	local_file_name = "4010Lecture2.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)
	
	local_file_name = "4010Assignment1.pdf"
	upload_file_path = os.path.join(local_path, local_file_name)
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
	print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
	with open(upload_file_path, "rb") as data:
		blob_client.upload_blob(data)
		
		
	
	"""	
	# List the blobs in the container
	print("\nListing blobs...")
	blob_list = container_client.list_blobs()
	for blob in blob_list:
		print("\t" + blob.name)

	# Download the blob to a local file
	# Add 'DOWNLOAD' before the .txt extension so you can see both files
	download_file_path = os.path.join(local_path, str.replace(local_file_name ,'.txt', 'DOWNLOAD.txt'))
	print("\nDownloading blob to \n\t" + download_file_path)

	with open(download_file_path, "wb") as download_file:
		download_file.write(blob_client.download_blob().readall())
	"""
except Exception as ex:
	print('Exception:')
	print(ex)
