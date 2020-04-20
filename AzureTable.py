from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import time
import json
import decimal
import os
# https://docs.microsoft.com/en-us/azure/cosmos-db/table-storage-how-to-use-python
#  Not used anymore https://pypi.org/project/azure-cosmos/
# NOT USED ANYMORE BUT MAY BE USEFUL IN THE FUTURE -> https://github.com/Azure/azure-cosmos-python/blob/master/samples/DatabaseManagement/Program.py

cs = os.getenv('AZURE_DB_CONNECTION_STRING')
table_service = TableService(connection_string=cs)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


try:
    startTime = time.time()  # time start create db
    table_service.create_table('IMDb')
    endTime = time.time()  # time end create db
    print("Time it took to create a table: " + str(endTime - startTime) + " seconds")
except:
    print("table already exists")

# adding data
startTime = time.time()
with open("moviedata.json") as json_file:
    movies = json.load(json_file, parse_float=decimal.Decimal)
    for movie in movies:
        print("adding " + str(movie['title']) + " " + str(movie['year']))

        movieTitle = str(movie['title'])

        movieTitle = movieTitle.replace("/", "\u2215")
        movieTitle = movieTitle.replace("?", "\uFFFD")
        movieTitle = movieTitle.replace("\\", "\uFFFD")
        movieTitle = movieTitle.replace("#", "\uFFFD")

        # making every info element its own column
        info = json.loads(json.dumps(movie['info'], cls=DecimalEncoder))
        try:
            directors = info['directors']
        except:
            directors = "N/A"
        try:
            rating = info['rating']
        except:
            rating = "N/A"
        try:
            release_date = info['release_date']
        except:
            release_date = "N/A"
        try:
            genres = info['genres']
        except:
            genres = "N/A"
        try:
            image_url = info['image_url']
        except:
            image_url = "N/A"
        try:
            plot = info['plot']
        except:
            plot = "N/A"
        try:
            rank = info['rank']
        except:
            rank = "N/A"
        try:
            running_time_secs = info['running_time_secs']
        except:
            running_time_secs = "N/A"
        try:
            actors = info['actors']
        except:
            actors = "N/A"

        mv = {
            'PartitionKey': str(int(movie['year'])),
            'RowKey': movieTitle,
            'info': str(movie['info']),
            'directors': str(directors),
            'rating': str(rating),
            'release_date': str(release_date),
            'genres': str(genres),
            'image_url': str(image_url),
            'plot': str(plot),
            'rank': str(rank),
            'running_time_secs': str(running_time_secs),
            'actors': str(actors)
        }
        table_service.insert_entity("IMDb", mv)
endTime = time.time()
print("Time it took to import data: " + str(endTime - startTime) + " seconds")