from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import time
import json
import decimal
import os

cs = os.getenv('AZURE_DB_CONNECTION_STRING')
table_service = TableService(connection_string=cs)

userInput = ''

while userInput != "exit":
    primaryRange1 = 0
    primaryRange2 = 0
    secondaryRange1 = ''
    secondaryRange2 = ''
    rating1 = 0
    rating2 = 9.9
    sortParameter = ''
    showing = 0
    addRating = False
    addSecondaryRange = False

    # primary key input
    userInput = str(input(
        'Primary/Partition Key [Individual/Range](hit enter for all years)(type [exit] to quit): '))
    print("you entered " + userInput)

    if userInput.lower() == "individual":
        userInput = str(input('Input individual value: '))
        try:
            primaryRange1 = int(userInput)
        except:
            print("invalid input")
            continue
        primaryRange2 = primaryRange1
    elif userInput.lower() == "range":
        userInput = str(input('Input first range value: '))
        try:
            primaryRange1 = int(userInput)
        except:
            print("invalid input")
            continue
        userInput = str(input('Input second range value: '))
        try:
            primaryRange2 = int(userInput)
        except:
            print("invalid input")
            continue
    elif userInput == '':
        primaryRange2 = 3000
    elif userInput == 'exit':
        print("exiting")
        break
    else:
        print("invalid input")
        continue

    # secondary key input
    userInput = str(
        input('Secondary/Sort Key Type [range] for range or hit [enter] for all: '))
    print("you entered " + userInput)

    if userInput.lower() == "range":
        addSecondaryRange = True
        secondaryRange1 = str(input('Input first value (case sensitive): '))
        secondaryRange2 = str(input('Input second value (case sensitive): '))
    elif userInput == '':
        secondaryRange1 = 'AAAAAAAAAAAAAA'
        secondaryRange2 = 'zzzzzzzzzzzz'
    else:
        print('unknown command, restarting')
        continue

    # rating filter
    userInput = str(
        input("Rating filter? [y/n] (this is the only filter I'm implementing): "))
    if userInput.lower() == "y":
        addRating = True
        userInput = str(
            input("[1] greater than\n[2] less than\n[3] between: "))
        if userInput == "1":
            rating1 = str(
                input("Enter the rating that the movies should be greater than: "))
            rating2 = 9.9
        elif userInput == "2":
            rating2 = str(
                input("Enter the rating that the movies should be less than: "))
            rating1 = 0
            if rating2 == "10":
                rating2 = "9.9"
        elif userInput == "3":
            rating1 = str(
                input("Enter the rating that the movies should be greater than: "))
            rating2 = str(
                input("Enter the rating that the movies should be less than: "))
            if rating2 == "10":
                rating2 = "9.9"
        else:
            print('unknown command, restarting')
            continue
    elif userInput.lower() != "n":
        print("wrong input, restarting")
        continue

    # sort by
    userInput = str(input("sort by [year/title/rating]: "))
    if userInput.lower() == "year":
        sortParameter = "year"
    elif userInput.lower() == "title":
        sortParameter = "title"
    elif userInput.lower() == "rating":
        sortParameter = "rating"
    else:
        print("wrong input, restarting")
        continue

    # what columns to show
    userInput = str(input(
        "[1] show year , title\n[2] show year,title,rating\n[3]show year,title,rating,genre "))
    if userInput == "1":
        showing = 1
    elif userInput == "2":
        showing = 2
    elif userInput == "3":
        showing = 3
    else:
        print("wrong input, restarting")
        continue

    startTime = time.time()
    filterQuery = "PartitionKey ge '" + \
        str(primaryRange1) + "' and PartitionKey le '" + \
        str(primaryRange2) + "'"
    if addSecondaryRange == True:
        filterQuery += " and RowKey ge '" + \
            str(secondaryRange1) + "' and RowKey le '" + \
            str(secondaryRange2) + "'"
    if addRating == True:
        filterQuery += " and rating ge '" + \
            str(rating1) + "' and rating le '" + str(rating2) + "'"

    tasks = table_service.query_entities('IMDb', filter=filterQuery)
    endTime = time.time()

    if sortParameter == "rating":
        myList = sorted(tasks, key=lambda i: i['rating'])
    elif sortParameter == "title":
        myList = sorted(tasks, key=lambda i: i['RowKey'])
    else:
        myList = sorted(tasks, key=lambda i: i['PartitionKey'])

    if len(myList) == 0:
        print("no results found with query: " + str(filterQuery))
    
    csvString = ""
    for task in myList:
        #print(f'{task.RowKey:25} : {task.PartitionKey:5} : {task.rating}')
        if showing > 0:
            csvString += str(task.PartitionKey) + "," + str(task.RowKey)
            print(f'{task.PartitionKey:6} : {task.RowKey:30}', end='')  # need python 3.6 or above
        if showing > 1:
            csvString += "," + str(task.rating)
            print(f' : {task.rating:5}', end='')
        if showing > 2:
            csvString += "," + str(task.genres)
            print(f' : {task.genres:20}', end='')
        csvString += "\n"
        print("")

    userInput = str(input("Save this table to csv? [y/n]"))
    if userInput == "y":
        f = open("data.csv", 'w')
        f.write(csvString)
        f.close()
        print("exported as data.csv")
    print("Query time: " + str(endTime - startTime) + " seconds")
    
