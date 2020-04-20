#
#  Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  This file is licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License. A copy of
#  the License is located at
#
#  http://aws.amazon.com/apache2.0/
#
#  This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
#  CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
#
from __future__ import print_function  # Python 2/3 compatibility
import boto3
import json
import time
import decimal
import operator
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

# Helper class to convert a DynamoDB item to JSON.


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('Movies')

userInput = ''


while userInput != "exit":
    primaryRange1 = 0
    primaryRange2 = 0
    secondaryRange1 = ''
    secondaryRange2 = ''
    rating1 = 0
    rating2 = 10
    sortParameter = ''
    showing = 0

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
        userInput = str(
            input("[1] greater than\n[2] less than\n[3] between: "))
        if userInput == "1":
            rating1 = str(
                input("Enter the rating that the movies should be greater than: "))
            rating2 = 10
        elif userInput == "2":
            rating2 = str(
                input("Enter the rating that the movies should be less than: "))
            rating1 = 0
        elif userInput == "3":
            rating1 = str(
                input("Enter the rating that the movies should be greater than: "))
            rating2 = str(
                input("Enter the rating that the movies should be less than: "))
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

    fe = Key('year').between(primaryRange1, primaryRange2) & Key('title').between(str(secondaryRange1), str(
        secondaryRange2)) & Key('info.rating').between(Decimal(rating1), Decimal(rating2))
    pe = "#yr, title, info.rating, info.genres"
    # Expression Attribute Names for Projection Expression only.
    ean = {"#yr": "year", }
    esk = None

    response = table.scan(
        FilterExpression=fe,
        ProjectionExpression=pe,
        ExpressionAttributeNames=ean
    )

    myList = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression=pe,
            FilterExpression=fe,
            ExpressionAttributeNames=ean,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        myList += response['Items']

    endTime = time.time()

    if sortParameter == "rating":
        myList = sorted(myList, key=lambda i: i['info']['rating'])
    elif sortParameter == "title":
        myList = sorted(myList, key=lambda i: i['title'])
    else:
        myList = sorted(myList, key=lambda i: i['year'])

    csvString = ""
    for i in myList:
        try:
            info = json.loads(json.dumps(i['info'], cls=DecimalEncoder))
        except:
            c = "N/A"
            d = 'N/A'
        try:
            a = i['year']
        except:
            a = 0000
        try:
            b = i['title']
        except:
            b = "_______"
        try:
            c = info['rating']
        except:
            c = "N/A"
        try:
            d = str(info['genres'])
        except:
            d = "N/A"
        if showing > 0:
            csvString += str(a) + "," + str(b)
            print(f'{a:6} : {b:30}', end='')  # need python 3.6 or above
        if showing > 1:
            csvString += "," + str(c)
            print(f' : {c:5}', end='')
        if showing > 2:
            csvString += "," + str(d)
            print(f' : {d:20}', end='')
        csvString += "\n"
        print("")
        # print(i['year'], ":", i['title'], ":", info['rating'],":", info['genres']

    userInput = str(input("Save to csv? [y/n]"))
    if userInput == "y":
        f = open("data.csv", 'w')
        f.write(csvString)
        f.close()
        print("exported as data.csv")

    print("Query took : " + str(endTime - startTime) + " seconds to complete")