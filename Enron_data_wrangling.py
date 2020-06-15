import numpy as np # linear algebra
import pandas as pd 
import re

pd.options.mode.chained_assignment = None

def final_work(data):
    x = len(data.index)
    headers = ['Message-ID: ', 'Date: ', 'From: ', 'To: ', 'Subject: ']
    for i, v in enumerate(headers):
        data = standard_format(data, data.message, v, i)
    data = data.reset_index()
    print("Got rid of {} useless emails! That's {}% of the total number of messages in this dataset.".format(x - len(data.index), np.round(((x - len(data.index)) / x) * 100, decimals=2)))
    data['text'] = get_text(data.message, 15)
    data['date'] = get_row(data.message, 1)
    data['senders'] = get_row(data.message, 2)
    data['recipients'] = get_row(data.message, 3)
    data['subject'] = get_row(data.message, 4)
    data.date = data.date.str.replace('Date: ', '')
    data.date = pd.to_datetime(data.date)
    data.subject = data.subject.str.replace('Subject: ', '')
    #data['recipient1'], data['recipient2'], data['recipient3'] = get_address(data, data.recipients, num_cols=3)
    data['recipient1'] = get_address(data, data.recipients, num_cols=3)
    data['sender'] = get_address(data, data.senders, num_cols=1)
    del data['recipients']
    del data['senders']
    del data['file']
    del data['message']
    #data = data[['date', 'sender', 'recipient1', 'recipient2', 'recipient3', 'subject', 'text']]
    data = data[['date', 'sender', 'recipient1', 'subject', 'text']]
    print(data.head())
    return data

def get_text(Series, row_num_slicer):
    """returns a Series with text sliced from a list split from each message. Row_num_slicer
    tells function where to slice split text to find only the body of the message."""
    result = pd.Series(index=Series.index)
    for row, message in enumerate(Series):
        message_words = message.split('\n')
        del message_words[:row_num_slicer]
        result.iloc[row] = message_words
    return result

def get_row(Series, row_num):
    """returns a single row split out from each message. Row_num is the index of the specific
    row that you want the function to return."""
    result = pd.Series(index=Series.index)
    for row, message in enumerate(Series):
        message_words = message.split('\n')
        message_words = message_words[row_num]
        result.iloc[row] = message_words
    return result

def get_address(df, Series, num_cols):
    """returns a specified email address from each row in a Series"""
    address = re.compile('[\w\.-]+@[\w\.-]+\.\w+')
    addresses = []
    result1 = pd.Series(index=df.index)
    i=0
    #result2 = pd.Series(index=df.index)
    #result3 = pd.Series(index=df.index)
   # for i in range(len(df)):
    for message in Series:
        correspondents = re.findall(address, message)
        if(len(correspondents)<1):
            correspondents.append('test@test.com')
        addresses.append(correspondents)
        result1[i] = addresses[i][0]
        i=i+1
        #if num_cols >= 2:
        #    if len(addresses[i]) >= 3:
        #       result2[i] = addresses[i][1]
        #        if num_cols == 3:
        #            if len(addresses[i]) >= 4:
        #                result3[i] = addresses[i][2]
    return result1 #, result2, result3

def standard_format(df, Series, string, slicer):
    """Drops rows containing messages without some specified value in the expected locations. 
    Returns original dataframe without these values. Don't forget to reindex after doing this!!!"""
    rows = []
    for row, message in enumerate(Series):
        message_words = message.split('\n')
        if string not in message_words[slicer]:
            rows.append(row)
    df = df.drop(df.index[rows])
    return df

data=pd.DataFrame()
i=0
#total 51 chunks
   
#chunks = pd.read_csv('C:\\Dev\\Capstone proj\\Data collection\\Enron_emails\\emails.csv', chunksize=10000)
chunks = pd.read_csv('C:\\Dev\\Capstone proj\\Data collection\\Enron_emails\\emails.csv',chunksize=100000)


for chunk in chunks:
    print('chunk number',i)   
    data=chunk       
    #data.info()
    print(data.first_valid_index())
    final_work(data).to_csv('C:\\Dev\\Capstone proj\\Data collection\\enron_outcome\\emaildata_100000_{}.csv'.format(i))
    i=i+1

print('process complete')