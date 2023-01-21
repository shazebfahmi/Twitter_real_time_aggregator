import twittercredentials
from tweepy import StreamListener, OAuthHandler, Stream, API
import json
import socket
from dotenv import load_dotenv
import os

load_dotenv('.env')
c = 0
class StreamClassUnderTwitter(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket
    def on_data(self,raw_data):
        d = dict()
        try:
            print(raw_data)
            data = json.loads(raw_data)

            print( "Data recieved is : ", data, end='\n' )

            d['timestamp_ms'] = data['timestamp_ms']
            if 'extended_entities' in data.keys():
                if 'media' in data['extended_entities']:
                    global c
                    for media_list in data['extended_entities']['media']:
                        if media_list['type'] == 'photo':
                            d['photo_link'] = media_list['media_url_https']
                            print('Photo link found : ', d['photo_link'] , end='\n' )
                            self.client_socket.send((str(d)+"\n").encode('utf-8'))
                            c = c + 1
                        elif media_list['type'] == 'video':
                            d['video_link'] = media_list['video_info']['variants'][0]['url']
                            print('Video link found : ', d['video_link'] , end='\n' )#
                            self.client_socket.send((str(d)+"\n").encode('utf-8'))
                            c = c + 1
        except BaseException as e:
            print("Error in on_data: %s" % str(e))
            return True
    def on_error(self, status):
        print(dir(self),status)
        return True

#Initializing the port and host
host = 'localhost'
port = 5599
address = (host, port)

#Initializing the socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(address)
server_socket.listen(2)
server_socket.settimeout(120)

keyword = str(input('Enter the Keyword you want to search on Twitter : '))
print("Listening for client...")
conn, address = server_socket.accept()
print("Connected to Client at " + str(address))

#Authentication
auth = OAuthHandler(os.getenv('CONSUMER_KEY'), os.getenv('CONSUMER_SECRET'))
auth.set_access_token(os.getenv('ACCESS_TOKEN'), os.getenv('ACCESS_TOKEN_SECRET'))

#Establishing the twitter stream
twitter_stream = Stream(auth, StreamClassUnderTwitter(conn), tweet_mode="extended_tweet")
twitter_stream.filter(track=[keyword],languages = ['en'] )
