# coding: utf-8
#metemos tweepy para el listener
#metemos os para poder modificar el archivo json
import tweepy
import json
import os
from keys import CONSUMER_KEY,ACCESS_TOKEN,ACCESS_TOKEN_SECRET,CONSUMER_SECRET

#Obtenemos lo necesario de tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
#Obtenemos lo necesario de mongodb
from pymongo import MongoClient

'''
Aplicacion para obtener los tweets con el hashtag #meToo
'''

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET )
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)


# Base de datos local con una base con nombre Twitter_db_Costa_Rica
MONGO_HOST= 'mongodb://localhost/meToo'
client = MongoClient(MONGO_HOST)
db = client.meToo

#Por default, los json que devuelve twitter no tienen el formato de un json verdadero
#Es decir, hay que agregar al inicio y al final del .json, un "[" y un "]" así como separar
#cada tuit con una coma, exceptuando el 1er tuit y el tuit final.
#Por eso en el código siguiente se hace todo lo que se hace.

#Creamos la clase listener
class StdOutListener(StreamListener):
    def on_status(self, data):
        tweet = self.process_tweet(data)
        #si el texto del tweet no está vacío
        if tweet['tweet']:
            # Solo agrega el tweet a la base
			print ('Se agrego tweet de #meToo a la DB')
			db.meToo.insert(tweet['tweet']._json)
        else:
            print ('No hay mensaje en el Tweet')
        return True

    #primero revisamos si hay error. Si lo hay, no se ejecuta on_data
    def on_error(self, status):
        if status == 400:
            print ('Error 400: Peticion invalida')
        if status == 401:
            print ('Error 401: Error en la autenticacion')
        if status == 404:
            print ('Error 404: Revisar a donde se hace la peticion')
        if status == 406:
            print ('Error 406: Error en el formato de las peticiones')
        if status == 420:
            print ('Error 420: Demasiadas peticiones!')
        return False


    def process_tweet(self, tweet):
        if tweet.id:
            identificador = tweet.id
        else:
            identificador = ""
        if tweet.place.country_code:
            lugar = tweet.place.country_code
        else:
            lugar = ""
        return {'tweet': tweet, 'identificador': identificador, 'lugar': lugar}

#declaramos L como nuestro listener
l = StdOutListener()
#declaramos el stream. le damos la autorización y L
stream = Stream(auth, l)


# Solo extrae tweets que contengan la cadena meToo

for i in range(500):
    try:
        stream.filter(track='meToo')
    except:
        print ('Error {}'.format(i))
        continue
