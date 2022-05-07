#!/bin/bash


HD_APP_NAME=bikeshare-tor-dash
HOST=0.0.0.0

git init
git add .
git commit -m 'Add dashboard files'

heroku create $(HD_APP_NAME)
heroku git:remote -a $(HD_APP_NAME)
heroku config:set HOST=$(HOST) --app $(HD_APP_NAME)

git push heroku master
heroku ps:scale web=1
