import os

class Config:
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost/test')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
