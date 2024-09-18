from flask import Flask
from routes import app as routes_blueprint
from config import Config
from flask_cors import CORS
app = Flask(__name__)
app.config.from_object(Config)
app.config['SECRET_KEY'] = 'the quick brown fox jumps over the lazy   dog'
app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS(app, resources={r"/foo": {"origins": "*"}})

#db.init_app(app)

app.register_blueprint(routes_blueprint)



if __name__ == '__main__':
    app.run()
