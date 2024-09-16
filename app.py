from flask import Flask
from routes import app as routes_blueprint
from config import Config
app = Flask(__name__)
app.config.from_object(Config)

#db.init_app(app)

app.register_blueprint(routes_blueprint)



if __name__ == '__main__':
    app.run()
