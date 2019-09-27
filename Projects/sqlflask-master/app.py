import flask
import flask_jwt
from flask_jwt import JWT
from flask import Flask
from flask_restful import Resource, Api

from security import authenticate, identity
from resources.user import UserRegister
from resources.item import Item, ItemList
from resources.store import Store, StoreList

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = 'charles'
api = Api(app)

jwt = JWT(app, authenticate, identity) # /auth


api.add_resource(Item, '/item/<string:name>')
api.add_resource(ItemList, '/items')
api.add_resource(UserRegister, '/register')
api.add_resource(Store, '/store/<string:name>')
api.add_resource(StoreList, '/stores')


if __name__ == '__main__':
    import db
    from db import db
    db.init_app(app)
    app.run(port=5000, debug=True)
