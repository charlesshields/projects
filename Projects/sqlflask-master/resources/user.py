import sqlite3
import flask_restful
from flask_restful import Resource, reqparse
from models.user import UserModel

class UserRegister(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('username',
        type=str,
        required=True,
        help = "This field cannot be left blank!"
    )
    parser.add_argument('password',
        type=str,
        required=True,
        help= "This field cannot be left blank!"
    )
    
    def post(self):
        data = UserRegister.parser.parse_args()

        if UserModel.find_by_username(data['username']): #essentially means is not None
            return {"message": "A user with that username already exists"}, 400

        user = UserModel(data['username'], data['password'])
        user.save_to_db()

        return {"message": "User created succesfully."}, 201
