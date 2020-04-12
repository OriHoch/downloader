from werkzeug.security import generate_password_hash, check_password_hash
from . import db
from collections import OrderedDict


MIN_USERNAME_LENGTH = 3
MIN_PASSWORD_LENGTH = 8


def verify_password(username, password):
    row = db.only_one('select password from "user" where name=%s', (username,))
    if row:
        return check_password_hash(row['password'], password)
    else:
        return False


def verify_user_app(verify_username, app_id):
    if verify_username:
        row = db.only_one('select id from "user" where name=%s', (verify_username,))
        if not row:
            raise PermissionError('invalid user')
        else:
            user_id = row['id']
            if not db.only_one('select 1 from superuser where user_id=%s', (user_id,)):
                if not db.only_one('select 1 from app_user where user_id=%s and app_user.app_id=%s', (user_id, app_id)):
                    raise PermissionError('user does not have permission for app')


def create(username, password):
    if len(username) < MIN_USERNAME_LENGTH:
        raise Exception('username is too short, minimum %s characters' % MIN_USERNAME_LENGTH)
    elif len(password) < MIN_PASSWORD_LENGTH:
        raise Exception('password is too short, minimum %s characters' % MIN_PASSWORD_LENGTH)
    else:
        try:
            db.execute('insert into "user" (name, password) values (%s, %s)', (username, generate_password_hash(password)))
        except db.UniqueViolation:
            raise Exception("username already exists")


def change_password(username, password):
    if len(password) < MIN_PASSWORD_LENGTH:
        raise Exception('password is too short, minimum %s characters' % MIN_PASSWORD_LENGTH)
    else:
        db.execute('update "user" set password=%s where name=%s', (generate_password_hash(password), username))


def list_users():
    for row in db.rows_iterator("""
        select "user".id user_id, "user".name user_name, superuser.id superuser_id
        from "user"
        left join superuser on superuser.user_id = "user".id
        order by "user".name
    """):
        app_names = [app['name'] for app in db.rows_iterator("""
            select app.name
            from app_user, app
            where app_user.app_id = app.id
            and app_user.user_id = %s
        """, (row['user_id'],))]
        yield OrderedDict(
            name=row['user_name'],
            is_superuser=bool(row['superuser_id']),
            apps=app_names
        )


def set_superuser(username, remove=False):
    user_id = db.only_one('select id from "user" where name=%s', (username,))['id']
    if remove:
        db.execute("delete from superuser where user_id=%s", (user_id,))
    else:
        db.execute("insert into superuser (user_id) values (%s)", (user_id,))


def allow_app(username, app_name, remove=False):
    user_id = db.only_one('select id from "user" where name=%s', (username,))['id']
    app_id = db.only_one('select id from app where name=%s', (app_name,))['id']
    if remove:
        db.execute("delete from app_user where app_id=%s and user_id=%s", (app_id, user_id))
    else:
        db.execute("insert into app_user (app_id, user_id) values (%s, %s)", (app_id, user_id))
