from flask import Flask, request, make_response
from downloader import url
from flask_httpauth import HTTPBasicAuth
from . import user


app = Flask(__name__)
auth = HTTPBasicAuth()


@auth.verify_password
def verify_password(username, password):
    return user.verify_password(username, password)


@app.route('/url/add')
@auth.login_required
def url_add():
    try:
        url.add(
            verify_username=auth.username(),
            **dict(request.args)
        )
        return {'ok': True}
    except Exception as e:
        return {'ok': False, 'error': str(e)}, 500


@app.route('/url/edit')
@auth.login_required
def url_edit():
    try:
        url.edit(
            verify_username=auth.username(),
            **dict(request.args)
        )
        return {'ok': True}
    except Exception as e:
        return {'ok': False, 'error': str(e)}, 500


@app.route('/url/list')
@auth.login_required
def url_list():
    try:
        return {'ok': True, 'urls': list(url.list_urls(
            verify_username=auth.username(),
            **dict(request.args)
        ))}
    except Exception as e:
        return {'ok': False, 'error': str(e)}, 500
