from flask import jsonify


class ResponseUtil:
    @staticmethod
    def response(message, data=None, errors=None, http_code=200):
        response = jsonify({
            'message': message,
            'data': data,
            'errors': errors
        })
        return response, http_code

    @staticmethod
    def success(data=None):
        return ResponseUtil.response('success', data, None)

    @staticmethod
    def error(errors=None):
        return ResponseUtil.response('error', None, errors)
