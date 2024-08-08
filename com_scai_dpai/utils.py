import gzip
import json
import logging
from django.http import HttpResponse
import msgspec
from rest_framework.response import Response
import jwt
from datetime import datetime
logger = logging.getLogger(__name__)

class Util:
    @staticmethod
    def validateRequestParams(self, request):

        tenant_id = request.query_params.get("tenant_id")
        bu_id = request.query_params.get('bu_id')

        if bu_id == "" or bu_id is None or tenant_id == "" or tenant_id is None:
            return False
        return True

    @staticmethod
    def getLoggedInUserEmailAddress(self, token):
        decoded_token = jwt.decode(token, options={"verify_signature": False})
        return decoded_token['email']

    @staticmethod
    def getLoggedInUserId(token):
        decoded_token = jwt.decode(token, options={"verify_signature": False})
        return decoded_token['user_id']

    @staticmethod
    def createSuccessResponse(self, data, code, message, status):
        return Response({'responseData': {'data': data, 'responseCode': code, 'responseMessage': message}}, status)

    @staticmethod
    def createFailureResponse(self, code, message, status):
        return Response({'responseData': {'responseCode': code, 'responseMessage': message}}, status)
    
    @staticmethod
    def createSuccessGZipResponse(data, code, message, status):
        response = {'responseData': {'data': data, 'responseCode': code, 'responseMessage': message}}
        gzipResponse = msgspec.json.encode(response)
        gzipResponse = gzip.compress(gzipResponse, 1, mtime=0)
        response = HttpResponse(gzipResponse, status)
        response['Content-Encoding'] = 'gzip'
        response['Content-Length'] = str(len(gzipResponse))
        return response

    @staticmethod
    def createFailureGZipResponse(self, code, message, status):
        response = {'responseData': {'responseCode': code, 'responseMessage': message}}
        gzipResponse = gzip.compress(msgspec.json.encode(response))
        response = HttpResponse(gzipResponse, status)
        response['Content-Encoding'] = 'gzip'
        response['Content-Length'] = str(len(gzipResponse))
        return response