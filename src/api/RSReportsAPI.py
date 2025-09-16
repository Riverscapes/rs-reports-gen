import os
from typing import Dict
import webbrowser
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode, urlparse, urlunparse
import threading
import hashlib
import base64
import logging

# We want to make inquirer optional so that we can use this module in other contexts
try:
    import inquirer
except ImportError:
    inquirer = None

import requests
from rsxml import Logger

# Disable all the weird terminal noise from urllib3
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").propagate = False

CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~'
LOCAL_PORT = 4721
ALT_PORT = 4723
LOGIN_SCOPE = 'openid'

AUTH_DETAILS = {
    "domain": "auth.riverscapes.net",
    "clientId": "Vhse6GZoU6vlJ9fcbrdmAAK6b4J9sjtT"
}


class RSReportsAPIException(Exception):
    """Exception raised for errors in the RSReportsAPI.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="RSReportsAPI encountered an error"):
        self.message = message
        super().__init__(self.message)


class RSReportsAPI:
    """This class is a wrapper around the Riverscapes API. It handles authentication and provides a
    simple interface for making queries.

    If you specify a secretId and clientId then this class will use machine authentication. This is
    appropriate for development and administration tasks. Otherwise it will use a browser-based
    authentication workflow which is appropriate for end-users.
    """

    def __init__(self, stage: str = None, api_token: str = None, dev_headers: Dict[str, str] = None):
        self.log = Logger('API')
        self.stage = stage.upper() if stage else self._get_stage_interactive()

        self.api_token = api_token
        if self.api_token and len(self.api_token) > 0:
            masked_token = self.api_token[:4] + "..." + self.api_token[-4:]
            self.log.warning(f"Using API token: {masked_token}. This is appropriate for development and administration tasks only.")
        self.dev_headers = dev_headers
        self.access_token = None
        self.token_timeout = None

        # If the RSAPI_ALTPORT environment variable is set then we use an alternative port for authentication
        # This is useful for keeping a local environment unblocked while also using this code inside a codespace
        self.auth_port = LOCAL_PORT if not os.environ.get('RSREPORTSAPI_ALTPORT') else ALT_PORT

        if self.stage.upper() == 'PRODUCTION':
            self.uri = 'https://api.reports.riverscapes.net'
        elif self.stage.upper() == 'STAGING':
            self.uri = 'https://api.reports.riverscapes.net/staging'
        elif self.stage.upper() == 'LOCAL':
            self.uri = 'http://127.0.0.1:7016'
        else:
            raise RSReportsAPIException(f'Unknown stage: {stage}')

    def _get_stage_interactive(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        if not inquirer:
            raise RSReportsAPIException("Inquirer is not installed so interactive stage choosing is not possible. Either install inquirer or specify the stage in the constructor.")

        questions = [
            inquirer.List('stage', message="Which Data Exchange stage?", choices=['production', 'staging'], default='production'),
        ]
        answers = inquirer.prompt(questions)
        return answers['stage'].upper()

    def __enter__(self) -> 'RSReportsAPI':
        """ Allows us to use this class as a context manager
        """
        self.refresh_token()
        return self

    def __exit__(self, _type, _value, _traceback):
        """Behaviour on close when using the "with RSReportsAPI():" Syntax
        """
        # Make sure to shut down the token poll event so the process can exit normally
        self.shutdown()

    def _generate_challenge(self, code: str) -> str:
        return self._base64_url(hashlib.sha256(code.encode('utf-8')).digest())

    def _generate_state(self, length: int) -> str:
        result = ''
        i = length
        chars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        while i > 0:
            result += chars[int(round(os.urandom(1)[0] * (len(chars) - 1)))]
            i -= 1
        return result

    def _base64_url(self, string: bytes) -> str:
        """ Convert a string to a base64url string

        Args:
            string (bytes): this is the string to convert

        Returns:
            str: the base64url string
        """
        return base64.urlsafe_b64encode(string).decode('utf-8').replace('=', '').replace('+', '-').replace('/', '_')

    def _generate_random(self, size: int) -> str:
        """ Generate a random string of a given size

        Args:
            size (int): the size of the string to generate

        Returns:
            str: the random string
        """
        buffer = os.urandom(size)
        state = []
        for b in buffer:
            index = b % len(CHARSET)
            state.append(CHARSET[index])
        return ''.join(state)

    def shutdown(self):
        """_summary_
        """
        self.log.debug("Shutting down Riverscapes API")
        if self.token_timeout:
            self.token_timeout.cancel()

    def refresh_token(self, force: bool = False):
        """_summary_

        Raises:
            error: _description_

        Returns:
            _type_: _description_
        """
        self.log.info(f"Authenticating on Riverscapes API: {self.uri}")
        if self.token_timeout:
            self.token_timeout.cancel()

        # On development there's no reason to actually go get a token
        if self.dev_headers and len(self.dev_headers) > 0:
            return self

        if self.access_token and not force:
            self.log.debug("   Token already exists. Not refreshing.")
            return self

        # Step 1: Determine if we're machine code or user auth
        # If it's machine then we can fetch tokens much easier:
        if self.api_token:
            # API Token doesn't need any more
            pass

        # If this is a user workflow then we need to pop open a web browser
        else:
            code_verifier = self._generate_random(128)
            code_challenge = self._generate_challenge(code_verifier)
            state = self._generate_random(32)
            redirect_url = f"http://localhost:{self.auth_port}/rscli/"
            login_url = urlparse(f"https://{AUTH_DETAILS['domain']}/authorize")
            query_params = {
                "client_id": AUTH_DETAILS["clientId"],
                "response_type": "code",
                "scope": LOGIN_SCOPE,
                "state": state,
                "audience": "https://api.riverscapes.net",
                "redirect_uri": redirect_url,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
            login_url = login_url._replace(query=urlencode(query_params))
            webbrowser.open_new_tab(urlunparse(login_url))

            auth_code = self._wait_for_auth_code()
            authentication_url = f"https://{AUTH_DETAILS['domain']}/oauth/token"

            data = {
                "grant_type": "authorization_code",
                "client_id": AUTH_DETAILS["clientId"],
                "code_verifier": code_verifier,
                "code": auth_code,
                "redirect_uri": redirect_url,
            }

            response = requests.post(authentication_url, headers={"content-type": "application/x-www-form-urlencoded"}, data=data, timeout=30)
            response.raise_for_status()
            res = response.json()
            self.token_timeout = threading.Timer(
                res["expires_in"] - 20, self.refresh_token)
            self.token_timeout.start()
            self.access_token = res["access_token"]
            self.log.info("SUCCESSFUL Browser Authentication")

    def _wait_for_auth_code(self):
        """ Wait for the auth code to come back from the server using a simple HTTP server

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        class AuthHandler(BaseHTTPRequestHandler):
            """_summary_

            Args:
                BaseHTTPRequestHandler (_type_): _description_
            """

            def stop(self):
                """Stop the server
                """
                self.server.shutdown()

            def do_GET(self):
                """ Do all the server stuff here
                """
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()

                url = "https://data.riverscapes.net/login_success?code=JSJJSDASDOAWIDJAW888dqwdqw88"

                success_html_body = f"""
                    <html>
                        <head>
                            <title>GraphQL API: Authentication successful</title>
                            <script>
                                window.onload = function() {{
                                    window.location.replace('{url}');
                                }}
                            </script>
                        </head>
                        <body>
                            <p>GraphQL API: Authentication successful. Redirecting....</p>
                        </body>
                    </html>
                """

                self.wfile.write(success_html_body.encode('utf-8'))

                query = urlparse(self.path).query
                if "=" in query and "code" in query:
                    self.server.auth_code = dict(x.split("=")
                                                 for x in query.split("&"))["code"]
                    # Now shut down the server and return
                    self.stop()

        server = ThreadingHTTPServer(("localhost", self.auth_port), AuthHandler)
        # Keep the server running until it is manually stopped
        try:
            print("Starting server to wait for auth, use <Ctrl-C> to stop")
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        if not hasattr(server, "auth_code"):
            raise RSReportsAPIException("Authentication failed")
        else:
            auth_code = server.auth_code if hasattr(server, "auth_code") else None
        return auth_code

    def load_query(self, query_name: str) -> str:
        """ Load a query file from the file system.

        Args:
            queryName (str): _description_

        Returns:
            str: _description_
        """
        qry_path = os.path.abspath(os.path.join(os.path.dirname(__file__),  'graphql', 'queries', f'{query_name}.gql'))
        with open(qry_path, 'r', encoding='utf-8') as queryFile:
            return queryFile.read()

    def load_mutation(self, mutation_name: str) -> str:
        """ Load a mutation file from the file system.

        Args:
            mutationName (str): _description_

        Returns:
            str: _description_
        """
        qry_path = os.path.abspath(os.path.join(os.path.dirname(__file__),  'graphql', 'mutations', f'{mutation_name}.gql'))
        with open(qry_path, 'r', encoding='utf-8') as queryFile:
            return queryFile.read()

    def run_query(self, query, variables):
        """ A simple function to use requests.post to make the API call. Note the json= section.

        Args:
            query (_type_): _description_
            variables (_type_): _description_

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        headers = {"authorization": "Bearer " + self.access_token} if self.access_token else {"x-api-key": self.api_token}
        request = requests.post(self.uri, json={
            'query': query,
            'variables': variables
        }, headers=headers, timeout=30)

        if request.status_code == 200:
            resp_json = request.json()
            if 'errors' in resp_json and len(resp_json['errors']) > 0:
                # Authentication timeout: re-login and retry the query
                if len(list(filter(lambda err: 'You must be authenticated' in err['message'], resp_json['errors']))) > 0:
                    self.log.debug("Authentication timed out. Fetching new token...")
                    self.refresh_token()
                    self.log.debug("   done. Re-trying query...")
                    return self.run_query(query, variables)

            else:
                # self.last_pass = True
                # self.retry = 0
                return request.json()
        else:
            raise RSReportsAPIException(f"Query failed to run by returning code of {request.status_code}. {query} {json.dumps(variables)}")


if __name__ == '__main__':
    log = Logger('API')
    gql = RSReportsAPI(os.environ.get('RS_API_URL'))
    gql.refresh_token()
    log.debug(gql.access_token)
    gql.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive

    gql2 = RSReportsAPI(os.environ.get('RS_API_URL'), {
        'clientId': os.environ['RS_CLIENT_ID'],
        'secretId': os.environ['RS_CLIENT_SECRET']
    })
    gql2.refresh_token()
    log.debug(gql2.access_token)
    gql2.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive
