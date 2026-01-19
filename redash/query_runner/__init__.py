import logging
from collections import defaultdict
from contextlib import ExitStack
from functools import wraps

import sqlparse
from dateutil import parser
from rq.timeouts import JobTimeoutException
from sshtunnel import open_tunnel

from redash import settings, utils
from redash.utils.requests_session import (
    UnacceptableAddressException,
    requests_or_advocate,
    requests_session,
)

logger = logging.getLogger(__name__)

__all__ = [
    "BaseQueryRunner",
    "BaseHTTPQueryRunner",
    "InterruptException",
    "JobTimeoutException",
    "BaseSQLQueryRunner",
    "TYPE_DATETIME",
    "TYPE_BOOLEAN",
    "TYPE_INTEGER",
    "TYPE_STRING",
    "TYPE_DATE",
    "TYPE_FLOAT",
    "SUPPORTED_COLUMN_TYPES",
    "register",
    "get_query_runner",
    "import_query_runners",
    "guess_type",
]

# Valid types of columns returned in results:
TYPE_INTEGER = "integer"
TYPE_FLOAT = "float"
TYPE_BOOLEAN = "boolean"
TYPE_STRING = "string"
TYPE_DATETIME = "datetime"
TYPE_DATE = "date"

SUPPORTED_COLUMN_TYPES = set([TYPE_INTEGER, TYPE_FLOAT, TYPE_BOOLEAN, TYPE_STRING, TYPE_DATETIME, TYPE_DATE])


def split_sql_statements(query):
    def strip_trailing_comments(stmt):
        idx = len(stmt.tokens) - 1
        while idx >= 0:
            tok = stmt.tokens[idx]
            if tok.is_whitespace or sqlparse.utils.imt(tok, i=sqlparse.sql.Comment, t=sqlparse.tokens.Comment):
                stmt.tokens[idx] = sqlparse.sql.Token(sqlparse.tokens.Whitespace, " ")
            else:
                break
            idx -= 1
        return stmt

    def strip_trailing_semicolon(stmt):
        idx = len(stmt.tokens) - 1
        while idx >= 0:
            tok = stmt.tokens[idx]
            # we expect that trailing comments already are removed
            if not tok.is_whitespace:
                if sqlparse.utils.imt(tok, t=sqlparse.tokens.Punctuation) and tok.value == ";":
                    stmt.tokens[idx] = sqlparse.sql.Token(sqlparse.tokens.Whitespace, " ")
                break
            idx -= 1
        return stmt

    def is_empty_statement(stmt):
        # copy statement object. `copy.deepcopy` fails to do this, so just re-parse it
        st = sqlparse.engine.FilterStack()
        st.stmtprocess.append(sqlparse.filters.StripCommentsFilter())
        stmt = next(st.run(str(stmt)), None)
        if stmt is None:
            return True

        return str(stmt).strip() == ""

    stack = sqlparse.engine.FilterStack()

    result = [stmt for stmt in stack.run(query)]
    result = [strip_trailing_comments(stmt) for stmt in result]
    result = [strip_trailing_semicolon(stmt) for stmt in result]
    result = [str(stmt).strip() for stmt in result if not is_empty_statement(stmt)]

    if len(result) > 0:
        return result

    return [""]  # if all statements were empty - return a single empty statement


def combine_sql_statements(queries):
    return ";\n".join(queries)


def find_last_keyword_idx(parsed_query):
    for i in reversed(range(len(parsed_query.tokens))):
        if parsed_query.tokens[i].ttype in sqlparse.tokens.Keyword:
            return i
    return -1


class InterruptException(Exception):
    pass


class NotSupported(Exception):
    pass


class BaseQueryRunner:
    deprecated = False
    should_annotate_query = True
    noop_query = None
    limit_query = " LIMIT 1000"
    limit_keywords = ["LIMIT", "OFFSET"]
    limit_after_select = False

    def __init__(self, configuration):
        self.syntax = "sql"
        self.configuration = configuration

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def type(cls):
        return cls.__name__.lower()

    @classmethod
    def enabled(cls):
        return True

    @property
    def host(self):
        """Returns this query runner's configured host.
        This is used primarily for temporarily swapping endpoints when using SSH tunnels to connect to a data source.

        `BaseQueryRunner`'s naïve implementation supports query runner implementations that store endpoints using `host` and `port`
        configuration values. If your query runner uses a different schema (e.g. a web address), you should override this function.
        """
        if "host" in self.configuration:
            return self.configuration["host"]
        else:
            raise NotImplementedError()

    @host.setter
    def host(self, host):
        """Sets this query runner's configured host.
        This is used primarily for temporarily swapping endpoints when using SSH tunnels to connect to a data source.

        `BaseQueryRunner`'s naïve implementation supports query runner implementations that store endpoints using `host` and `port`
        configuration values. If your query runner uses a different schema (e.g. a web address), you should override this function.
        """
        if "host" in self.configuration:
            self.configuration["host"] = host
        else:
            raise NotImplementedError()

    @property
    def port(self):
        """Returns this query runner's configured port.
        This is used primarily for temporarily swapping endpoints when using SSH tunnels to connect to a data source.

        `BaseQueryRunner`'s naïve implementation supports query runner implementations that store endpoints using `host` and `port`
        configuration values. If your query runner uses a different schema (e.g. a web address), you should override this function.
        """
        if "port" in self.configuration:
            return self.configuration["port"]
        else:
            raise NotImplementedError()

    @port.setter
    def port(self, port):
        """Sets this query runner's configured port.
        This is used primarily for temporarily swapping endpoints when using SSH tunnels to connect to a data source.

        `BaseQueryRunner`'s naïve implementation supports query runner implementations that store endpoints using `host` and `port`
        configuration values. If your query runner uses a different schema (e.g. a web address), you should override this function.
        """
        if "port" in self.configuration:
            self.configuration["port"] = port
        else:
            raise NotImplementedError()

    @classmethod
    def configuration_schema(cls):
        return {}

    def annotate_query(self, query, metadata):
        if not self.should_annotate_query:
            return query

        annotation = ", ".join(["{}: {}".format(k, v) for k, v in metadata.items()])
        annotated_query = "/* {} */ {}".format(annotation, query)
        return annotated_query

    def test_connection(self):
        if self.noop_query is None:
            raise NotImplementedError()
        data, error = self.run_query(self.noop_query, None)

        if error is not None:
            raise Exception(error)

    def run_query(self, query, user):
        raise NotImplementedError()

    def fetch_columns(self, columns):
        column_names = set()
        duplicates_counters = defaultdict(int)
        new_columns = []

        for col in columns:
            column_name = col[0]
            while column_name in column_names:
                duplicates_counters[col[0]] += 1
                column_name = "{}{}".format(col[0], duplicates_counters[col[0]])

            column_names.add(column_name)
            new_columns.append({"name": column_name, "friendly_name": column_name, "type": col[1]})

        return new_columns

    def get_schema(self, get_stats=False):
        raise NotSupported()

    def _handle_run_query_error(self, error):
        if error is None:
            return

        logger.error(error)
        raise Exception(f"Error during query execution. Reason: {error}")

    def _run_query_internal(self, query):
        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed running query [%s]." % query)
        return results["rows"]

    @classmethod
    def to_dict(cls):
        return {
            "name": cls.name(),
            "type": cls.type(),
            "configuration_schema": cls.configuration_schema(),
            **({"deprecated": True} if cls.deprecated else {}),
        }

    @property
    def supports_auto_limit(self):
        return False

    def apply_auto_limit(self, query_text, should_apply_auto_limit):
        return query_text

    def gen_query_hash(self, query_text, set_auto_limit=False):
        query_text = self.apply_auto_limit(query_text, set_auto_limit)
        return utils.gen_query_hash(query_text)


class BaseSQLQueryRunner(BaseQueryRunner):
    def get_schema(self, get_stats=False):
        schema_dict = {}
        self._get_tables(schema_dict)
        if settings.SCHEMA_RUN_TABLE_SIZE_CALCULATIONS and get_stats:
            self._get_tables_stats(schema_dict)
        return list(schema_dict.values())

    def _get_tables(self, schema_dict):
        return []

    def _get_tables_stats(self, tables_dict):
        for t in tables_dict.keys():
            if isinstance(tables_dict[t], dict):
                res = self._run_query_internal("select count(*) as cnt from %s" % t)
                tables_dict[t]["size"] = res[0]["cnt"]

    @property
    def supports_auto_limit(self):
        return True

    def query_is_select_no_limit(self, query):
        parsed_query_list = sqlparse.parse(query)
        if len(parsed_query_list) == 0:
            return False
        parsed_query = parsed_query_list[0]
        last_keyword_idx = find_last_keyword_idx(parsed_query)
        # Either invalid query or query that is not select
        if last_keyword_idx == -1 or parsed_query.tokens[0].value.upper() != "SELECT":
            return False

        no_limit = parsed_query.tokens[last_keyword_idx].value.upper() not in self.limit_keywords

        return no_limit

    def add_limit_to_query(self, query):
        parsed_query = sqlparse.parse(query)[0]
        limit_tokens = sqlparse.parse(self.limit_query)[0].tokens
        length = len(parsed_query.tokens)
        if not self.limit_after_select:
            if parsed_query.tokens[length - 1].ttype == sqlparse.tokens.Punctuation:
                parsed_query.tokens[length - 1 : length - 1] = limit_tokens
            else:
                parsed_query.tokens += limit_tokens
        else:
            for i in range(length - 1, -1, -1):
                if parsed_query[i].value.upper() == "SELECT":
                    index = parsed_query.token_index(parsed_query[i + 1])
                    parsed_query = sqlparse.sql.Statement(
                        parsed_query.tokens[:index] + limit_tokens + parsed_query.tokens[index:]
                    )
                    break
        return str(parsed_query)

    def apply_auto_limit(self, query_text, should_apply_auto_limit):
        queries = split_sql_statements(query_text)
        if should_apply_auto_limit:
            # we only check for last one in the list because it is the one that we show result
            last_query = queries[-1]
            if self.query_is_select_no_limit(last_query):
                queries[-1] = self.add_limit_to_query(last_query)
        return combine_sql_statements(queries)


class BaseHTTPQueryRunner(BaseQueryRunner):
    should_annotate_query = False
    response_error = "Endpoint returned unexpected status code"
    requires_authentication = False
    requires_url = True
    url_title = "URL base path"
    username_title = "HTTP Basic Auth Username"
    password_title = "HTTP Basic Auth Password"

    @classmethod
    def configuration_schema(cls):
        schema = {
            "type": "object",
            "properties": {
                "url": {"type": "string", "title": cls.url_title},
                "username": {"type": "string", "title": cls.username_title},
                "password": {"type": "string", "title": cls.password_title},
            },
            "secret": ["password"],
            "order": ["url", "username", "password"],
        }

        if cls.requires_url or cls.requires_authentication:
            schema["required"] = []

        if cls.requires_url:
            schema["required"] += ["url"]

        if cls.requires_authentication:
            schema["required"] += ["username", "password"]
        return schema

    def get_auth(self):
        username = self.configuration.get("username")
        password = self.configuration.get("password")
        if username and password:
            return (username, password)
        if self.requires_authentication:
            raise ValueError("Username and Password required")
        else:
            return None

    def get_response(self, url, auth=None, http_method="get", **kwargs):
        # Get authentication values if not given
        if auth is None:
            auth = self.get_auth()

        # Then call requests to get the response from the given endpoint
        # URL optionally, with the additional requests parameters.
        error = None
        response = None
        try:
            response = requests_session.request(http_method, url, auth=auth, **kwargs)
            # Raise a requests HTTP exception with the appropriate reason
            # for 4xx and 5xx response status codes which is later caught
            # and passed back.
            response.raise_for_status()

            # Any other responses (e.g. 2xx and 3xx):
            if response.status_code != 200:
                error = "{} ({}).".format(self.response_error, response.status_code)

        except requests_or_advocate.HTTPError as exc:
            logger.exception(exc)
            error = "Failed to execute query. "
            f"Return Code: {response.status_code} Reason: {response.text}"
        except UnacceptableAddressException as exc:
            logger.exception(exc)
            error = "Can't query private addresses."
        except requests_or_advocate.RequestException as exc:
            # Catch all other requests exceptions and return the error.
            logger.exception(exc)
            error = str(exc)

        # Return response and error.
        return response, error


query_runners = {}


def register(query_runner_class):
    global query_runners
    if query_runner_class.enabled():
        logger.debug(
            "Registering %s (%s) query runner.",
            query_runner_class.name(),
            query_runner_class.type(),
        )
        query_runners[query_runner_class.type()] = query_runner_class
    else:
        logger.debug(
            "%s query runner enabled but not supported, not registering. Either disable or install missing "
            "dependencies.",
            query_runner_class.name(),
        )


def get_query_runner(query_runner_type, configuration):
    query_runner_class = query_runners.get(query_runner_type, None)
    if query_runner_class is None:
        return None

    return query_runner_class(configuration)


def get_configuration_schema_for_query_runner_type(query_runner_type):
    query_runner_class = query_runners.get(query_runner_type, None)
    if query_runner_class is None:
        return None

    return query_runner_class.configuration_schema()


def import_query_runners(query_runner_imports):
    for runner_import in query_runner_imports:
        __import__(runner_import)


def guess_type(value):
    if isinstance(value, bool):
        return TYPE_BOOLEAN
    elif isinstance(value, int):
        return TYPE_INTEGER
    elif isinstance(value, float):
        return TYPE_FLOAT

    return guess_type_from_string(value)


def guess_type_from_string(string_value):
    if string_value == "" or string_value is None:
        return TYPE_STRING

    try:
        int(string_value)
        return TYPE_INTEGER
    except (ValueError, OverflowError):
        pass

    try:
        float(string_value)
        return TYPE_FLOAT
    except (ValueError, OverflowError):
        pass

    if str(string_value).lower() in ("true", "false"):
        return TYPE_BOOLEAN

    try:
        parser.parse(string_value)
        return TYPE_DATETIME
    except (ValueError, OverflowError):
        pass

    return TYPE_STRING


def with_ssh_tunnel(query_runner, details):
    def tunnel(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # #region agent log - function entry
            try:
                import sys
                import json as json_entry
                import os as os_entry
                entry_log = {
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "M",
                    "location": "query_runner/__init__.py:with_ssh_tunnel:wrapper:entry",
                    "message": "SSH tunnel wrapper function called",
                    "data": {
                        "details_keys": list(details.keys()) if details else [],
                        "has_ssh_host": "ssh_host" in details if details else False,
                        "has_ssh_username": "ssh_username" in details if details else False,
                    },
                    "timestamp": int(__import__('time').time() * 1000)
                }
                sys.stderr.write("DEBUG SSH TUNNEL ENTRY: " + json_entry.dumps(entry_log) + "\n")
                sys.stderr.flush()
                try:
                    debug_log_path_entry = "/home/hok/redash/redash/.cursor/debug.log"
                    with open(debug_log_path_entry, 'a') as log_file_entry:
                        log_file_entry.write(json_entry.dumps(entry_log) + '\n')
                except: pass
            except: pass
            # #endregion
            
            # Define debug log path at the start of the function
            debug_log_path = "/home/hok/redash/redash/.cursor/debug.log"
            
            try:
                remote_host, remote_port = query_runner.host, query_runner.port
            except NotImplementedError:
                raise NotImplementedError("SSH tunneling is not implemented for this query runner yet.")

            stack = ExitStack()
            try:
                bastion_address = (details["ssh_host"], details.get("ssh_port", 22))
                remote_address = (remote_host, remote_port)
                auth = {
                    "ssh_username": details["ssh_username"],
                    **settings.dynamic_settings.ssh_tunnel_auth(),
                }
                
                # Handle private key from data source configuration
                # Priority: ssh_private_key_path > ssh_private_key > ssh_tunnel_auth()
                ssh_pkey = None
                
                # #region agent log
                try:
                    import json
                    import os
                    debug_log_path = os.path.join(os.path.dirname(__file__), '..', '..', '.cursor', 'debug.log')
                    with open(debug_log_path, 'a') as log_file:
                        log_file.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "F",
                            "location": "query_runner/__init__.py:with_ssh_tunnel:auth_setup",
                            "message": "SSH tunnel authentication setup",
                            "data": {
                                "has_ssh_private_key_path": "ssh_private_key_path" in details,
                                "has_ssh_private_key": "ssh_private_key" in details,
                                "has_ssh_passphrase": "ssh_passphrase" in details,
                                "has_ssh_password": "ssh_password" in details,
                                "ssh_private_key_path_value": details.get("ssh_private_key_path", "")[:50] if details.get("ssh_private_key_path") else None,
                                "ssh_private_key_length": len(details.get("ssh_private_key", "")) if details.get("ssh_private_key") else 0,
                                "details_keys": list(details.keys()),
                            },
                            "timestamp": int(__import__('time').time() * 1000)
                        }) + '\n')
                except: pass
                # #endregion
                
                # Check for private key path first (explicit file path)
                key_path = details.get("ssh_private_key_path") or details.get("ssh_private_key")
                
                if key_path:
                    import os
                    # Check if it's a file path (starts with / or ~) NOT key content
                    # IMPORTANT: Key content contains '/' characters, so we must check for -----BEGIN first
                    is_key_content = key_path.strip().startswith('-----BEGIN')
                    is_path = not is_key_content and (key_path.startswith('/') or key_path.startswith('~') or key_path.startswith('C:') or key_path.startswith('c:'))
                    
                    # #region agent log
                    try:
                        with open(debug_log_path, 'a') as log_file:
                            log_file.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": "run1",
                                "hypothesisId": "F",
                                "location": "query_runner/__init__.py:with_ssh_tunnel:check_key_path",
                                "message": "Checking private key path",
                                "data": {
                                    "key_path_first_50": key_path[:50] if key_path else None,
                                    "is_key_content": is_key_content,
                                    "is_path": is_path,
                                    "path_exists": os.path.exists(key_path) if is_path and key_path else False,
                                    "is_file": os.path.isfile(key_path) if is_path and key_path and os.path.exists(key_path) else False,
                                    "in_docker": os.path.exists('/.dockerenv'),
                                },
                                "timestamp": int(__import__('time').time() * 1000)
                            }) + '\n')
                    except: pass
                    # #endregion
                    
                    if is_path:
                        # Expand ~ to home directory
                        if key_path.startswith('~'):
                            key_path = os.path.expanduser(key_path)
                        
                        original_path = key_path
                        found_path = None
                        
                        # Check if file exists at the original path
                        if os.path.exists(key_path) and os.path.isfile(key_path):
                            found_path = key_path
                        else:
                            # If running in Docker and file not found, try WSL mount points
                            if os.path.exists('/.dockerenv'):
                                # Try common WSL mount points
                                wsl_mount_points = [
                                    '/mnt/wsl',  # WSL2 mount point
                                    '/run/desktop/mnt/host',  # Docker Desktop WSL2 integration
                                ]
                                
                                for mount_point in wsl_mount_points:
                                    if os.path.exists(mount_point):
                                        # Try to find the file relative to mount point
                                        # For /home/hok/.ssh/id_rsa, try /mnt/wsl/home/hok/.ssh/id_rsa
                                        test_path = os.path.join(mount_point, key_path.lstrip('/'))
                                        if os.path.exists(test_path) and os.path.isfile(test_path):
                                            found_path = test_path
                                            break
                                        
                                        # Also try without leading /home (in case mount is at /home)
                                        if key_path.startswith('/home/'):
                                            test_path2 = os.path.join(mount_point, key_path[6:])  # Remove /home/
                                            if os.path.exists(test_path2) and os.path.isfile(test_path2):
                                                found_path = test_path2
                                                break
                        
                        if found_path:
                            ssh_pkey = found_path
                        else:
                            # If file not found and it looks like key content (starts with -----BEGIN), treat as content directly
                            if key_path.strip().startswith('-----BEGIN'):
                                # Store key content directly - we'll load it with paramiko later
                                ssh_pkey = key_path
                            else:
                                # Provide helpful error message
                                error_msg = "SSH private key file not found: {}. ".format(original_path)
                                if os.path.exists('/.dockerenv'):
                                    error_msg += "If running in Docker, you need to mount the SSH key directory as a volume. "
                                    error_msg += "Add this to your docker-compose.yaml volumes: '- /home/hok/.ssh:/mnt/ssh_keys:ro' "
                                    error_msg += "and use the path '/mnt/ssh_keys/id_rsa' in the configuration. "
                                    error_msg += "Alternatively, paste the key content directly (it should start with '-----BEGIN')."
                                raise ValueError(error_msg)
                    else:
                        # Private key content (base64 encoded or raw) - DO NOT use temp files
                        # Just pass the raw key content - we'll load it with paramiko directly later
                        import base64
                        try:
                            # Try to decode as base64
                            key_content = base64.b64decode(key_path).decode("utf-8")
                        except:
                            # If not base64, assume it's already the key content
                            key_content = key_path
                        
                        # Store the key content directly - we'll load it with paramiko later
                        # This avoids all temp file handling which was causing issues
                        ssh_pkey = key_content
                
                # Use private key from data source if available, otherwise use global config
                if ssh_pkey:
                    # FIX: Load the key with paramiko and pass the PKey object directly to sshtunnel
                    # This avoids file handling issues and the TextIOWrapper error
                    import paramiko
                    import io
                    pkey_obj = None
                    
                    if isinstance(ssh_pkey, str):
                        # It's either a file path or key content
                        if ssh_pkey.strip().startswith('-----BEGIN'):
                            # It's key content - load it from StringIO
                            try:
                                try:
                                    pkey_obj = paramiko.RSAKey.from_private_key(
                                        io.StringIO(ssh_pkey),
                                        password=details.get("ssh_passphrase") or None
                                    )
                                except (paramiko.ssh_exception.SSHException, paramiko.ssh_exception.PasswordRequiredException):
                                    try:
                                        pkey_obj = paramiko.Ed25519Key.from_private_key(
                                            io.StringIO(ssh_pkey),
                                            password=details.get("ssh_passphrase") or None
                                        )
                                    except (paramiko.ssh_exception.SSHException, paramiko.ssh_exception.PasswordRequiredException):
                                        try:
                                            pkey_obj = paramiko.ECDSAKey.from_private_key(
                                                io.StringIO(ssh_pkey),
                                                password=details.get("ssh_passphrase") or None
                                            )
                                        except Exception as e:
                                            try:
                                                import sys
                                                sys.stderr.write("DEBUG: Failed to load key content with paramiko: {}\n".format(str(e)))
                                                sys.stderr.flush()
                                            except: pass
                            except Exception as key_load_err:
                                try:
                                    import sys
                                    import traceback
                                    sys.stderr.write("DEBUG: Key content loading failed: {}\n".format(str(key_load_err)))
                                    sys.stderr.write("DEBUG: Traceback: {}\n".format(traceback.format_exc()))
                                    sys.stderr.flush()
                                except: pass
                        else:
                            # It's a file path - load it from file
                            try:
                                try:
                                    pkey_obj = paramiko.RSAKey.from_private_key_file(
                                        ssh_pkey,
                                        password=details.get("ssh_passphrase") or None
                                    )
                                except (paramiko.ssh_exception.SSHException, paramiko.ssh_exception.PasswordRequiredException):
                                    try:
                                        pkey_obj = paramiko.Ed25519Key.from_private_key_file(
                                            ssh_pkey,
                                            password=details.get("ssh_passphrase") or None
                                        )
                                    except (paramiko.ssh_exception.SSHException, paramiko.ssh_exception.PasswordRequiredException):
                                        try:
                                            pkey_obj = paramiko.ECDSAKey.from_private_key_file(
                                                ssh_pkey,
                                                password=details.get("ssh_passphrase") or None
                                            )
                                        except Exception as e:
                                            try:
                                                import sys
                                                sys.stderr.write("DEBUG: Failed to load key file with paramiko: {}\n".format(str(e)))
                                                sys.stderr.flush()
                                            except: pass
                            except Exception as key_load_err:
                                try:
                                    import sys
                                    import traceback
                                    sys.stderr.write("DEBUG: Key file loading failed: {}\n".format(str(key_load_err)))
                                    sys.stderr.write("DEBUG: Traceback: {}\n".format(traceback.format_exc()))
                                    sys.stderr.flush()
                                except: pass
                    elif isinstance(ssh_pkey, (paramiko.PKey, paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey)):
                        # It's already a PKey object
                        pkey_obj = ssh_pkey
                    
                    if pkey_obj:
                        # Pass the PKey object directly to sshtunnel - this should avoid file handling issues
                        auth["ssh_pkey"] = pkey_obj
                        try:
                            import sys
                            sys.stderr.write("DEBUG: Using paramiko PKey object (type: {}) instead of file path\n".format(type(pkey_obj).__name__))
                            sys.stderr.flush()
                        except: pass
                    else:
                        # Fallback: if we can't load the key, try using the file path (if it's a path)
                        if isinstance(ssh_pkey, str) and not ssh_pkey.strip().startswith('-----BEGIN'):
                            # It's a file path and we couldn't load it - pass the path anyway
                            auth["ssh_pkey"] = str(ssh_pkey)
                            try:
                                import sys
                                sys.stderr.write("DEBUG: Could not load key with paramiko, falling back to file path: {}\n".format(ssh_pkey))
                                sys.stderr.flush()
                            except: pass
                        else:
                            # Can't use it - skip it
                            try:
                                import sys
                                sys.stderr.write("DEBUG: Cannot use ssh_pkey (type: {}), skipping it\n".format(type(ssh_pkey).__name__))
                                sys.stderr.flush()
                            except: pass
                
                # Handle passphrase
                if details.get("ssh_passphrase"):
                    auth["ssh_private_key_password"] = details["ssh_passphrase"]
                
                # Handle password authentication (alternative to key)
                if details.get("ssh_password"):
                    auth["ssh_password"] = details["ssh_password"]
                
                # #region agent log
                try:
                    log_file = open(debug_log_path, 'a')
                    log_file.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "F",
                        "location": "query_runner/__init__.py:with_ssh_tunnel:auth_final",
                        "message": "Final auth configuration",
                        "data": {
                            "has_ssh_pkey": "ssh_pkey" in auth,
                            "ssh_pkey_type": type(auth.get("ssh_pkey")).__name__ if "ssh_pkey" in auth else None,
                            "ssh_pkey_value": str(auth.get("ssh_pkey", ""))[:100] if "ssh_pkey" in auth else None,
                            "has_ssh_private_key_password": "ssh_private_key_password" in auth,
                            "has_ssh_password": "ssh_password" in auth,
                            "auth_keys": list(auth.keys()),
                        },
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + '\n')
                    log_file.close()
                except: pass
                # #endregion
                
                # #region agent log
                try:
                    log_file2 = open(debug_log_path, 'a')
                    log_file2.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "G",
                        "location": "query_runner/__init__.py:with_ssh_tunnel:before_open_tunnel",
                        "message": "About to call open_tunnel",
                        "data": {
                            "bastion_address": str(bastion_address),
                            "remote_address": str(remote_address),
                            "auth_dict_keys": list(auth.keys()),
                            "auth_ssh_pkey": str(auth.get("ssh_pkey", ""))[:50] if "ssh_pkey" in auth else None,
                        },
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + '\n')
                    log_file2.close()
                except: pass
                # #endregion
                
                try:
                    # Ensure ssh_pkey is either a string path or a PKey object (not a file object)
                    if "ssh_pkey" in auth:
                        import paramiko
                        ssh_pkey_value = auth["ssh_pkey"]
                        # Allow both string paths and PKey objects
                        if isinstance(ssh_pkey_value, (paramiko.pkey.PKey, paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey)):
                            # It's a PKey object, no validation needed
                            pass
                        elif isinstance(ssh_pkey_value, str):
                            # It's a string path, verify the file exists and is readable
                            import os
                            if not os.path.exists(ssh_pkey_value):
                                raise ValueError("SSH private key file does not exist: {}".format(ssh_pkey_value))
                            if not os.access(ssh_pkey_value, os.R_OK):
                                raise ValueError("SSH private key file is not readable: {}".format(ssh_pkey_value))
                        else:
                            raise ValueError("ssh_pkey must be a string path or paramiko PKey object, got: {}".format(type(ssh_pkey_value)))
                    
                    # #region agent log
                    try:
                        import traceback
                        import json as json_module
                        log_file_pre = open(debug_log_path, 'a')
                        log_file_pre.write(json_module.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "G",
                            "location": "query_runner/__init__.py:with_ssh_tunnel:right_before_open_tunnel",
                            "message": "Right before calling open_tunnel",
                            "data": {
                                "auth_ssh_pkey_type": type(auth.get("ssh_pkey")).__name__ if "ssh_pkey" in auth else None,
                                "auth_ssh_pkey_repr": repr(auth.get("ssh_pkey", ""))[:200] if "ssh_pkey" in auth else None,
                            },
                            "timestamp": int(__import__('time').time() * 1000)
                        }) + '\n')
                        log_file_pre.close()
                    except: pass
                    # #endregion
                    
                    # Verify the key file exists and is readable right before calling open_tunnel
                    # Only validate if ssh_pkey is a string path, not if it's already a PKey object
                    if "ssh_pkey" in auth:
                        import os
                        import sys
                        import paramiko
                        ssh_pkey_value = auth["ssh_pkey"]
                        
                        # Check if it's a PKey object (already loaded) or a string path
                        if isinstance(ssh_pkey_value, (paramiko.pkey.PKey, paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey)):
                            # It's already a PKey object, no need to validate file path
                            # #region agent log
                            try:
                                import json as json_mod
                                import platform
                                log_info = {
                                    "sessionId": "debug-session",
                                    "runId": "run1",
                                    "hypothesisId": "H",
                                    "location": "query_runner/__init__.py:with_ssh_tunnel:pkey_object_check",
                                    "message": "SSH key is PKey object, skipping file validation",
                                    "data": {
                                        "pkey_type": type(ssh_pkey_value).__name__,
                                        "platform": platform.platform(),
                                        "in_docker": os.path.exists('/.dockerenv'),
                                    },
                                    "timestamp": int(__import__('time').time() * 1000)
                                }
                                sys.stderr.write("DEBUG PKEY OBJECT: " + json_mod.dumps(log_info) + "\n")
                                sys.stderr.flush()
                            except: pass
                            # #endregion
                        elif isinstance(ssh_pkey_value, str):
                            # It's a string path, validate the file exists and is readable
                            key_file_path = ssh_pkey_value
                            
                            # #region agent log - Check file path and environment
                            try:
                                import json as json_mod
                                import os as os_mod
                                import platform
                                log_info = {
                                    "sessionId": "debug-session",
                                    "runId": "run1",
                                    "hypothesisId": "H",
                                    "location": "query_runner/__init__.py:with_ssh_tunnel:file_path_check",
                                    "message": "Checking SSH key file path and environment",
                                    "data": {
                                        "key_file_path": key_file_path,
                                        "path_exists": os.path.exists(key_file_path),
                                        "path_is_file": os.path.isfile(key_file_path) if os.path.exists(key_file_path) else False,
                                        "path_is_readable": os.access(key_file_path, os.R_OK) if os.path.exists(key_file_path) else False,
                                        "path_abs": os.path.abspath(key_file_path),
                                        "path_real": os.path.realpath(key_file_path) if os.path.exists(key_file_path) else None,
                                        "platform": platform.platform(),
                                        "in_docker": os.path.exists('/.dockerenv'),
                                        "cwd": os.getcwd(),
                                        "path_sep": os.path.sep,
                                    },
                                    "timestamp": int(__import__('time').time() * 1000)
                                }
                                sys.stderr.write("DEBUG FILE CHECK: " + json_mod.dumps(log_info) + "\n")
                                sys.stderr.flush()
                                try:
                                    with open(debug_log_path, 'a') as log_file_check:
                                        log_file_check.write(json_mod.dumps(log_info) + '\n')
                                except: pass
                            except: pass
                            # #endregion
                            
                            if not os.path.exists(key_file_path):
                                raise ValueError("SSH private key file does not exist: {}".format(key_file_path))
                            if not os.access(key_file_path, os.R_OK):
                                raise ValueError("SSH private key file is not readable: {}".format(key_file_path))
                            
                            # Try to read a small portion of the file to verify it's accessible
                            try:
                                with open(key_file_path, 'r') as test_file:
                                    first_line = test_file.readline()
                                    if not first_line.strip().startswith('-----BEGIN'):
                                        # Log warning but don't fail - might be a different key format
                                        pass
                            except Exception as read_err:
                                raise ValueError("Cannot read SSH private key file {}: {}".format(key_file_path, str(read_err)))
                        else:
                            raise ValueError("ssh_pkey must be a string path or paramiko PKey object, got: {}".format(type(ssh_pkey_value)))
                    
                    # Call open_tunnel with the auth dict
                    # Make a copy of auth to avoid any potential issues
                    auth_copy = dict(auth)
                    
                    # Wrap in inner try-except to catch the actual error
                    # Use sys.stderr to log errors to avoid any conflicts with open()
                    import sys
                    import traceback
                    try:
                        # #region agent log - right before calling open_tunnel
                        try:
                            import json as json_mod
                            with open(debug_log_path, 'a') as log_file_pre_call:
                                log_file_pre_call.write(json_mod.dumps({
                                    "sessionId": "debug-session",
                                    "runId": "run1",
                                    "hypothesisId": "I",
                                    "location": "query_runner/__init__.py:with_ssh_tunnel:about_to_call_open_tunnel",
                                    "message": "About to actually call open_tunnel function",
                                    "data": {
                                        "auth_copy_keys": list(auth_copy.keys()),
                                        "auth_ssh_pkey_type": type(auth_copy.get("ssh_pkey")).__name__ if "ssh_pkey" in auth_copy else None,
                                        "auth_ssh_pkey_is_str": isinstance(auth_copy.get("ssh_pkey"), str) if "ssh_pkey" in auth_copy else None,
                                    },
                                    "timestamp": int(__import__('time').time() * 1000)
                                }) + '\n')
                        except: pass
                        # #endregion
                        
                        # #region agent log - right before the actual call
                        try:
                            import sys
                            sys.stderr.write("DEBUG: About to call open_tunnel with auth_copy keys: {}\n".format(list(auth_copy.keys())))
                            if "ssh_pkey" in auth_copy:
                                sys.stderr.write("DEBUG: ssh_pkey type: {}, value: {}\n".format(type(auth_copy["ssh_pkey"]).__name__, repr(auth_copy["ssh_pkey"])[:100]))
                            sys.stderr.flush()
                        except: pass
                        # #endregion
                        
                        try:
                            # Use auth_copy directly - we'll filter out None values later if needed
                            # Don't add ssh_config_file=None as sshtunnel may not accept None
                            auth_copy_with_config = dict(auth_copy)
                            
                            # Enable paramiko debug logging to see what's happening
                            try:
                                import logging
                                paramiko_logger = logging.getLogger("paramiko")
                                paramiko_logger.setLevel(logging.DEBUG)
                                # Create a handler that writes to stderr
                                import sys
                                handler = logging.StreamHandler(sys.stderr)
                                handler.setLevel(logging.DEBUG)
                                formatter = logging.Formatter('PARAMIKO DEBUG: %(name)s - %(levelname)s - %(message)s')
                                handler.setFormatter(formatter)
                                paramiko_logger.addHandler(handler)
                            except Exception as log_setup_err:
                                # If logging setup fails, continue anyway
                                try:
                                    import sys
                                    sys.stderr.write("DEBUG: Failed to setup paramiko logging: {}\n".format(str(log_setup_err)))
                                    sys.stderr.flush()
                                except: pass
                            
                            # Test direct paramiko connection to get more detailed error messages
                            # Store in outer scope so it can be included in error logs
                            paramiko_test_result = None
                            if "ssh_pkey" in auth_copy_with_config:
                                try:
                                    import paramiko
                                    import sys
                                    test_client = paramiko.SSHClient()
                                    test_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                                    try:
                                        pkey_for_test = auth_copy_with_config["ssh_pkey"]
                                        if isinstance(pkey_for_test, str):
                                            # It's a file path, load it
                                            try:
                                                pkey_for_test = paramiko.RSAKey.from_private_key_file(pkey_for_test, password=details.get("ssh_passphrase") or None)
                                            except:
                                                try:
                                                    pkey_for_test = paramiko.Ed25519Key.from_private_key_file(pkey_for_test, password=details.get("ssh_passphrase") or None)
                                                except:
                                                    pkey_for_test = paramiko.ECDSAKey.from_private_key_file(pkey_for_test, password=details.get("ssh_passphrase") or None)
                                        
                                        # Try to connect with longer timeouts and more lenient settings
                                        test_client.connect(
                                            hostname=bastion_address[0],
                                            port=bastion_address[1],
                                            username=auth_copy_with_config["ssh_username"],
                                            pkey=pkey_for_test,
                                            timeout=60,  # Increased from 30 to 60 seconds
                                            look_for_keys=False,
                                            allow_agent=False,
                                            banner_timeout=60,  # Increased banner timeout
                                            auth_timeout=60,  # Increased auth timeout
                                            compress=False,  # Disable compression to avoid issues
                                            disabled_algorithms={'pubkeys': []}  # Allow all key algorithms
                                        )
                                        # If we get here, connection was successful
                                        paramiko_test_result = {"success": True, "message": "Direct paramiko connection successful"}
                                        test_client.close()
                                    except Exception as paramiko_test_err:
                                        # Get more detailed error information
                                        paramiko_test_result = {
                                            "success": False,
                                            "error_type": type(paramiko_test_err).__name__,
                                            "error_message": str(paramiko_test_err),
                                            "error_repr": repr(paramiko_test_err),
                                        }
                                        # Try to get underlying error details
                                        if hasattr(paramiko_test_err, '__cause__') and paramiko_test_err.__cause__:
                                            paramiko_test_result["underlying_cause"] = {
                                                "type": type(paramiko_test_err.__cause__).__name__,
                                                "message": str(paramiko_test_err.__cause__),
                                            }
                                        if hasattr(paramiko_test_err, '__context__') and paramiko_test_err.__context__:
                                            paramiko_test_result["context"] = {
                                                "type": type(paramiko_test_err.__context__).__name__,
                                                "message": str(paramiko_test_err.__context__),
                                            }
                                        # Check if it's a socket error (network issue)
                                        if "socket" in str(type(paramiko_test_err)).lower() or "timeout" in str(paramiko_test_err).lower():
                                            paramiko_test_result["likely_network_issue"] = True
                                        # Check if it's an authentication error
                                        if "auth" in str(paramiko_test_err).lower() or "permission" in str(paramiko_test_err).lower() or "denied" in str(paramiko_test_err).lower():
                                            paramiko_test_result["likely_auth_issue"] = True
                                        # "No existing session" usually means transport handshake failed
                                        if "no existing session" in str(paramiko_test_err).lower():
                                            paramiko_test_result["likely_handshake_failure"] = True
                                            paramiko_test_result["diagnosis"] = "SSH transport handshake failed - server may be rejecting connection, protocol mismatch, or timeout during handshake"
                                        try:
                                            import traceback
                                            paramiko_test_result["traceback"] = traceback.format_exc()
                                        except: pass
                                except Exception as test_setup_err:
                                    paramiko_test_result = {"test_setup_error": str(test_setup_err)}
                            
                            # Log paramiko test result to both stderr and debug log
                            if paramiko_test_result:
                                try:
                                    import sys
                                    import json as json_test
                                    test_log_msg = "DEBUG PARAMIKO TEST: " + json_test.dumps(paramiko_test_result) + "\n"
                                    sys.stderr.write(test_log_msg)
                                    sys.stderr.flush()
                                    # Also log to debug log file
                                    try:
                                        with open(debug_log_path, 'a') as log_file_test:
                                            log_file_test.write(json_test.dumps({
                                                "sessionId": "debug-session",
                                                "runId": "run1",
                                                "hypothesisId": "K",
                                                "location": "query_runner/__init__.py:with_ssh_tunnel:paramiko_test",
                                                "message": "Direct paramiko connection test result",
                                                "data": paramiko_test_result,
                                                "timestamp": int(__import__('time').time() * 1000)
                                            }) + '\n')
                                    except: pass
                                except: pass
                            
                            # Before calling open_tunnel, verify all values in auth_copy_with_config are not file objects
                            # #region agent log - verify no file objects in auth
                            try:
                                import sys
                                import json as json_verify
                                file_obj_check = {}
                                for key, value in auth_copy_with_config.items():
                                    if hasattr(value, 'read') or hasattr(value, 'write') or hasattr(value, 'close'):
                                        file_obj_check[key] = {
                                            "type": type(value).__name__,
                                            "is_file_like": True,
                                            "repr": repr(value)[:100]
                                        }
                                if file_obj_check:
                                    sys.stderr.write("DEBUG FILE OBJ CHECK: Found file-like objects in auth_copy_with_config: " + json_verify.dumps(file_obj_check) + "\n")
                                    sys.stderr.flush()
                            except: pass
                            # #endregion
                            
                            # Final check: ensure no file objects are passed to open_tunnel
                            # Convert any file-like objects to their string representation or remove them
                            cleaned_auth = {}
                            for key, value in auth_copy_with_config.items():
                                # IMPORTANT: Check for PKey objects FIRST, before checking for file-like objects
                                # PKey objects have read/write methods but ARE valid for sshtunnel
                                try:
                                    import paramiko
                                    if isinstance(value, (paramiko.PKey, paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey)):
                                        # PKey objects are allowed - pass them through
                                        try:
                                            import sys
                                            sys.stderr.write("DEBUG: Found PKey object in auth_copy_with_config[{}], allowing it through (type: {})\n".format(
                                                key, type(value).__name__
                                            ))
                                            sys.stderr.flush()
                                        except: pass
                                        cleaned_auth[key] = value
                                        continue
                                except ImportError:
                                    pass
                                
                                # Now check if value is a file-like object (but not a PKey - already handled above)
                                if hasattr(value, 'read') or hasattr(value, 'write') or hasattr(value, 'close'):
                                    # This is a file object - log it and skip it
                                    try:
                                        import sys
                                        sys.stderr.write("DEBUG: Found file object in auth_copy_with_config[{}]: type={}, repr={}\n".format(
                                            key, type(value).__name__, repr(value)[:200]
                                        ))
                                        sys.stderr.flush()
                                    except: pass
                                    # Skip file objects - they shouldn't be passed to open_tunnel
                                    continue
                                
                                # For ssh_pkey, allow strings
                                if key == "ssh_pkey":
                                    if isinstance(value, str):
                                        # String path is fine
                                        pass
                                    else:
                                        # Not a string - log and skip
                                        try:
                                            import sys
                                            sys.stderr.write("DEBUG: ssh_pkey is not a string or PKey (type: {}), skipping\n".format(type(value).__name__))
                                            sys.stderr.flush()
                                        except: pass
                                        continue
                                
                                # Filter out None values - sshtunnel doesn't accept None for parameters
                                if value is None:
                                    try:
                                        import sys
                                        sys.stderr.write("DEBUG: Filtering out None value for key: {}\n".format(key))
                                        sys.stderr.flush()
                                    except: pass
                                    continue
                                cleaned_auth[key] = value
                            
                            # Log what we're about to pass to open_tunnel
                            try:
                                import sys
                                import json as json_final
                                final_auth_info = {k: type(v).__name__ for k, v in cleaned_auth.items()}
                                sys.stderr.write("DEBUG FINAL AUTH: About to call open_tunnel with: {}\n".format(json_final.dumps(final_auth_info)))
                                sys.stderr.flush()
                            except: pass
                            
                            # Use cleaned_auth instead of auth_copy_with_config
                            # Wrap the open_tunnel call to catch any errors that might occur
                            # #region agent log - right before open_tunnel call
                            try:
                                import sys
                                import json as json_pre_call
                                pre_call_log = {
                                    "sessionId": "debug-session",
                                    "runId": "run1",
                                    "hypothesisId": "N",
                                    "location": "query_runner/__init__.py:with_ssh_tunnel:right_before_open_tunnel_call",
                                    "message": "Right before calling open_tunnel with cleaned_auth",
                                    "data": {
                                        "cleaned_auth_keys": list(cleaned_auth.keys()),
                                        "cleaned_auth_types": {k: type(v).__name__ for k, v in cleaned_auth.items()},
                                        "cleaned_auth_values": {k: (str(v)[:50] if isinstance(v, str) else repr(v)[:50]) for k, v in cleaned_auth.items()},
                                    },
                                    "timestamp": int(__import__('time').time() * 1000)
                                }
                                sys.stderr.write("DEBUG PRE CALL: " + json_pre_call.dumps(pre_call_log) + "\n")
                                sys.stderr.flush()
                                try:
                                    with open(debug_log_path, 'a') as log_file_pre:
                                        log_file_pre.write(json_pre_call.dumps(pre_call_log) + '\n')
                                except: pass
                            except: pass
                            # #endregion
                            
                            try:
                                # Create the tunnel object first (this might fail)
                                # #region agent log - before creating tunnel object
                                try:
                                    import sys
                                    sys.stderr.write("DEBUG: About to create tunnel object with cleaned_auth keys: {}\n".format(list(cleaned_auth.keys())))
                                    if "ssh_pkey" in cleaned_auth:
                                        sys.stderr.write("DEBUG: ssh_pkey value: {}, type: {}\n".format(
                                            cleaned_auth["ssh_pkey"][:100] if isinstance(cleaned_auth["ssh_pkey"], str) else repr(cleaned_auth["ssh_pkey"])[:100],
                                            type(cleaned_auth["ssh_pkey"]).__name__
                                        ))
                                    sys.stderr.flush()
                                except: pass
                                # #endregion
                                
                                # Wrap open_tunnel call in a try/except to catch initialization errors
                                try:
                                    # #region agent log - right before open_tunnel call with exact parameters
                                    try:
                                        import sys
                                        import json as json_exact
                                        exact_params_log = {
                                            "sessionId": "debug-session",
                                            "runId": "run1",
                                            "hypothesisId": "T",
                                            "location": "query_runner/__init__.py:with_ssh_tunnel:exact_open_tunnel_call",
                                            "message": "Exact parameters being passed to open_tunnel",
                                            "data": {
                                                "bastion_address": str(bastion_address),
                                                "remote_bind_address": str(remote_address),
                                                "cleaned_auth": {k: (str(v)[:100] if isinstance(v, str) else type(v).__name__) for k, v in cleaned_auth.items()},
                                                "cleaned_auth_types": {k: type(v).__name__ for k, v in cleaned_auth.items()},
                                            },
                                            "timestamp": int(__import__('time').time() * 1000)
                                        }
                                        sys.stderr.write("DEBUG EXACT PARAMS: " + json_exact.dumps(exact_params_log) + "\n")
                                        sys.stderr.flush()
                                        try:
                                            with open(debug_log_path, 'a') as log_file_exact:
                                                log_file_exact.write(json_exact.dumps(exact_params_log) + '\n')
                                        except: pass
                                    except: pass
                                    # #endregion
                                    
                                    # Final validation: ensure open_tunnel is actually a function, not a file object
                                    if not callable(open_tunnel):
                                        raise TypeError("open_tunnel is not callable! It is: {} (type: {})".format(
                                            open_tunnel, type(open_tunnel).__name__
                                        ))
                                    
                                    # Final validation: ensure all values in cleaned_auth are not file objects
                                    # But allow PKey objects which are valid for sshtunnel
                                    for key, value in cleaned_auth.items():
                                        # Check if it's a file object (but not a PKey object)
                                        is_file_obj = hasattr(value, 'read') or hasattr(value, 'write') or hasattr(value, 'close')
                                        if is_file_obj:
                                            # Check if it's a PKey object (which is allowed)
                                            try:
                                                import paramiko
                                                if isinstance(value, (paramiko.PKey, paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey)):
                                                    # PKey objects are allowed - they have read/write methods but are not file objects
                                                    continue
                                            except ImportError:
                                                pass
                                            # It's a file object and not a PKey - reject it
                                            raise TypeError("cleaned_auth['{}'] is a file object (type: {}), not a string or PKey!".format(
                                                key, type(value).__name__
                                            ))
                                        # Allow strings, ints, bools, None, and PKey objects
                                        if not isinstance(value, (str, int, bool, type(None))):
                                            # Check if it's a PKey object (which is allowed)
                                            try:
                                                import paramiko
                                                if isinstance(value, (paramiko.PKey, paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey)):
                                                    # PKey objects are allowed
                                                    continue
                                            except ImportError:
                                                pass
                                            # Log warning for non-standard types (but don't reject PKey objects)
                                            try:
                                                import sys
                                                sys.stderr.write("WARNING: cleaned_auth['{}'] has unexpected type: {}\n".format(
                                                    key, type(value).__name__
                                                ))
                                                sys.stderr.flush()
                                            except: pass
                                    
                                    # Ensure None values are filtered out
                                    final_auth = {k: v for k, v in cleaned_auth.items() if v is not None}
                                    
                                    # CRITICAL FIX: sshtunnel accepts 'ssh_pkey' for PKey objects, but 'ssh_private_key' for file paths
                                    # Since we're passing a PKey object, keep it as 'ssh_pkey' - don't rename!
                                    # Only rename if it's a string (file path)
                                    if "ssh_pkey" in final_auth:
                                        ssh_pkey_value = final_auth["ssh_pkey"]
                                        # Check if it's a PKey object or a string
                                        try:
                                            import paramiko
                                            if isinstance(ssh_pkey_value, (paramiko.PKey, paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey)):
                                                # It's a PKey object - keep it as ssh_pkey (sshtunnel accepts this)
                                                try:
                                                    import sys
                                                    sys.stderr.write("DEBUG: Keeping ssh_pkey as-is (PKey object, type: {})\n".format(type(ssh_pkey_value).__name__))
                                                    sys.stderr.flush()
                                                except: pass
                                            elif isinstance(ssh_pkey_value, str):
                                                # It's a string (file path) - rename to ssh_private_key for sshtunnel compatibility
                                                final_auth["ssh_private_key"] = final_auth.pop("ssh_pkey")
                                                try:
                                                    import sys
                                                    sys.stderr.write("DEBUG: Renamed ssh_pkey to ssh_private_key (file path string)\n")
                                                    sys.stderr.flush()
                                                    # Also log to file
                                                    try:
                                                        import os as os_rename
                                                        with open(debug_log_path, 'a') as log_file_rename:
                                                            log_file_rename.write("DEBUG: Renamed ssh_pkey to ssh_private_key (file path)\n")
                                                    except: pass
                                                except: pass
                                        except ImportError:
                                            # paramiko not available - assume it's a string and rename
                                            if isinstance(ssh_pkey_value, str):
                                                final_auth["ssh_private_key"] = final_auth.pop("ssh_pkey")
                                                try:
                                                    import sys
                                                    sys.stderr.write("DEBUG: Renamed ssh_pkey to ssh_private_key (assuming string, paramiko not available)\n")
                                                    sys.stderr.flush()
                                                except: pass
                                    
                                    # Final check: ensure open_tunnel is the function, not a file object
                                    import sshtunnel
                                    if open_tunnel is not sshtunnel.open_tunnel:
                                        raise TypeError("open_tunnel has been shadowed! It is: {} (type: {})".format(
                                            open_tunnel, type(open_tunnel).__name__
                                        ))
                                    
                                    # Log final_auth before calling open_tunnel
                                    try:
                                        import sys
                                        import json as json_final_auth
                                        final_auth_log = {
                                            "sessionId": "debug-session",
                                            "runId": "run1",
                                            "hypothesisId": "V",
                                            "location": "query_runner/__init__.py:with_ssh_tunnel:final_auth_before_call",
                                            "message": "Final auth dict before open_tunnel call",
                                            "data": {
                                                "final_auth_keys": list(final_auth.keys()),
                                                "final_auth_types": {k: type(v).__name__ for k, v in final_auth.items()},
                                                "final_auth_values": {k: (str(v)[:100] if isinstance(v, str) else repr(v)[:100]) for k, v in final_auth.items()},
                                            },
                                            "timestamp": int(__import__('time').time() * 1000)
                                        }
                                        sys.stderr.write("DEBUG FINAL AUTH: " + json_final_auth.dumps(final_auth_log) + "\n")
                                        sys.stderr.flush()
                                        try:
                                            import os as os_final_auth
                                            with open(debug_log_path, 'a') as log_file_final_auth:
                                                log_file_final_auth.write(json_final_auth.dumps(final_auth_log) + '\n')
                                        except: pass
                                    except: pass
                                    
                                    # Wrap the actual call in a try/except that will catch ANY error, even from deep inside sshtunnel
                                    # #region agent log - CRITICAL: log right before open_tunnel call
                                    try:
                                        import sys
                                        sys.stderr.write("DEBUG CRITICAL: About to call open_tunnel NOW\n")
                                        sys.stderr.flush()
                                        # Also write to file
                                        try:
                                            with open(debug_log_path, 'a') as critical_log:
                                                critical_log.write('{"hypothesisId":"CRITICAL","message":"About to call open_tunnel NOW","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                                        except: pass
                                    except: pass
                                    # #endregion
                                    
                                    try:
                                        # VERIFY open_tunnel is callable right before calling it
                                        try:
                                            import sys
                                            sys.stderr.write("DEBUG: open_tunnel type = {}, callable = {}\n".format(type(open_tunnel).__name__, callable(open_tunnel)))
                                            sys.stderr.flush()
                                        except: pass
                                        
                                        tunnel_obj = open_tunnel(bastion_address, remote_bind_address=remote_address, **final_auth)
                                        
                                        # #region agent log - after open_tunnel call succeeds
                                        try:
                                            import sys
                                            sys.stderr.write("DEBUG CRITICAL: open_tunnel returned successfully! tunnel_obj type = {}\n".format(type(tunnel_obj).__name__))
                                            sys.stderr.flush()
                                            try:
                                                with open(debug_log_path, 'a') as success_log:
                                                    success_log.write('{"hypothesisId":"CRITICAL_SUCCESS","message":"open_tunnel returned successfully","tunnel_type":"' + type(tunnel_obj).__name__ + '","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                                            except: pass
                                        except: pass
                                        # #endregion
                                    except BaseException as tunnel_create_err:
                                        # #region agent log - CRITICAL: catch any error from open_tunnel
                                        try:
                                            import sys
                                            import traceback
                                            sys.stderr.write("DEBUG CRITICAL ERROR: Exception caught from open_tunnel: {} - {}\n".format(
                                                type(tunnel_create_err).__name__, str(tunnel_create_err)
                                            ))
                                            sys.stderr.write("DEBUG CRITICAL ERROR TRACEBACK:\n{}\n".format(traceback.format_exc()))
                                            sys.stderr.flush()
                                            try:
                                                import json as json_crit_err
                                                with open(debug_log_path, 'a') as crit_err_log:
                                                    crit_err_log.write(json_crit_err.dumps({
                                                        "hypothesisId": "CRITICAL_ERROR",
                                                        "message": "Exception from open_tunnel",
                                                        "error_type": type(tunnel_create_err).__name__,
                                                        "error_message": str(tunnel_create_err),
                                                        "traceback": traceback.format_exc(),
                                                        "timestamp": int(__import__('time').time() * 1000)
                                                    }) + '\n')
                                            except: pass
                                        except: pass
                                        # #endregion
                                        
                                        # This should catch the TextIOWrapper error if it happens during tunnel creation
                                        error_msg = str(tunnel_create_err)
                                        error_type = type(tunnel_create_err).__name__
                                        if "TextIOWrapper" in error_msg or "not callable" in error_msg:
                                            # This is the error we're looking for!
                                            try:
                                                import sys
                                                import traceback
                                                import json as json_textio_err
                                                textio_error_info = {
                                                    "sessionId": "debug-session",
                                                    "runId": "run1",
                                                    "hypothesisId": "W",
                                                    "location": "query_runner/__init__.py:with_ssh_tunnel:textiowrapper_caught",
                                                    "message": "TextIOWrapper error caught during open_tunnel call",
                                                    "data": {
                                                        "error_type": error_type,
                                                        "error_message": error_msg,
                                                        "error_repr": repr(tunnel_create_err),
                                                        "traceback": traceback.format_exc(),
                                                        "final_auth_keys": list(final_auth.keys()),
                                                        "final_auth_types": {k: type(v).__name__ for k, v in final_auth.items()},
                                                        "final_auth_values": {k: (str(v)[:100] if isinstance(v, str) else repr(v)[:100]) for k, v in final_auth.items()},
                                                    },
                                                    "timestamp": int(__import__('time').time() * 1000)
                                                }
                                                sys.stderr.write("DEBUG TEXTIOWRAPPER CAUGHT: " + json_textio_err.dumps(textio_error_info) + "\n")
                                                sys.stderr.flush()
                                                try:
                                                    import os as os_textio_err
                                                    with open(debug_log_path, 'a') as log_file_textio_err:
                                                        log_file_textio_err.write(json_textio_err.dumps(textio_error_info) + '\n')
                                                except: pass
                                            except: pass
                                        # Re-raise to be caught by outer handler
                                        raise
                                except Exception as init_err:
                                    # Catch errors during open_tunnel initialization
                                    error_msg = str(init_err)
                                    error_type = type(init_err).__name__
                                    try:
                                        import sys
                                        import traceback
                                        import json as json_init_err
                                        init_error_info = {
                                            "sessionId": "debug-session",
                                            "runId": "run1",
                                            "hypothesisId": "U",
                                            "location": "query_runner/__init__.py:with_ssh_tunnel:open_tunnel_init_error",
                                            "message": "Error during open_tunnel initialization",
                                            "data": {
                                                "error_type": error_type,
                                                "error_message": error_msg,
                                                "error_repr": repr(init_err),
                                                "traceback": traceback.format_exc(),
                                                "cleaned_auth_keys": list(cleaned_auth.keys()),
                                                "cleaned_auth_types": {k: type(v).__name__ for k, v in cleaned_auth.items()},
                                                "cleaned_auth_values": {k: (str(v)[:100] if isinstance(v, str) else repr(v)[:100]) for k, v in cleaned_auth.items()},
                                            },
                                            "timestamp": int(__import__('time').time() * 1000)
                                        }
                                        sys.stderr.write("DEBUG INIT ERROR: " + json_init_err.dumps(init_error_info) + "\n")
                                        sys.stderr.flush()
                                        try:
                                            with open(debug_log_path, 'a') as log_file_init_err:
                                                log_file_init_err.write(json_init_err.dumps(init_error_info) + '\n')
                                        except: pass
                                    except: pass
                                    # Re-raise the exception so it can be handled by the outer exception handler
                                    raise
                                
                                # #region agent log - after creating tunnel object
                                try:
                                    import sys
                                    sys.stderr.write("DEBUG: Tunnel object created successfully, type: {}\n".format(type(tunnel_obj).__name__))
                                    sys.stderr.flush()
                                    # Also log to file
                                    try:
                                        with open(debug_log_path, 'a') as tunnel_created_log:
                                            tunnel_created_log.write('{"hypothesisId":"TUNNEL_CREATED","message":"Tunnel object created","tunnel_type":"' + type(tunnel_obj).__name__ + '","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                                    except: pass
                                except: pass
                                # #endregion
                                
                                # Then enter the context (this is where the error might occur)
                                # #region agent log - before entering context
                                try:
                                    import sys
                                    sys.stderr.write("DEBUG: About to enter context manager\n")
                                    sys.stderr.flush()
                                    # Also log to file
                                    try:
                                        with open(debug_log_path, 'a') as ctx_entry_log:
                                            ctx_entry_log.write('{"hypothesisId":"CONTEXT_ENTER","message":"About to enter context manager","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                                    except: pass
                                except: pass
                                # #endregion
                                
                                try:
                                    server = stack.enter_context(tunnel_obj)
                                except BaseException as ctx_err:
                                    # Log any error during context entry
                                    try:
                                        import sys
                                        import traceback
                                        import json as json_ctx_err
                                        sys.stderr.write("DEBUG CONTEXT ERROR: {}: {}\n".format(type(ctx_err).__name__, str(ctx_err)))
                                        sys.stderr.write("DEBUG CONTEXT TRACEBACK:\n{}\n".format(traceback.format_exc()))
                                        sys.stderr.flush()
                                        with open(debug_log_path, 'a') as ctx_err_log:
                                            ctx_err_log.write(json_ctx_err.dumps({
                                                "hypothesisId": "CONTEXT_ERROR",
                                                "message": "Error during stack.enter_context",
                                                "error_type": type(ctx_err).__name__,
                                                "error_message": str(ctx_err),
                                                "traceback": traceback.format_exc(),
                                                "timestamp": int(__import__('time').time() * 1000)
                                            }) + '\n')
                                    except: pass
                                    raise
                                
                                # #region agent log - after entering context
                                try:
                                    import sys
                                    sys.stderr.write("DEBUG: Successfully entered context manager\n")
                                    sys.stderr.flush()
                                    # Also log to file
                                    try:
                                        with open(debug_log_path, 'a') as ctx_success_log:
                                            ctx_success_log.write('{"hypothesisId":"CONTEXT_SUCCESS","message":"Successfully entered context manager","local_port":' + str(server.local_bind_port if hasattr(server, 'local_bind_port') else 'null') + ',"timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                                    except: pass
                                except: pass
                                # #endregion
                            except BaseException as open_tunnel_err:
                                # Catch ANY exception (including SystemExit, KeyboardInterrupt) from open_tunnel
                                # This will help us catch the TextIOWrapper error if it occurs
                                error_msg = str(open_tunnel_err)
                                error_type = type(open_tunnel_err).__name__
                                
                                # Log ALL errors for debugging - this MUST execute even if there are issues
                                import sys
                                import traceback
                                try:
                                    import json as json_all_err
                                    all_error_info = {
                                        "sessionId": "debug-session",
                                        "runId": "run1",
                                        "hypothesisId": "S",
                                        "location": "query_runner/__init__.py:with_ssh_tunnel:all_errors_in_open_tunnel",
                                        "message": "Exception caught during open_tunnel call",
                                        "data": {
                                            "error_type": error_type,
                                            "error_message": error_msg,
                                            "error_repr": repr(open_tunnel_err),
                                            "traceback": traceback.format_exc(),
                                            "cleaned_auth_keys": list(cleaned_auth.keys()),
                                            "cleaned_auth_types": {k: type(v).__name__ for k, v in cleaned_auth.items()},
                                            "cleaned_auth_values": {k: (str(v)[:100] if isinstance(v, str) else repr(v)[:100]) for k, v in cleaned_auth.items()},
                                        },
                                        "timestamp": int(__import__('time').time() * 1000)
                                    }
                                    error_json = json_all_err.dumps(all_error_info)
                                    sys.stderr.write("DEBUG ALL ERRORS: " + error_json + "\n")
                                    sys.stderr.flush()
                                    # Also log to file - use simple write to avoid nested exceptions
                                    try:
                                        import os as os_all_err
                                        with open(debug_log_path, 'a') as log_file_all_err:
                                            log_file_all_err.write(error_json + '\n')
                                    except: pass
                                except Exception as log_err:
                                    # If JSON logging fails, at least write the raw error
                                    try:
                                        sys.stderr.write("DEBUG ALL ERRORS (raw): type={}, msg={}, traceback:\n{}\n".format(
                                            error_type, error_msg, traceback.format_exc()
                                        ))
                                        sys.stderr.flush()
                                    except: pass
                                
                                # Check if this is the TextIOWrapper error
                                if "TextIOWrapper" in error_msg and "not callable" in error_msg:
                                    # This is the error we're looking for
                                    try:
                                        import sys
                                        import traceback
                                        import json as json_err
                                        error_info = {
                                            "sessionId": "debug-session",
                                            "runId": "run1",
                                            "hypothesisId": "L",
                                            "location": "query_runner/__init__.py:with_ssh_tunnel:textiowrapper_error_in_open_tunnel",
                                            "message": "TextIOWrapper error caught in open_tunnel call",
                                            "data": {
                                                "error_type": error_type,
                                                "error_message": error_msg,
                                                "error_repr": repr(open_tunnel_err),
                                                "traceback": traceback.format_exc(),
                                                "cleaned_auth_keys": list(cleaned_auth.keys()),
                                                "cleaned_auth_types": {k: type(v).__name__ for k, v in cleaned_auth.items()},
                                            },
                                            "timestamp": int(__import__('time').time() * 1000)
                                        }
                                        sys.stderr.write("DEBUG TEXTIOWRAPPER ERROR: " + json_err.dumps(error_info) + "\n")
                                        sys.stderr.flush()
                                        # Also log to file
                                        try:
                                            with open(debug_log_path, 'a') as log_file_err:
                                                log_file_err.write(json_err.dumps(error_info) + '\n')
                                        except: pass
                                    except: pass
                                
                                # Re-raise the exception so it can be handled by the outer exception handler
                                raise
                        except TypeError as type_err:
                            # Catch TypeError specifically (like "'_io.TextIOWrapper' object is not callable")
                            # Check if this is the specific error we're looking for
                            is_textiowrapper_error = "TextIOWrapper" in str(type_err) and "not callable" in str(type_err)
                            try:
                                import sys
                                import traceback
                                import json as json_mod
                                error_info = {
                                    "sessionId": "debug-session",
                                    "runId": "run1",
                                    "hypothesisId": "J",
                                    "location": "query_runner/__init__.py:with_ssh_tunnel:typeerror_in_open_tunnel",
                                    "message": "TypeError caught in open_tunnel call" + (" - TextIOWrapper not callable error detected!" if is_textiowrapper_error else ""),
                                    "data": {
                                        "error_type": "TypeError",
                                        "error_message": str(type_err),
                                        "error_repr": repr(type_err),
                                        "traceback": traceback.format_exc(),
                                        "auth_copy_keys": list(auth_copy.keys()),
                                        "auth_ssh_pkey_type": type(auth_copy.get("ssh_pkey")).__name__ if "ssh_pkey" in auth_copy else None,
                                        "is_textiowrapper_error": is_textiowrapper_error,
                                    },
                                    "timestamp": int(__import__('time').time() * 1000)
                                }
                                # If this is the TextIOWrapper error, add detailed inspection of auth_copy_with_config
                                if is_textiowrapper_error:
                                    error_info["data"]["auth_copy_with_config_inspection"] = {}
                                    for key, value in auth_copy_with_config.items():
                                        error_info["data"]["auth_copy_with_config_inspection"][key] = {
                                            "type": type(value).__name__,
                                            "is_file_like": hasattr(value, 'read') or hasattr(value, 'write') or hasattr(value, 'close'),
                                            "repr": repr(value)[:200] if not isinstance(value, (str, int, float, bool, type(None))) else repr(value),
                                        }
                                sys.stderr.write("DEBUG TYPERROR: " + json_mod.dumps(error_info) + "\n")
                                sys.stderr.flush()
                                # Also log to file
                                import os as os_mod
                                try:
                                    with open(debug_log_path, 'a') as log_file_typeerr:
                                        log_file_typeerr.write(json_mod.dumps(error_info) + '\n')
                                except: pass
                            except: pass
                            raise
                    except BaseException as inner_err:
                        # Log the inner error with full traceback using sys.stderr
                        try:
                            import json as json_mod
                            import socket
                            # Test network connectivity to SSH server before logging error
                            connectivity_test = {}
                            try:
                                test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                test_sock.settimeout(5)
                                result = test_sock.connect_ex(bastion_address)
                                test_sock.close()
                                connectivity_test = {
                                    "can_reach_ssh_host": result == 0,
                                    "connection_result": result,
                                    "ssh_host": bastion_address[0],
                                    "ssh_port": bastion_address[1],
                                }
                            except Exception as conn_test_err:
                                connectivity_test = {
                                    "connectivity_test_error": str(conn_test_err),
                                }
                            
                            # Try to get more detailed error information
                            error_details = {
                                "error_type": type(inner_err).__name__,
                                "error_message": str(inner_err),
                                "error_repr": repr(inner_err),
                                "traceback": traceback.format_exc(),
                                "connectivity_test": connectivity_test,
                            }
                            # Add paramiko test result if it was run (check if we can access it from the try block scope)
                            # Note: paramiko_test_result is in the outer try block, so we need to check if it exists
                            # We'll add a note that paramiko test should have been logged separately
                            error_details["paramiko_test_note"] = "Check for 'paramiko_test' log entry above for direct paramiko connection test results"
                            # If it's an SSH tunnel error, try to get underlying exception details
                            if hasattr(inner_err, '__cause__') and inner_err.__cause__:
                                error_details["underlying_error"] = {
                                    "type": type(inner_err.__cause__).__name__,
                                    "message": str(inner_err.__cause__),
                                    "repr": repr(inner_err.__cause__),
                                }
                            if hasattr(inner_err, '__context__') and inner_err.__context__:
                                error_details["context_error"] = {
                                    "type": type(inner_err.__context__).__name__,
                                    "message": str(inner_err.__context__),
                                    "repr": repr(inner_err.__context__),
                                }
                            # Try to get attributes from sshtunnel error
                            if hasattr(inner_err, 'args') and inner_err.args:
                                error_details["error_args"] = [str(arg) for arg in inner_err.args]
                            # Try to get any additional attributes
                            if hasattr(inner_err, '__dict__'):
                                error_details["error_attributes"] = {k: str(v)[:200] for k, v in inner_err.__dict__.items() if not k.startswith('_')}
                            error_info = {
                                "sessionId": "debug-session",
                                "runId": "run1",
                                "hypothesisId": "G",
                                "location": "query_runner/__init__.py:with_ssh_tunnel:inner_open_tunnel_error",
                                "message": "Error caught in inner try block around open_tunnel",
                                "data": error_details,
                                "timestamp": int(__import__('time').time() * 1000)
                            }
                            # Write to stderr first (always works)
                            sys.stderr.write("DEBUG ERROR: " + json_mod.dumps(error_info) + "\n")
                            sys.stderr.flush()
                            # Also try to write to log file
                            try:
                                with open(debug_log_path, 'a') as log_file_inner:
                                    log_file_inner.write(json_mod.dumps(error_info) + '\n')
                            except: pass
                        except Exception as log_err:
                            # If logging fails, at least try stderr
                            try:
                                sys.stderr.write("DEBUG LOGGING ERROR: {}\n".format(str(log_err)))
                                sys.stderr.flush()
                            except: pass
                        raise
                except BaseException as tunnel_error:
                    # #region agent log
                    try:
                        import traceback
                        log_file3 = open(debug_log_path, 'a')
                        log_file3.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "G",
                            "location": "query_runner/__init__.py:with_ssh_tunnel:open_tunnel_error",
                            "message": "Error in open_tunnel",
                            "data": {
                                "error_type": type(tunnel_error).__name__,
                                "error_message": str(tunnel_error),
                                "error_args": str(tunnel_error.args) if hasattr(tunnel_error, 'args') else None,
                                "traceback": traceback.format_exc(),
                            },
                            "timestamp": int(__import__('time').time() * 1000)
                        }) + '\n')
                        log_file3.close()
                    except Exception as log_err:
                        # If logging fails, at least try to log that
                        try:
                            log_file_err = open(debug_log_path, 'a')
                            log_file_err.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": "run1",
                                "hypothesisId": "G",
                                "location": "query_runner/__init__.py:with_ssh_tunnel:logging_error",
                                "message": "Failed to log error",
                                "data": {"log_error": str(log_err)},
                                "timestamp": int(__import__('time').time() * 1000)
                            }) + '\n')
                            log_file_err.close()
                        except: pass
                    # #endregion
                    raise type(tunnel_error)("SSH tunnel: {}".format(str(tunnel_error)))
            except BaseException as error:
                # #region agent log
                try:
                    import traceback
                    log_file_outer = open(debug_log_path, 'a')
                    log_file_outer.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "G",
                        "location": "query_runner/__init__.py:with_ssh_tunnel:outer_exception",
                        "message": "Outer exception handler",
                        "data": {
                            "error_type": type(error).__name__,
                            "error_message": str(error),
                            "traceback": traceback.format_exc(),
                        },
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + '\n')
                    log_file_outer.close()
                except: pass
                # #endregion
                raise type(error)("SSH tunnel: {}".format(str(error)))

            with stack:
                try:
                    # #region agent log - about to call wrapped function
                    try:
                        import sys
                        sys.stderr.write("DEBUG: About to call wrapped function f(*args, **kwargs)\n")
                        sys.stderr.flush()
                        try:
                            with open(debug_log_path, 'a') as wrapped_log:
                                wrapped_log.write('{"hypothesisId":"WRAPPED_CALL","message":"About to call wrapped function","local_bind_address":"' + str(server.local_bind_address) + '","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                        except: pass
                    except: pass
                    # #endregion
                    
                    # #region agent log - setting host/port
                    try:
                        import sys
                        sys.stderr.write("DEBUG: Setting host/port to: {}\n".format(server.local_bind_address))
                        sys.stderr.flush()
                        try:
                            with open(debug_log_path, 'a') as host_log:
                                host_log.write('{"hypothesisId":"HOST_PORT_SET","message":"Setting host/port","local_bind_address":"' + str(server.local_bind_address) + '","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                        except: pass
                    except: pass
                    # #endregion
                    
                    query_runner.host, query_runner.port = server.local_bind_address
                    
                    # #region agent log - calling f
                    try:
                        import sys
                        sys.stderr.write("DEBUG: Calling f(*args, **kwargs) now\n")
                        sys.stderr.flush()
                        try:
                            with open(debug_log_path, 'a') as f_call_log:
                                f_call_log.write('{"hypothesisId":"F_CALL","message":"About to call f(*args, **kwargs)","f_name":"' + str(f.__name__ if hasattr(f, '__name__') else 'unknown') + '","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                        except: pass
                    except: pass
                    # #endregion
                    
                    try:
                        result = f(*args, **kwargs)
                    except Exception as f_call_err:
                        # Log the exception from f
                        try:
                            import sys
                            import traceback
                            sys.stderr.write("DEBUG F_CALL ERROR: {} - {}\n".format(type(f_call_err).__name__, str(f_call_err)))
                            sys.stderr.write("DEBUG F_CALL TRACEBACK:\n{}\n".format(traceback.format_exc()))
                            sys.stderr.flush()
                            try:
                                import json as json_f_err
                                with open(debug_log_path, 'a') as f_err_log:
                                    f_err_log.write(json_f_err.dumps({
                                        "hypothesisId": "F_CALL_ERROR",
                                        "message": "Exception in f(*args, **kwargs)",
                                        "error_type": type(f_call_err).__name__,
                                        "error_message": str(f_call_err),
                                        "traceback": traceback.format_exc(),
                                        "timestamp": int(__import__('time').time() * 1000)
                                    }) + '\n')
                            except: pass
                        except: pass
                        raise
                    
                    # #region agent log - wrapped function returned
                    try:
                        import sys
                        sys.stderr.write("DEBUG: Wrapped function returned successfully\n")
                        sys.stderr.flush()
                        try:
                            with open(debug_log_path, 'a') as success_log:
                                success_log.write('{"hypothesisId":"WRAPPED_SUCCESS","message":"Wrapped function returned successfully","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                        except: pass
                    except: pass
                    # #endregion
                finally:
                    query_runner.host, query_runner.port = remote_host, remote_port

                return result

        return wrapper

    query_runner.run_query = tunnel(query_runner.run_query)

    return query_runner
