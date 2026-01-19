import json
import logging
import os
import threading
import tempfile

from redash.query_runner import (
    TYPE_DATE,
    TYPE_DATETIME,
    TYPE_FLOAT,
    TYPE_INTEGER,
    TYPE_STRING,
    BaseSQLQueryRunner,
    InterruptException,
    JobTimeoutException,
    register,
)
from redash.settings import parse_boolean

# Use a reliable log path
DEBUG_LOG_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '.cursor', 'debug.log')
DEBUG_LOG_DIR = os.path.dirname(DEBUG_LOG_PATH)
if not os.path.exists(DEBUG_LOG_DIR):
    try:
        os.makedirs(DEBUG_LOG_DIR, exist_ok=True)
    except:
        DEBUG_LOG_PATH = os.path.join(tempfile.gettempdir(), 'redash_mariadb_debug.log')

try:
    import MySQLdb

    enabled = True
except ImportError:
    enabled = False

logger = logging.getLogger(__name__)
types_map = {
    0: TYPE_FLOAT,
    1: TYPE_INTEGER,
    2: TYPE_INTEGER,
    3: TYPE_INTEGER,
    4: TYPE_FLOAT,
    5: TYPE_FLOAT,
    7: TYPE_DATETIME,
    8: TYPE_INTEGER,
    9: TYPE_INTEGER,
    10: TYPE_DATE,
    12: TYPE_DATETIME,
    15: TYPE_STRING,
    16: TYPE_INTEGER,
    246: TYPE_FLOAT,
    253: TYPE_STRING,
    254: TYPE_STRING,
}


class Result:
    def __init__(self):
        pass


class MariaDB(BaseSQLQueryRunner):
    noop_query = "SELECT 1"
    
    def __init__(self, configuration):
        super().__init__(configuration)
        # Ensure host and port are in configuration for SSH tunnel support
        # The base class host/port properties require these keys to exist
        if "host" not in self.configuration or not self.configuration["host"]:
            self.configuration["host"] = "127.0.0.1"
        if "port" not in self.configuration or self.configuration["port"] is None:
            self.configuration["port"] = 3306

    @classmethod
    def configuration_schema(cls):
        show_ssl_settings = parse_boolean(os.environ.get("MYSQL_SHOW_SSL_SETTINGS", "true"))

        schema = {
            "type": "object",
            "properties": {
                "host": {"type": "string", "default": "127.0.0.1"},
                "user": {"type": "string"},
                "passwd": {"type": "string", "title": "Password"},
                "db": {"type": "string", "title": "Database name"},
                "port": {"type": "number", "default": 3306},
                "connect_timeout": {"type": "number", "default": 60, "title": "Connection Timeout"},
                "charset": {"type": "string", "default": "utf8"},
                "use_unicode": {"type": "boolean", "default": True},
                "autocommit": {"type": "boolean", "default": False},
                # WSL/Docker connection option
                "use_wsl_connection": {
                    "type": "boolean",
                    "title": "Use WSL/Docker Connection (Enable if Redash runs in Docker and MariaDB is on WSL2 host)",
                    "default": False,
                },
                # SSH Tunnel configuration
                "ssh_tunnel_enabled": {"type": "boolean", "title": "Use SSH Tunnel", "default": False},
                "ssh_tunnel_host": {"type": "string", "title": "SSH Host (Bastion/Jump Host)"},
                "ssh_tunnel_port": {"type": "number", "title": "SSH Port", "default": 22},
                "ssh_tunnel_username": {"type": "string", "title": "SSH Username"},
                "ssh_tunnel_private_key_path": {"type": "string", "title": "SSH Private Key File Path"},
                "ssh_tunnel_passphrase": {"type": "string", "title": "SSH Private Key Passphrase"},
                "ssh_tunnel_password": {"type": "string", "title": "SSH Password"},
            },
            "order": [
                "host",
                "port",
                "user",
                "passwd",
                "db",
                "connect_timeout",
                "charset",
                "use_unicode",
                "autocommit",
                "use_wsl_connection",
                "ssh_tunnel_enabled",
                "ssh_tunnel_host",
                "ssh_tunnel_port",
                "ssh_tunnel_username",
                "ssh_tunnel_private_key_path",
                "ssh_tunnel_passphrase",
                "ssh_tunnel_password",
            ],
            "required": ["db"],
            "secret": ["passwd", "ssh_tunnel_private_key_path", "ssh_tunnel_passphrase", "ssh_tunnel_password"],
        }

        if show_ssl_settings:
            schema["properties"].update(
                {
                    "ssl_mode": {
                        "type": "string",
                        "title": "SSL Mode",
                        "default": "preferred",
                        "extendedEnum": [
                            {"value": "disabled", "name": "Disabled"},
                            {"value": "preferred", "name": "Preferred"},
                            {"value": "required", "name": "Required"},
                            {"value": "verify-ca", "name": "Verify CA"},
                            {"value": "verify-identity", "name": "Verify Identity"},
                        ],
                    },
                    "use_ssl": {"type": "boolean", "title": "Use SSL"},
                    "ssl_cacert": {
                        "type": "string",
                        "title": "Path to CA certificate file to verify peer against (SSL)",
                    },
                    "ssl_cert": {
                        "type": "string",
                        "title": "Path to client certificate file (SSL)",
                    },
                    "ssl_key": {
                        "type": "string",
                        "title": "Path to private key file (SSL)",
                    },
                }
            )

        return schema

    @classmethod
    def name(cls):
        return "MariaDB"

    @classmethod
    def type(cls):
        return "mariadb"

    @classmethod
    def enabled(cls):
        return enabled

    def _connection(self):
        # #region agent log
        logger.debug("MariaDB _connection: host=%s, port=%s, user=%s, db=%s", 
                    self.configuration.get("host"), self.configuration.get("port"),
                    self.configuration.get("user"), self.configuration.get("db"))
        try:
            log_entry = {
                "sessionId": "debug-session",
                "runId": "run1",
                "hypothesisId": "A",
                "location": "mariadb.py:_connection:entry",
                "message": "Connection function called",
                "data": {
                    "host": self.configuration.get("host"),
                    "port": self.configuration.get("port"),
                    "user": self.configuration.get("user"),
                    "db": self.configuration.get("db"),
                    "host_type": type(self.configuration.get("host")).__name__,
                    "host_empty": self.configuration.get("host") == "",
                    "host_is_localhost": self.configuration.get("host") == "localhost",
                },
                "timestamp": int(__import__('time').time() * 1000)
            }
            logger.debug("MariaDB debug log: %s", json.dumps(log_entry))
            with open(DEBUG_LOG_PATH, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as log_err:
            logger.debug("Failed to write debug log file: %s", str(log_err))
        # #endregion
        
        raw_host = self.configuration.get("host", "")
        
        # #region agent log
        try:
            with open(DEBUG_LOG_PATH, 'a') as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "A,B",
                    "location": "mariadb.py:_connection:host_processing",
                    "message": "Raw host value before processing",
                    "data": {
                        "raw_host": raw_host,
                        "raw_host_repr": repr(raw_host),
                        "is_empty": raw_host == "",
                        "is_none": raw_host is None,
                    },
                    "timestamp": int(__import__('time').time() * 1000)
                }) + '\n')
        except: pass
        # #endregion
        
        # Check if WSL connection mode is enabled
        use_wsl_connection = self.configuration.get("use_wsl_connection", False)
        
        # Force TCP/IP connection: convert empty string or localhost to 127.0.0.1
        # If WSL connection mode is enabled, use Docker gateway IP for localhost connections
        def get_docker_host_ip():
            """Get the Docker host IP when running inside a container"""
            try:
                # Only get Docker host IP if WSL connection mode is enabled
                # Check if we're in Docker by looking for .dockerenv or cgroup
                if use_wsl_connection and os.path.exists('/.dockerenv'):
                    import socket
                    # First try host.docker.internal (works on Docker Desktop and some Linux setups)
                    try:
                        socket.gethostbyname('host.docker.internal')
                        return 'host.docker.internal'
                    except:
                        pass
                    
                    # Try to get gateway from default route
                    try:
                        with open('/proc/net/route', 'r') as f:
                            for line in f:
                                parts = line.strip().split()
                                if len(parts) >= 2 and parts[1] == '00000000':  # default route
                                    gateway_hex = parts[2]
                                    # Convert hex IP to dotted decimal
                                    gateway_ip = '.'.join([
                                        str(int(gateway_hex[i:i+2], 16)) 
                                        for i in range(6, -1, -2)
                                    ])
                                    if gateway_ip and gateway_ip != '0.0.0.0':
                                        return gateway_ip
                    except:
                        pass
                    
                    # Fallback: try common Docker gateway IPs
                    for test_ip in ['172.17.0.1', '172.18.0.1', '172.19.0.1', '172.20.0.1']:
                        try:
                            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            test_socket.settimeout(0.5)
                            result = test_socket.connect_ex((test_ip, 3306))
                            test_socket.close()
                            if result == 0:  # Connection successful
                                return test_ip
                        except:
                            continue
            except:
                pass
            return None
        
        docker_host_ip = get_docker_host_ip() if use_wsl_connection else None
        
        if not raw_host or raw_host == "":
            host = docker_host_ip if (use_wsl_connection and docker_host_ip) else "127.0.0.1"
        elif raw_host == "localhost" or raw_host == "127.0.0.1":
            host = docker_host_ip if (use_wsl_connection and docker_host_ip) else "127.0.0.1"
        else:
            host = raw_host
        
        # Update configuration with processed host so SSH tunnel can access it
        # This ensures host/port properties work correctly for SSH tunneling
        if host != raw_host:
            self.configuration["host"] = host
        
        # #region agent log
        try:
            with open(DEBUG_LOG_PATH, 'a') as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "A,B,C",
                    "location": "mariadb.py:_connection:host_processed",
                    "message": "Host value after processing",
                    "data": {
                        "processed_host": host,
                        "raw_to_processed": f"{raw_host} -> {host}",
                        "use_wsl_connection": use_wsl_connection,
                        "docker_host_ip": docker_host_ip,
                        "in_docker": os.path.exists('/.dockerenv'),
                    },
                    "timestamp": int(__import__('time').time() * 1000)
                }) + '\n')
        except: pass
        # #endregion
        
        # Ensure port is an integer
        port = self.configuration.get("port", 3306)
        if isinstance(port, str):
            try:
                port = int(port)
            except (ValueError, TypeError):
                port = 3306
        
        params = dict(
            host=str(host),  # Ensure host is a string
            user=str(self.configuration.get("user", "")),
            passwd=str(self.configuration.get("passwd", "")),
            db=str(self.configuration["db"]),
            port=int(port),  # Ensure port is an integer
            charset=str(self.configuration.get("charset", "utf8")),
            use_unicode=bool(self.configuration.get("use_unicode", True)),
            connect_timeout=int(self.configuration.get("connect_timeout", 60)),
            autocommit=bool(self.configuration.get("autocommit", True)),
        )
        
        # Force TCP/IP: Don't include unix_socket parameter at all
        # Using a non-empty host (127.0.0.1) ensures TCP/IP connection

        # #region agent log
        try:
            with open(DEBUG_LOG_PATH, 'a') as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "C",
                    "location": "mariadb.py:_connection:params_before_ssl",
                    "message": "Connection parameters before SSL",
                    "data": {
                        "host": params.get("host"),
                        "port": params.get("port"),
                        "unix_socket_in_params": "unix_socket" in params,
                        "has_port": "port" in params,
                    },
                    "timestamp": int(__import__('time').time() * 1000)
                }) + '\n')
        except: pass
        # #endregion

        ssl_options = self._get_ssl_parameters()

        if ssl_options:
            params["ssl"] = ssl_options

        # #region agent log
        try:
            with open(DEBUG_LOG_PATH, 'a') as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "C",
                    "location": "mariadb.py:_connection:before_connect",
                    "message": "About to call MySQLdb.connect",
                    "data": {
                        "final_host": params.get("host"),
                        "final_port": params.get("port"),
                        "unix_socket_in_params": "unix_socket" in params,
                        "has_ssl": "ssl" in params,
                    },
                    "timestamp": int(__import__('time').time() * 1000)
                }) + '\n')
        except: pass
        # #endregion

        try:
            # #region agent log
            try:
                import socket
                test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_socket.settimeout(2)
                socket_result = test_socket.connect_ex((host, params.get("port", 3306)))
                test_socket.close()
                with open(DEBUG_LOG_PATH, 'a') as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "D",
                        "location": "mariadb.py:_connection:socket_test",
                        "message": "Socket connectivity test",
                        "data": {
                            "host": host,
                            "port": params.get("port", 3306),
                            "socket_connect_result": socket_result,
                            "socket_error_none": socket_result == 0,
                            "socket_error_msg": "Connection successful" if socket_result == 0 else f"Error code: {socket_result}",
                        },
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + '\n')
            except Exception as sock_err:
                try:
                    with open(DEBUG_LOG_PATH, 'a') as f:
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "D",
                            "location": "mariadb.py:_connection:socket_test_error",
                            "message": "Socket test failed",
                            "data": {
                                "socket_test_error": str(sock_err),
                            },
                            "timestamp": int(__import__('time').time() * 1000)
                        }) + '\n')
                except: pass
            # #endregion
            
            # #region agent log
            try:
                with open(DEBUG_LOG_PATH, 'a') as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "E",
                        "location": "mariadb.py:_connection:mysql_connect_call",
                        "message": "Calling MySQLdb.connect with final params",
                        "data": {
                            "param_types": {k: type(v).__name__ for k, v in params.items() if k != "passwd"},
                            "param_values": {k: v if k != "passwd" else "***" for k, v in params.items()},
                        },
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + '\n')
            except Exception as log_err:
                # Try to log the logging error
                try:
                    with open(DEBUG_LOG_PATH, 'a') as f:
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "E",
                            "location": "mariadb.py:_connection:log_error",
                            "message": "Failed to write log",
                            "data": {"log_error": str(log_err)},
                            "timestamp": int(__import__('time').time() * 1000)
                        }) + '\n')
                except: pass
            # #endregion
            
            connection = MySQLdb.connect(**params)
            
            # #region agent log
            try:
                with open(DEBUG_LOG_PATH, 'a') as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "A,B,C",
                        "location": "mariadb.py:_connection:success",
                        "message": "Connection successful",
                        "data": {
                            "connection_type": type(connection).__name__,
                        },
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + '\n')
            except: pass
            # #endregion
            
            return connection
        except Exception as e:
            # #region agent log
            try:
                error_code = None
                error_msg = str(e)
                if hasattr(e, 'args') and e.args:
                    if len(e.args) >= 2:
                        error_code = e.args[0]
                        error_msg = e.args[1] if len(e.args) > 1 else str(e)
                
                with open(DEBUG_LOG_PATH, 'a') as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "A,B,C,D",
                        "location": "mariadb.py:_connection:error",
                        "message": "Connection failed",
                        "data": {
                            "error_type": type(e).__name__,
                            "error_message": error_msg,
                            "error_code": error_code,
                            "error_args": str(e.args) if hasattr(e, 'args') else None,
                            "params_used": {k: v if k != "passwd" else "***" for k, v in params.items()},
                            "final_host": host,
                            "final_port": params.get("port"),
                        },
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + '\n')
            except: pass
            # #endregion
            
            # Provide helpful error message for Docker/WSL networking issues
            if use_wsl_connection and docker_host_ip and host == docker_host_ip:
                error_msg = str(e)
                if "Can't connect" in error_msg or "2002" in str(e.args[0] if e.args else ""):
                    enhanced_msg = (
                        f"{error_msg}\n\n"
                        f"Note: You're connecting from Docker to MariaDB on the host. "
                        f"MariaDB needs to be configured to accept connections from the Docker network.\n"
                        f"Detected Docker gateway IP: {docker_host_ip}\n"
                        f"To fix this, configure MariaDB to bind to 0.0.0.0 or {docker_host_ip}:\n"
                        f"1. Edit MariaDB config (usually /etc/mysql/mariadb.conf.d/50-server.cnf)\n"
                        f"2. Set bind-address = 0.0.0.0 (or bind-address = {docker_host_ip})\n"
                        f"3. Restart MariaDB: sudo systemctl restart mariadb\n"
                        f"4. Ensure firewall allows connections from Docker network"
                    )
                    raise type(e)(enhanced_msg) from e
            
            raise

    def _get_tables(self, schema):
        query = """
        SELECT col.table_schema as table_schema,
               col.table_name as table_name,
               col.column_name as column_name,
               col.data_type as data_type,
               col.column_comment as column_comment
        FROM `information_schema`.`columns` col
        WHERE LOWER(col.table_schema) NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys');
        """

        results, error = self.run_query(query, None)

        if error is not None:
            self._handle_run_query_error(error)

        for row in results["rows"]:
            if row["table_schema"] != self.configuration["db"]:
                table_name = "{}.{}".format(row["table_schema"], row["table_name"])
            else:
                table_name = row["table_name"]

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            schema[table_name]["columns"].append(
                {
                    "name": row["column_name"],
                    "type": row["data_type"],
                    "description": row["column_comment"],
                }
            )

        table_query = """
                      SELECT col.table_schema as table_schema,
                             col.table_name as table_name,
                             col.table_comment as table_comment
                      FROM `information_schema`.`tables` col
                      WHERE LOWER(col.table_schema) NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys'); \
                      """

        results, error = self.run_query(table_query, None)

        if error is not None:
            self._handle_run_query_error(error)

        for row in results["rows"]:
            if row["table_schema"] != self.configuration["db"]:
                table_name = "{}.{}".format(row["table_schema"], row["table_name"])
            else:
                table_name = row["table_name"]

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            if "table_comment" in row and row["table_comment"]:
                schema[table_name]["description"] = row["table_comment"]

        return list(schema.values())

    def run_query(self, query, user):
        # #region agent log - run_query entry
        try:
            import sys
            sys.stderr.write("DEBUG MARIADB run_query: Entry\n")
            sys.stderr.flush()
            try:
                with open(DEBUG_LOG_PATH, 'a') as log_rq:
                    log_rq.write('{"hypothesisId":"MARIADB_RUN_QUERY","message":"run_query called","query":"' + str(query)[:50].replace('"', "'") + '","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
            except: pass
        except: pass
        # #endregion
        
        ev = threading.Event()
        thread_id = ""
        r = Result()
        t = None

        try:
            # #region agent log - about to call _connection
            try:
                import sys
                sys.stderr.write("DEBUG MARIADB run_query: About to call _connection()\n")
                sys.stderr.flush()
                try:
                    with open(DEBUG_LOG_PATH, 'a') as log_conn:
                        log_conn.write('{"hypothesisId":"MARIADB_BEFORE_CONNECTION","message":"About to call _connection()","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                except: pass
            except: pass
            # #endregion
            
            connection = self._connection()
            
            # #region agent log - _connection succeeded
            try:
                import sys
                sys.stderr.write("DEBUG MARIADB run_query: _connection() returned successfully\n")
                sys.stderr.flush()
                try:
                    with open(DEBUG_LOG_PATH, 'a') as log_conn_ok:
                        log_conn_ok.write('{"hypothesisId":"MARIADB_CONNECTION_SUCCESS","message":"_connection() succeeded","timestamp":' + str(int(__import__('time').time() * 1000)) + '}\n')
                except: pass
            except: pass
            # #endregion
            thread_id = connection.thread_id()
            t = threading.Thread(target=self._run_query, args=(query, user, connection, r, ev))
            t.start()
            while not ev.wait(1):
                pass
        except (KeyboardInterrupt, InterruptException, JobTimeoutException):
            self._cancel(thread_id)
            t.join()
            raise
        except Exception as run_query_err:
            # #region agent log - run_query exception
            try:
                import sys
                import traceback
                sys.stderr.write("DEBUG MARIADB run_query EXCEPTION: {} - {}\n".format(type(run_query_err).__name__, str(run_query_err)))
                sys.stderr.write("DEBUG MARIADB run_query TRACEBACK:\n{}\n".format(traceback.format_exc()))
                sys.stderr.flush()
                try:
                    import json as json_err
                    with open(DEBUG_LOG_PATH, 'a') as log_err:
                        log_err.write(json_err.dumps({
                            "hypothesisId": "MARIADB_RUN_QUERY_ERROR",
                            "message": "Exception in run_query",
                            "error_type": type(run_query_err).__name__,
                            "error_message": str(run_query_err),
                            "traceback": traceback.format_exc(),
                            "timestamp": int(__import__('time').time() * 1000)
                        }) + '\n')
                except: pass
            except: pass
            # #endregion
            raise

        return r.data, r.error

    def _run_query(self, query, user, connection, r, ev):
        try:
            cursor = connection.cursor()
            logger.debug("MariaDB running query: %s", query)
            cursor.execute(query)

            data = cursor.fetchall()
            desc = cursor.description

            while cursor.nextset():
                if cursor.description is not None:
                    data = cursor.fetchall()
                    desc = cursor.description

            # TODO - very similar to pg.py
            if desc is not None:
                columns = self.fetch_columns([(i[0], types_map.get(i[1], None)) for i in desc])
                rows = [dict(zip((column["name"] for column in columns), row)) for row in data]

                data = {"columns": columns, "rows": rows}
                r.data = data
                r.error = None
            else:
                r.data = None
                r.error = "No data was returned."

            cursor.close()
        except MySQLdb.Error as e:
            if cursor:
                cursor.close()
            r.data = None
            r.error = e.args[1]
        finally:
            ev.set()
            if connection:
                connection.close()

    def _get_ssl_parameters(self):
        if not self.configuration.get("use_ssl"):
            return None

        ssl_params = {}

        if self.configuration.get("use_ssl"):
            config_map = {"ssl_mode": "preferred", "ssl_cacert": "ca", "ssl_cert": "cert", "ssl_key": "key"}
            for key, cfg in config_map.items():
                val = self.configuration.get(key)
                if val:
                    ssl_params[cfg] = val

        return ssl_params

    def _cancel(self, thread_id):
        connection = None
        cursor = None
        error = None

        try:
            connection = self._connection()
            cursor = connection.cursor()
            query = "KILL %d" % (thread_id)
            logging.debug(query)
            cursor.execute(query)
        except MySQLdb.Error as e:
            if cursor:
                cursor.close()
            error = e.args[1]
        finally:
            if connection:
                connection.close()

        return error


register(MariaDB)
