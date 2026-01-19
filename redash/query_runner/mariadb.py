import logging
import os
import threading

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
                "ssh_tunnel_private_key_path": {
                    "type": "string",
                    "title": "SSH Private Key (paste key content or enter server file path)",
                    "fieldType": "filepath",
                },
                "ssh_tunnel_passphrase": {"type": "string", "title": "SSH Key Passphrase"},
                "ssh_tunnel_password": {"type": "string", "title": "SSH Password (if not using key)"},
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
            "secret": ["passwd", "ssh_tunnel_passphrase", "ssh_tunnel_password"],
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

    def _get_docker_host_ip(self):
        """Get the Docker host IP address when running inside a Docker container."""
        import subprocess
        
        # Try /host.docker.internal first
        try:
            import socket
            socket.gethostbyname('host.docker.internal')
            return 'host.docker.internal'
        except:
            pass
        
        # Try to get gateway IP from /proc/net/route
        try:
            with open('/proc/net/route', 'r') as route_file:
                for line in route_file:
                    parts = line.strip().split()
                    if len(parts) >= 3 and parts[1] == '00000000':
                        gateway_hex = parts[2]
                        gateway_ip = '.'.join(str(int(gateway_hex[i:i+2], 16)) for i in (6, 4, 2, 0))
                        return gateway_ip
        except:
            pass
        
        # Try ip route command
        try:
            result = subprocess.run(['ip', 'route'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if 'default' in line and 'via' in line:
                        parts = line.split()
                        via_idx = parts.index('via')
                        if via_idx + 1 < len(parts):
                            return parts[via_idx + 1]
        except:
            pass
        
        return None

    def _connection(self):
        host = self.configuration.get("host", "")
        port = self.configuration.get("port", 3306)
        
        # Handle WSL/Docker connection
        use_wsl = self.configuration.get("use_wsl_connection", False)
        is_docker = os.path.exists('/.dockerenv')
        
        final_host = host
        if use_wsl and is_docker:
            if host in ("", "localhost", "127.0.0.1"):
                docker_host = self._get_docker_host_ip()
                if docker_host:
                    final_host = docker_host
        
        # Ensure we use TCP/IP connection for localhost
        if final_host in ("", "localhost"):
            final_host = "127.0.0.1"
        
        params = dict(
            host=final_host,
            user=self.configuration.get("user", ""),
            passwd=self.configuration.get("passwd", ""),
            db=self.configuration["db"],
            port=port,
            charset=self.configuration.get("charset", "utf8"),
            use_unicode=self.configuration.get("use_unicode", True),
            connect_timeout=self.configuration.get("connect_timeout", 60),
            autocommit=self.configuration.get("autocommit", True),
        )

        ssl_options = self._get_ssl_parameters()
        if ssl_options:
            params["ssl"] = ssl_options

        connection = MySQLdb.connect(**params)
        return connection

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
        ev = threading.Event()
        thread_id = ""
        r = Result()
        t = None

        try:
            connection = self._connection()
            thread_id = connection.thread_id()
            t = threading.Thread(target=self._run_query, args=(query, user, connection, r, ev))
            t.start()
            while not ev.wait(1):
                pass
        except (KeyboardInterrupt, InterruptException, JobTimeoutException):
            self._cancel(thread_id)
            t.join()
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
