def with_ssh_tunnel(query_runner, details):
    def tunnel(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
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
                ssh_pkey = None
                key_path = details.get("ssh_private_key_path") or details.get("ssh_private_key")
                
                if key_path:
                    import os
                    # Check if it's key content or a file path
                    is_key_content = key_path.strip().startswith('-----BEGIN')
                    is_path = not is_key_content and (key_path.startswith('/') or key_path.startswith('~') or key_path.startswith('C:') or key_path.startswith('c:'))
                    
                    if is_path:
                        # Expand ~ to home directory
                        if key_path.startswith('~'):
                            key_path = os.path.expanduser(key_path)
                        
                        original_path = key_path
                        found_path = None
                        
                        if os.path.exists(key_path) and os.path.isfile(key_path):
                            found_path = key_path
                        elif os.path.exists('/.dockerenv'):
                            # Try WSL mount points if running in Docker
                            for mount_point in ['/mnt/wsl', '/run/desktop/mnt/host']:
                                if os.path.exists(mount_point):
                                    test_path = os.path.join(mount_point, key_path.lstrip('/'))
                                    if os.path.exists(test_path) and os.path.isfile(test_path):
                                        found_path = test_path
                                        break
                                    if key_path.startswith('/home/'):
                                        test_path2 = os.path.join(mount_point, key_path[6:])
                                        if os.path.exists(test_path2) and os.path.isfile(test_path2):
                                            found_path = test_path2
                                            break
                        
                        if found_path:
                            ssh_pkey = found_path
                        elif key_path.strip().startswith('-----BEGIN'):
                            ssh_pkey = key_path
                        else:
                            error_msg = "SSH private key file not found: {}. ".format(original_path)
                            if os.path.exists('/.dockerenv'):
                                error_msg += "Mount the SSH key directory as a volume or paste the key content directly."
                            raise ValueError(error_msg)
                    else:
                        # Key content - decode if base64
                        import base64
                        try:
                            key_content = base64.b64decode(key_path).decode("utf-8")
                        except:
                            key_content = key_path
                        ssh_pkey = key_content
                
                # Load the key with paramiko
                if ssh_pkey:
                    import paramiko
                    import io
                    pkey_obj = None
                    
                    if isinstance(ssh_pkey, str):
                        passphrase = details.get("ssh_passphrase") or None
                        if ssh_pkey.strip().startswith('-----BEGIN'):
                            # Key content
                            for key_class in [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey]:
                                try:
                                    pkey_obj = key_class.from_private_key(io.StringIO(ssh_pkey), password=passphrase)
                                    break
                                except:
                                    continue
                        else:
                            # File path
                            for key_class in [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey]:
                                try:
                                    pkey_obj = key_class.from_private_key_file(ssh_pkey, password=passphrase)
                                    break
                                except:
                                    continue
                    elif isinstance(ssh_pkey, paramiko.PKey):
                        pkey_obj = ssh_pkey
                    
                    if pkey_obj:
                        auth["ssh_pkey"] = pkey_obj
                    elif isinstance(ssh_pkey, str) and not ssh_pkey.strip().startswith('-----BEGIN'):
                        auth["ssh_pkey"] = str(ssh_pkey)
                
                # Handle passphrase and password
                if details.get("ssh_passphrase"):
                    auth["ssh_private_key_password"] = details["ssh_passphrase"]
                if details.get("ssh_password"):
                    auth["ssh_password"] = details["ssh_password"]
                
                # Validate ssh_pkey if present
                if "ssh_pkey" in auth:
                    import paramiko
                    ssh_pkey_value = auth["ssh_pkey"]
                    if not isinstance(ssh_pkey_value, (paramiko.PKey, str)):
                        raise ValueError("ssh_pkey must be a string path or paramiko PKey object")
                    if isinstance(ssh_pkey_value, str):
                        import os
                        if not os.path.exists(ssh_pkey_value):
                            raise ValueError("SSH private key file does not exist: {}".format(ssh_pkey_value))
                
                # Filter auth dict - keep only valid values
                cleaned_auth = {}
                for key, value in auth.items():
                    if value is None:
                        continue
                    # Allow PKey objects and strings
                    try:
                        import paramiko
                        if isinstance(value, paramiko.PKey):
                            cleaned_auth[key] = value
                            continue
                    except ImportError:
                        pass
                    # Skip file-like objects (but not PKey)
                    if hasattr(value, 'read') and hasattr(value, 'close'):
                        continue
                    cleaned_auth[key] = value
                
                # Open the tunnel
                server = stack.enter_context(open_tunnel(bastion_address, remote_bind_address=remote_address, **cleaned_auth))
                
            except BaseException as error:
                raise type(error)("SSH tunnel: {}".format(str(error)))

            with stack:
                try:
                    query_runner.host, query_runner.port = server.local_bind_address
                    result = f(*args, **kwargs)
                finally:
                    query_runner.host, query_runner.port = remote_host, remote_port

                return result

        return wrapper

    query_runner.run_query = tunnel(query_runner.run_query)

    return query_runner
