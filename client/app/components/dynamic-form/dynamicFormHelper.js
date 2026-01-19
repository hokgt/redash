import React from "react";
import { each, includes, isUndefined, isEmpty, isNil, map, get, some } from "lodash";

function orderedInputs(properties, order, targetOptions) {
  const inputs = new Array(order.length);
  Object.keys(properties).forEach(key => {
    const position = order.indexOf(key);
    const input = {
      name: key,
      title: properties[key].title,
      type: properties[key].type,
      placeholder: isNil(properties[key].default) ? null : properties[key].default.toString(),
      required: properties[key].required,
      extra: properties[key].extra,
      initialValue: targetOptions[key],
    };

    if (input.type === "select") {
      input.placeholder = "Select an option";
      input.options = properties[key].options;
    }

    if (position > -1) {
      inputs[position] = input;
    } else {
      inputs.push(input);
    }
  });
  return inputs;
}

function normalizeSchema(configurationSchema) {
  each(configurationSchema.properties, (prop, name) => {
    if (name.endsWith("File")) {
      prop.type = "file";
    }

    if (prop.type === "boolean") {
      prop.type = "checkbox";
    }

    if (prop.type === "string") {
      // Check if this is a password field
      if (name === "password" || name === "passwd" || name.includes("password") || name.includes("passphrase")) {
        prop.type = "password";
      } else if (name.includes("private_key_path") || name.includes("_path") && name.includes("key")) {
        // File path fields - use filepath selector
        prop.type = "filepath";
      } else if (name.includes("private_key") || (name.includes("key") && name.includes("private"))) {
        // Private keys are typically multi-line, use textarea
        prop.type = "textarea";
      } else {
        prop.type = "text";
      }
    }

    if (!isEmpty(prop.enum)) {
      prop.type = "select";
      prop.options = map(prop.enum, value => ({ value, name: value }));
    }

    if (!isEmpty(prop.extendedEnum)) {
      prop.type = "select";
      prop.options = prop.extendedEnum;
    }

    prop.required = includes(configurationSchema.required, name);
    prop.extra = includes(configurationSchema.extra_options, name);
  });

  configurationSchema.order = configurationSchema.order || [];
}

function setDefaultValueToFields(configurationSchema, options = {}) {
  const properties = configurationSchema.properties;
  Object.keys(properties).forEach(key => {
    const property = properties[key];
    // set default value for checkboxes
    if (!isUndefined(property.default) && property.type === "checkbox") {
      options[key] = property.default;
    }
    // set default or first value when value has predefined options
    if (property.type === "select") {
      const optionValues = map(property.options, option => option.value);
      options[key] = includes(optionValues, property.default) ? property.default : optionValues[0];
    }
  });
}

function flattenSSHTunnel(options) {
  // If ssh_tunnel exists, flatten it to ssh_tunnel_* fields for form display
  if (options && options.ssh_tunnel) {
    const sshTunnel = options.ssh_tunnel;
    const flattened = { ...options };
    
    flattened.ssh_tunnel_enabled = true;
    flattened.ssh_tunnel_host = sshTunnel.ssh_host || "";
    flattened.ssh_tunnel_port = sshTunnel.ssh_port || 22;
    flattened.ssh_tunnel_username = sshTunnel.ssh_username || "";
    // Map ssh_private_key to ssh_tunnel_private_key_path for file path field
    // If ssh_private_key_path exists, use it; otherwise use ssh_private_key (might be a path or content)
    flattened.ssh_tunnel_private_key_path = sshTunnel.ssh_private_key_path || sshTunnel.ssh_private_key || "";
    flattened.ssh_tunnel_passphrase = sshTunnel.ssh_passphrase || "";
    flattened.ssh_tunnel_password = sshTunnel.ssh_password || "";
    
    // Remove the nested ssh_tunnel object
    delete flattened.ssh_tunnel;
    
    return flattened;
  }
  // Return a copy to avoid mutating the original
  return options ? { ...options } : {};
}

function getFields(type = {}, target = { options: {} }) {
  const configurationSchema = type.configuration_schema;
  normalizeSchema(configurationSchema);
  
  // Flatten SSH tunnel if it exists (create a copy to avoid mutating original)
  const flattenedOptions = flattenSSHTunnel(target.options);
  const optionsForFields = flattenedOptions;
  
  const hasTargetObject = Object.keys(optionsForFields).length > 0;
  if (!hasTargetObject) {
    setDefaultValueToFields(configurationSchema, optionsForFields);
  }

  const isNewTarget = !target.id;
  const inputs = [
    {
      name: "name",
      title: "Name",
      type: "text",
      required: true,
      initialValue: target.name,
      contentAfter: React.createElement("hr"),
      placeholder: `My ${type.name}`,
      autoFocus: isNewTarget,
    },
    ...orderedInputs(configurationSchema.properties, configurationSchema.order, optionsForFields),
  ];

  return inputs;
}

function transformSSHTunnelFields(values) {
  // Transform flat ssh_tunnel_* fields to nested ssh_tunnel object
  const transformed = { ...values };
  const sshTunnelFields = {};
  
  // Check if SSH tunnel is enabled
  const sshTunnelEnabled = transformed.ssh_tunnel_enabled;
  
  if (sshTunnelEnabled) {
    // Collect SSH tunnel fields
    if (transformed.ssh_tunnel_host) {
      sshTunnelFields.ssh_host = transformed.ssh_tunnel_host;
    }
    if (transformed.ssh_tunnel_port !== undefined && transformed.ssh_tunnel_port !== null) {
      sshTunnelFields.ssh_port = transformed.ssh_tunnel_port;
    }
    if (transformed.ssh_tunnel_username) {
      sshTunnelFields.ssh_username = transformed.ssh_tunnel_username;
    }
    // Handle private key path - read the file if path is provided
    if (transformed.ssh_tunnel_private_key_path) {
      // Store the file path - the backend will read the file from this path
      sshTunnelFields.ssh_private_key_path = transformed.ssh_tunnel_private_key_path;
      // Also set ssh_private_key for backward compatibility (backend expects this)
      sshTunnelFields.ssh_private_key = transformed.ssh_tunnel_private_key_path;
    } else if (transformed.ssh_tunnel_private_keyFile) {
      // Handle old file upload field (base64 content)
      sshTunnelFields.ssh_private_key = transformed.ssh_tunnel_private_keyFile;
    } else if (transformed.ssh_tunnel_private_key) {
      // Backward compatibility with old field name (direct key content)
      sshTunnelFields.ssh_private_key = transformed.ssh_tunnel_private_key;
    }
    if (transformed.ssh_tunnel_passphrase) {
      sshTunnelFields.ssh_passphrase = transformed.ssh_tunnel_passphrase;
    }
    if (transformed.ssh_tunnel_password) {
      sshTunnelFields.ssh_password = transformed.ssh_tunnel_password;
    }
    
    // Only create ssh_tunnel object if we have at least host and username
    if (sshTunnelFields.ssh_host && sshTunnelFields.ssh_username) {
      transformed.ssh_tunnel = sshTunnelFields;
    }
  }
  
  // Remove flat ssh_tunnel_* fields
  delete transformed.ssh_tunnel_enabled;
  delete transformed.ssh_tunnel_host;
  delete transformed.ssh_tunnel_port;
  delete transformed.ssh_tunnel_username;
  delete transformed.ssh_tunnel_private_key;
  delete transformed.ssh_tunnel_private_keyFile;
  delete transformed.ssh_tunnel_private_key_path;
  delete transformed.ssh_tunnel_passphrase;
  delete transformed.ssh_tunnel_password;
  
  return transformed;
}

function updateTargetWithValues(target, values) {
  target.name = values.name;
  
  // Transform SSH tunnel fields if present
  const transformedValues = transformSSHTunnelFields(values);
  
  Object.keys(transformedValues).forEach(key => {
    if (key !== "name") {
      target.options[key] = transformedValues[key];
    }
  });
  
  // If SSH tunnel was disabled, make sure to remove it
  if (!values.ssh_tunnel_enabled && target.options.ssh_tunnel) {
    delete target.options.ssh_tunnel;
  }
}

function getBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.readAsDataURL(file);
    reader.onload = () => resolve(reader.result.substr(reader.result.indexOf(",") + 1));
    reader.onerror = error => reject(error);
  });
}

function hasFilledExtraField(type, target) {
  const extraOptions = get(type, "configuration_schema.extra_options", []);
  return some(extraOptions, optionName => {
    const defaultOptionValue = get(type, ["configuration_schema", "properties", optionName, "default"]);
    const targetOptionValue = get(target, ["options", optionName]);
    return !isNil(targetOptionValue) && targetOptionValue !== defaultOptionValue;
  });
}

export default {
  getFields,
  updateTargetWithValues,
  getBase64,
  hasFilledExtraField,
};
