import React from "react";
import Input from "antd/lib/input";
import Button from "antd/lib/button";
import FolderOpenOutlinedIcon from "@ant-design/icons/FolderOpenOutlined";

const { TextArea } = Input;

export default function FilePathField({ form, field, ...otherProps }) {
  const { name } = field;
  const { setFieldsValue } = form;

  const handleFileSelect = (e) => {
    const file = e.target.files[0];
    if (file) {
      // Try to read the file content (for SSH keys, this is more useful than the path)
      const reader = new FileReader();
      reader.onload = (event) => {
        const content = event.target.result;
        // If the content looks like a private key, use it directly
        if (content.trim().startsWith('-----BEGIN')) {
          setFieldsValue({ [name]: content });
        } else {
          // Otherwise, try to get the file path
          let filePath = "";
          try {
            // Method 1: Check if file.path exists (works in Electron, some Node.js environments)
            if (file.path && typeof file.path === "string") {
              filePath = file.path;
            }
            // Method 2: Check webkitRelativePath (for directory uploads)
            else if (file.webkitRelativePath) {
              filePath = file.webkitRelativePath;
            }
            // Method 3: Try to access the file's fullPath (non-standard, some browsers)
            else if (file.fullPath) {
              filePath = file.fullPath;
            }
            // Method 4: Try to get path from the input element value
            if (!filePath && e.target.value) {
              const inputValue = e.target.value;
              // Check if it's a fake path (contains fakepath) - extract filename
              if (inputValue.includes("fakepath")) {
                filePath = inputValue.replace(/^.*[/\\]/, "");
              } else {
                filePath = inputValue;
              }
            }
            // Fallback: Use filename only
            if (!filePath) {
              filePath = file.name;
            }
          } catch (err) {
            filePath = file.name;
          }
          setFieldsValue({ [name]: filePath });
        }
      };
      reader.onerror = () => {
        // If reading fails, just use the filename
        setFieldsValue({ [name]: file.name });
      };
      reader.readAsText(file);
    }
    // Reset the input so the same file can be selected again
    e.target.value = "";
  };

  return (
    <Input.Group compact>
      <TextArea
        {...otherProps}
        name={name}
        style={{ width: "calc(100% - 100px)" }}
        placeholder="Enter server file path (e.g., /home/user/.ssh/id_rsa) OR paste private key content (starts with -----BEGIN)"
        allowClear
        rows={3}
        autoSize={{ minRows: 2, maxRows: 6 }}
      />
      <input
        type="file"
        id={`${name}_file_input`}
        style={{ display: "none" }}
        onChange={handleFileSelect}
        accept=".pem,.key,.rsa,.id_rsa,.*"
      />
      <Button
        onClick={() => document.getElementById(`${name}_file_input`).click()}
        style={{ width: "100px" }}
        title="Select file to get filename (browsers only show filename, edit to add full server path)"
      >
        <FolderOpenOutlinedIcon /> Browse
      </Button>
    </Input.Group>
  );
}
