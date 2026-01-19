import React from "react";
import ReactDOM from "react-dom";

import "@/config";

import ApplicationArea from "@/components/ApplicationArea";
import offlineListener from "@/services/offline-listener";

// Suppress ResizeObserver loop error - this is a known browser issue that's harmless
// See: https://github.com/WICG/resize-observer/issues/38
const resizeObserverErr = window.onerror;
window.onerror = (message, ...args) => {
  if (message && message.includes && message.includes("ResizeObserver loop")) {
    return true; // Suppress the error
  }
  return resizeObserverErr ? resizeObserverErr(message, ...args) : false;
};

// Also handle unhandled promise rejections for ResizeObserver
window.addEventListener("error", (event) => {
  if (event.message && event.message.includes("ResizeObserver loop")) {
    event.stopPropagation();
    event.preventDefault();
  }
});

ReactDOM.render(<ApplicationArea />, document.getElementById("application-root"), () => {
  offlineListener.init();
});
