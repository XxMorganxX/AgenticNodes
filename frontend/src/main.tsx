import ReactDOM from "react-dom/client";

import App from "./App";
import "./styles.css";

function isBenignResizeObserverMessage(message: string): boolean {
  return message.includes("ResizeObserver loop completed with undelivered notifications");
}

function getErrorMessage(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Error) {
    return value.message;
  }
  if (value && typeof value === "object" && "message" in value && typeof value.message === "string") {
    return value.message;
  }
  return "";
}

window.addEventListener("error", (event) => {
  const message = getErrorMessage(event.error) || event.message;
  if (isBenignResizeObserverMessage(message)) {
    event.preventDefault();
    event.stopImmediatePropagation();
  }
}, true);

window.addEventListener("unhandledrejection", (event) => {
  if (isBenignResizeObserverMessage(getErrorMessage(event.reason))) {
    event.preventDefault();
    event.stopImmediatePropagation();
  }
}, true);

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(<App />);
