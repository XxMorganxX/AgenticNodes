import { Component } from "react";
import type { ErrorInfo, ReactNode } from "react";

type Props = {
  children: ReactNode;
};

type State = {
  error: Error | null;
};

export class AppErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Surface to console so the failure isn't silent during long-run sessions.
    // eslint-disable-next-line no-console
    console.error("AppErrorBoundary caught a render-time error", error, info);
  }

  private handleReload = (): void => {
    window.location.reload();
  };

  private handleReset = (): void => {
    this.setState({ error: null });
  };

  render(): ReactNode {
    if (!this.state.error) {
      return this.props.children;
    }
    const message = this.state.error.message || "Something broke while rendering the studio.";
    return (
      <div className="app-error-boundary">
        <div className="app-error-boundary__panel">
          <h1 className="app-error-boundary__title">Studio hit a render error</h1>
          <p className="app-error-boundary__message">{message}</p>
          <p className="app-error-boundary__hint">
            This can happen after long agent runs if the UI ran out of memory. Reloading clears the in-memory event buffer; your run state is preserved server-side.
          </p>
          <div className="app-error-boundary__actions">
            <button type="button" className="app-error-boundary__button app-error-boundary__button--primary" onClick={this.handleReload}>
              Reload page
            </button>
            <button type="button" className="app-error-boundary__button" onClick={this.handleReset}>
              Try to recover without reloading
            </button>
          </div>
        </div>
      </div>
    );
  }
}
