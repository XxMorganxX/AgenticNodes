import { useEffect, useRef, useState } from "react";
import type { MouseEvent } from "react";

import { preflightDiscordToken } from "../lib/api";
import type { DiscordTokenPreflightResult } from "../lib/types";

export const DISCORD_BOT_TOKEN_ENV_KEY = "DISCORD_BOT_TOKEN";
const DISCORD_BOT_TOKEN_ENV_REF = `{${DISCORD_BOT_TOKEN_ENV_KEY}}`;

type DiscordTriggerModalProps = {
  botToken: string;
  channelId: string;
  ignoreBotMessages: boolean;
  ignoreSelfMessages: boolean;
  graphEnvVars: Record<string, string>;
  onChangeBotToken: (value: string) => void;
  onChangeChannelId: (value: string) => void;
  onChangeIgnoreBotMessages: (value: boolean) => void;
  onChangeIgnoreSelfMessages: (value: boolean) => void;
  onClose: () => void;
};

export function DiscordTriggerModal({
  botToken,
  channelId,
  ignoreBotMessages,
  ignoreSelfMessages,
  graphEnvVars,
  onChangeBotToken,
  onChangeChannelId,
  onChangeIgnoreBotMessages,
  onChangeIgnoreSelfMessages,
  onClose,
}: DiscordTriggerModalProps) {
  const [preflight, setPreflight] = useState<DiscordTokenPreflightResult | null>(null);
  const [preflightError, setPreflightError] = useState<string | null>(null);
  const [isCheckingToken, setIsCheckingToken] = useState(false);
  const requestIdRef = useRef(0);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  useEffect(() => {
    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    setIsCheckingToken(true);
    setPreflightError(null);
    const handle = window.setTimeout(() => {
      preflightDiscordToken({
        env_var_name: DISCORD_BOT_TOKEN_ENV_REF,
        graph_env_vars: graphEnvVars,
      })
        .then((result) => {
          if (requestIdRef.current !== requestId) {
            return;
          }
          setPreflight(result);
          setIsCheckingToken(false);
        })
        .catch((error: Error) => {
          if (requestIdRef.current !== requestId) {
            return;
          }
          setPreflight(null);
          setPreflightError(error.message);
          setIsCheckingToken(false);
        });
    }, 250);
    return () => window.clearTimeout(handle);
  }, [botToken, graphEnvVars]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal supabase-auth-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="discord-trigger-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Trigger</div>
            <h3 id="discord-trigger-modal-title">Discord Message Trigger</h3>
            <p>
              The token is stored in this graph's env vars under <code>{DISCORD_BOT_TOKEN_ENV_KEY}</code>. Setting{" "}
              <code>{DISCORD_BOT_TOKEN_ENV_KEY}</code> in your <code>.env</code> overrides this value at runtime.
            </p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body supabase-auth-modal-body">
          <div className="supabase-auth-modal-grid">
            <label>
              Discord Bot Token
              <input
                type="password"
                autoComplete="off"
                spellCheck={false}
                value={botToken}
                placeholder="Paste bot token"
                onChange={(event) => onChangeBotToken(event.target.value)}
              />
            </label>
            <label>
              Discord Channel ID
              <input
                type="text"
                value={channelId}
                placeholder="123456789012345678"
                onChange={(event) => onChangeChannelId(event.target.value)}
              />
            </label>
          </div>

          <DiscordTokenStatus
            preflight={preflight}
            preflightError={preflightError}
            isChecking={isCheckingToken}
          />

          <label className="checkbox-option">
            <input
              type="checkbox"
              checked={ignoreBotMessages}
              onChange={(event) => onChangeIgnoreBotMessages(event.target.checked)}
            />
            <span>Ignore bot-authored messages</span>
          </label>
          <label className="checkbox-option">
            <input
              type="checkbox"
              checked={ignoreSelfMessages}
              onChange={(event) => onChangeIgnoreSelfMessages(event.target.checked)}
            />
            <span>Ignore this bot's own messages</span>
          </label>
        </div>

        <div className="tool-details-modal-footer">
          <button type="button" className="primary-button" onClick={onClose}>
            Done
          </button>
        </div>
      </section>
    </div>
  );
}

function DiscordTokenStatus({
  preflight,
  preflightError,
  isChecking,
}: {
  preflight: DiscordTokenPreflightResult | null;
  preflightError: string | null;
  isChecking: boolean;
}) {
  if (preflightError) {
    return (
      <div className="discord-token-status discord-token-status--warning" role="status">
        Could not check token: {preflightError}
      </div>
    );
  }
  if (isChecking && !preflight) {
    return (
      <div className="discord-token-status discord-token-status--neutral" role="status">
        Checking token resolution…
      </div>
    );
  }
  if (!preflight) {
    return null;
  }
  if (preflight.token_resolved) {
    const sourceLabel =
      preflight.source === "process_env"
        ? "process environment (.env)"
        : "graph env vars";
    return (
      <div className="discord-token-status discord-token-status--ok" role="status">
        Token detected — source: {sourceLabel}
        {isChecking ? " (rechecking…)" : ""}
      </div>
    );
  }
  return (
    <div className="discord-token-status discord-token-status--warning" role="status">
      Token not detected. Paste the bot token above, or set <code>{DISCORD_BOT_TOKEN_ENV_KEY}</code> in your <code>.env</code>{" "}
      file.
      {isChecking ? " (rechecking…)" : ""}
    </div>
  );
}
