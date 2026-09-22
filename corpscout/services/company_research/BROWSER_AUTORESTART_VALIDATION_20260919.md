# Saved browser auto-restart

Deployed version **0.34.2** to the Linux crawler on 19 September 2026.

Each profile has a persistent **Auto-restart** switch, enabled by default. Closing
its last tab or window schedules one recovery after a one-second delay. The new
browser opens a blank tab, preserving the profile and saved session cookies.
Closing one of multiple tabs leaves the remaining browser unchanged. Both viewers
request a new connection ticket when a replacement saved desktop is discovered.

Explicit Stop, disabling the setting, and service cleanup cancel pending recovery.
A failed restart remains an error requiring manual retry; there is no failure loop.
The systemd unit now uses `KillMode=mixed`, so the main service can cancel recovery
and flush profiles before remaining child processes are terminated. This follows
the upstream [systemd termination documentation](https://github.com/systemd/systemd/blob/main/man/systemd.kill.xml).
The updated unit was validated and reloaded without another browser interruption.

Validation:

- Python suite: **327 tests run, 36 skipped**, no failures.
- Frontend: **17 tests passed**; React Router type generation and TypeScript passed.
- Ruff and targeted Python type checks passed.
- Installed-release Linux/Xvfb test passed: one-tab closure does not restart;
  last-tab closure replaces the browser with one blank tab; cookies and local
  storage survive; disabled recovery remains off; Stop cancels pending recovery;
  desktop inventory cleanup and existing manual-restart behavior still work.
- Backoffice displayed both enabled switches. Turning browser-1 off and back on
  succeeded through the UI. Both profiles were left with auto-restart enabled.

The native closure test uses isolated temporary profiles and a local HTTP fixture,
so it does not close the operator's live browsing tabs.
