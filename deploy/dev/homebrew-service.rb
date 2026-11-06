# The `service do` block for the fleetring Homebrew formula (FR-060), so that
# `brew services start fleetring` runs a dev mode node (ADR-0039) at login, as the user.
#
# Homebrew turns it into ~/Library/LaunchAgents/homebrew.mxcl.fleetring.plist, which the file
# homebrew.mxcl.fleetring.plist beside this one shows. The node keeps everything in ~/.fleetring,
# and every start writes the `dev` context, which points the commands at it.
# `brew services stop fleetring` stops it; `fleetring dev stop` refuses a node launchd runs.
service do
  run [opt_bin/"fleetring", "dev"]
  keep_alive true
  log_path var/"log/fleetring.log"
  error_log_path var/"log/fleetring.log"
end
