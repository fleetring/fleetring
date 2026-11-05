# The `service do` block for the fleetringd Homebrew formula (FR-060), so that
# `brew services start fleetringd` runs a dev mode node (ADR-0039) at login, as the user.
#
# Homebrew turns it into ~/Library/LaunchAgents/homebrew.mxcl.fleetringd.plist, which the file
# homebrew.mxcl.fleetringd.plist beside this one shows. The node keeps everything in ~/.fleetring;
# load ~/.fleetring/env in a shell to point the commands at it.
service do
  run [opt_bin/"fleetringd", "dev"]
  keep_alive true
  log_path var/"log/fleetringd.log"
  error_log_path var/"log/fleetringd.log"
end
