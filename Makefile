dev:
	mix deps.get && MIX_ENV=dev ERL_AFLAGS="-kernel shell_history enabled" iex -S mix
