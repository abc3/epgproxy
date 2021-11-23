dev:
	MIX_ENV=dev ERL_AFLAGS="-kernel shell_history enabled" iex -S mix

bench_simple:
	pgbench -M simple -h localhost -p 5555 -U postgres -j 4 -c 1

bench_ext:
	pgbench -M extended -h localhost -p 5555 -U postgres -j 4 -c 1

bench_pb:
	pgbench -M extended -h localhost -p 6432 -U postgres -j 4 -c 1
