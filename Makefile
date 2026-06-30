# EXEC = docker exec -it


# ?= - можно переопределить ✅
# = - нельзя переопределить ❌
# := - нельзя переопределить ❌

#.PHONY: app
#up:
#	${DC} -f ${APP_FILE} ${ENV} up --build -d

ENV_FILE ?= .env.dev

.PHONY: test
test:
	cargo test --target x86_64-unknown-linux-gnu -- --nocapture

.PHONY: check
check:
	cargo check --target x86_64-unknown-linux-gnu

.PHONY: run
run:
	ENV_FILE=$(ENV_FILE) cargo run --target x86_64-unknown-linux-gnu

.PHONY: build-release
build-release:
	cargo build --release --target x86_64-unknown-linux-gnu

.PHONY: build-release-win10
build-release-win10:
	cargo build --release --target x86_64-pc-windows-gnu

.PHONY: build-release-win7
build-release-win7:
	cargo +nightly build --release

.PHONY: objdump
objdump:
	objdump -p target/x86_64-win7-windows-msvc/release/traffic-api.exe | grep "DLL Name"

.PHONY: example-traceroute
example-traceroute:
	cargo build --target x86_64-unknown-linux-gnu --example test_traceroute
	sudo setcap cap_net_raw+ep target/x86_64-unknown-linux-gnu/debug/examples/test_traceroute
	./target/x86_64-unknown-linux-gnu/debug/examples/test_traceroute
