<!-- vim: set tw=80: -->

# Tempsys scan

This repository contains the code for the scanning component of Tempsys. It
reads the advertising events emitted by the
[tempsys-firmware](https://github.com/fabiojmendes/tempsys-firmware) and
publishes them to a MQTT broker.

## Configuration

A TOML configuration file is necessary to provide the location for the MQTT
broker and also friendly names for each Tempsys thermometer. This file should be
named `/etc/tempsys/config.toml` and a sample of what it should look like is as
follows:

```toml
devices = [
  { name = "TestDevice", addr = "AA:BB:CC:11:22:33", device_type = "Fridge" },
]

[mqtt]
id = "tempsys-scan"
host = "localhost"
port = 1883
username = "user"
password = "pass"
topic = "tempsys/temperature"
```

## Build

This code is meant to run on a Raspberry Pi device, so a
[cross](https://github.com/cross-rs/cross) configuration is provided for ease of
use.

To build it locally you need to install the dependencies `libdbus-1-dev` and
`pkg-config`.

## WIP

- Breakdown code in modules
- Read advertising packages from different devices
- Improve error handling
- Support CLI arguments?
