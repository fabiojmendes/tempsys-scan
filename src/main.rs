use std::{
    collections::HashMap,
    fmt::Display,
    fs,
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::bail;
use bluer::{AdapterEvent, DiscoveryFilter, Session};
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use futures::StreamExt;
use log::Level;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::Deserialize;
use tokio::task;

const TEMPSYS_MANUF_ID: u16 = 0xffff;

#[derive(Deserialize, Debug)]
struct MqttConfig {
    id: String,
    host: String,
    port: u16,
    username: String,
    password: String,
    topic: String,
}

#[derive(Deserialize, Debug)]
struct DeviceConfig {
    addr: String,
    name: String,
    device_type: DeviceType,
}

#[derive(Deserialize, Debug)]
enum DeviceType {
    Fridge,
    Freezer,
    Room,
    Unknown,
}

impl Display for DeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Fridge => "fridge",
            Self::Freezer => "freezer",
            Self::Room => "room",
            Self::Unknown => "unknown",
        };
        write!(f, "{name}")
    }
}

#[derive(Deserialize, Debug)]
struct TempsysScanConfig {
    mqtt: MqttConfig,
    devices: Vec<DeviceConfig>,
}

struct TempReading {
    version: u8,
    counter: u8,
    voltage: u16,
    temp: i16,
}

impl TempReading {
    fn temperature(&self) -> Result<f32, String> {
        if self.temp == i16::MAX {
            return Err(format!("Invalid temperature {}", self.temp));
        }
        Ok(f32::from(self.temp) / 100.0)
    }
}

impl TryFrom<&[u8]> for TempReading {
    type Error = &'static str;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 6 {
            return Err("Buffer is too small");
        }
        let version = data[0];
        let counter = data[1];
        let voltage = LittleEndian::read_u16(&data[2..4]);
        let temp = LittleEndian::read_i16(&data[4..]);

        Ok(TempReading {
            version,
            counter,
            voltage,
            temp,
        })
    }
}

fn timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .format(|buf, record| {
            let priority = match record.level() {
                Level::Trace => 7,
                Level::Debug => 7,
                Level::Info => 6,
                Level::Warn => 4,
                Level::Error => 3,
            };
            writeln!(buf, "<{}>[{}]: {}", priority, record.level(), record.args())
        })
        .init();

    log::info!(
        "Tempsys scan version {}, built for {} by {}.",
        built_info::PKG_VERSION,
        built_info::TARGET,
        built_info::RUSTC_VERSION
    );
    if let (Some(version), Some(hash), Some(dirty)) = (
        built_info::GIT_VERSION,
        built_info::GIT_COMMIT_HASH_SHORT,
        built_info::GIT_DIRTY,
    ) {
        log::info!("Git version: {version} ({hash})");
        if dirty {
            log::warn!("Repo was dirty");
        }
    }

    let config_file = fs::read_to_string("/etc/tempsys/config.toml")?;
    let config: TempsysScanConfig = toml::from_str(&config_file)?;

    let addr_map: HashMap<_, _> = config
        .devices
        .into_iter()
        .map(|d| (d.addr.clone(), d))
        .collect();

    let mqtt = config.mqtt;
    let mut opts = MqttOptions::new(mqtt.id, mqtt.host, mqtt.port);
    opts.set_credentials(mqtt.username, mqtt.password);

    let (client, mut eventloop) = AsyncClient::new(opts, 10);

    let session = Session::new().await?;
    let adapter = session.default_adapter().await?;
    log::info!(
        "Discovering devices using Bluetooth adapater {}",
        adapter.name()
    );
    adapter
        .set_discovery_filter(DiscoveryFilter {
            duplicate_data: false,
            ..Default::default()
        })
        .await?;
    adapter.set_powered(true).await?;

    let bt_handle = task::spawn(async move {
        let mut device_events = adapter.discover_devices_with_changes().await?;
        while let Some(evt) = device_events.next().await {
            if let AdapterEvent::DeviceAdded(addr) = evt {
                let device = adapter.device(addr)?;
                log::trace!("Device: {:?} name: {:?}", device, device.name().await?);
                let rssi = device.rssi().await?.unwrap_or(0);
                let timestamp = timestamp_nanos();
                let addr_string = addr.to_string();
                let unknown_device = DeviceConfig {
                    addr: addr_string.clone(),
                    name: addr_string.clone(),
                    device_type: DeviceType::Unknown,
                };
                let sender = addr_map.get(&addr_string).unwrap_or(&unknown_device);

                match device.manufacturer_data().await? {
                    Some(mut mfg) => {
                        if let Some(data) = mfg.remove(&TEMPSYS_MANUF_ID) {
                            match TempReading::try_from(&data[..]) {
                                Ok(reading) => {
                                    let payload = match reading.temperature() {
                                        Ok(temp) => {
                                            format!("sensor,addr={},name={},type={},version={} temperature={:.2},voltage={},rssi={} {}",
                                                sender.addr, sender.name, sender.device_type, reading.version, temp, reading.voltage, rssi, timestamp)
                                        }
                                        Err(e) => {
                                            log::warn!("Error parsing temperature: {e}");
                                            format!("sensor,addr={},name={},type={},version={} voltage={},rssi={} {}",
                                                sender.addr, sender.name, sender.device_type, reading.version, reading.voltage, rssi, timestamp)
                                        }
                                    };
                                    log::info!("{} (counter={})", payload, reading.counter);
                                    client
                                        .publish(&mqtt.topic, QoS::AtLeastOnce, false, payload)
                                        .await
                                        .unwrap();
                                }
                                Err(e) => {
                                    log::error!("Error parsing the reading {e}");
                                }
                            }
                        }

                        if let Some(data) = mfg.remove(&0xEC88) {
                            // Data conversion reference:
                            // https://github.com/Heckie75/govee-h5075-thermo-hygrometer/blob/main/API.md#decode-temperature-and-humitity
                            log::debug!("Groove Data (hex): {data:02x?}");
                            let encoded = BigEndian::read_u32(&data[0..4]);
                            let temperature = if encoded & 0x800000 == 0 {
                                encoded as f32 / 10000.0
                            } else {
                                0.0 - (encoded ^ 0x800000) as f32 / 10000.0
                            };
                            let humidity = ((encoded & 0x7fffff) % 1000) as f32 / 10.0;
                            let battery = data[4];
                            log::debug!(
                                "Temp: {temperature}, Humidity: {humidity}, Battery: {battery}%",
                            );
                            let payload = format!("sensor,addr={},name={},type={} temperature={:.2},humidity={},battery={},rssi={} {}",
                                            sender.addr, sender.name, sender.device_type, temperature, humidity,battery, rssi, timestamp);
                            log::info!("{payload}");
                            client
                                .publish(&mqtt.topic, QoS::AtLeastOnce, false, payload)
                                .await
                                .unwrap();
                        }
                    }
                    None => {
                        log::trace!("Manufacurer data not found");
                    }
                }
            }
        }
        log::warn!("Bluetooth task exited");
        Ok::<(), bluer::Error>(())
    });

    loop {
        match eventloop.poll().await {
            Ok(notification) => log::debug!("Received: {notification:?}"),
            Err(e) => {
                log::error!("Error on mqtt: {e}");
                bail!(e);
            }
        }
        if bt_handle.is_finished() {
            bail!("Bluetooth task has returned");
        }
    }
}

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
