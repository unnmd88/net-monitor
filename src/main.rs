mod config;
use serde_json::json;
mod constants;
mod event_loop;
mod icmp;
mod logging;
mod models;
mod poller;
mod sender;
mod snmp;
mod traits;
mod utils;

use std::{net::IpAddr, task::Poll, time::Duration};
use tokio::sync::mpsc::{self};
use tracing::{error, info};

use crate::{
    config::Config,
    icmp::{IcmpProvider, TracertProvider},
    models::{
        ConfigStrategyDetails,
        Event,
        IndependentPollerConfig,
        PollType,
        Strategy, //SynchronizedProviderConfig,
    },
    poller::{IndependentPoller, PollTimings, SynchronizedPoller},
    sender::{EventSender, JsonSender},
    snmp::SnmpProvider,
    traits::Pollable,
    utils::{get_session_id, get_timestamp_fmt},
};

type Senders = Vec<Box<dyn EventSender + Send>>;

const CONFIG_ERROR_MSG: &str = "Ошибка конфигурации.\n";
const CONFIG_NAME: &str = "config.toml";
const INIT_SUCCESSFULLY: &str = "init successfully.";
const INIT_FAILED: &str = "init failed.";
const ICMP_PROVIDER: &str = "IcmpProvider";
const SNMP_PROVIDER: &str = "SnmpProvider";
const ERROR_READING_CONFIG: &str = "Error reading configuration file";
const ERROR_INIT_ICMP: &str = "❌ Ошибка инициализации ICMP";
const ERROR_INIT_SNMP: &str = "❌ Ошибка инициализации SNMP";

fn load_config(path: &str) -> Result<Config, String> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        error!("{ERROR_READING_CONFIG} '{path}': {e}");
        format!("❌ Не удалось прочитать '{path}': {}", e)
    })?;

    let config = toml::from_str(&contents).map_err(|e| {
        error!("{ERROR_READING_CONFIG} '{path}': {e}");
        format!("❌ Ошибка в {CONFIG_NAME}: {}", user_friendly_error(&e))
    })?;
    info!("Config {path} loaded successfully.");

    Ok(config)
}

fn user_friendly_error(e: &toml::de::Error) -> String {
    let msg = e.message();

    if msg.contains("missing field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!("отсутствует поле '{}'", field);
        }
        return "отсутствует обязательное поле".to_string();
    }

    if msg.contains("unknown field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!(
                "неизвестное поле '{}' — проверьте название",
                field
            );
        }
    }

    if msg.contains("invalid type") {
        return "неверный тип значения (например, строка вместо числа)"
            .to_string();
    }

    msg.to_string()
}

/*
fn init_tracert(config: &Config) -> Result<Option<TracertProvider>, String> {
    let tracert = if config.ping.fallback_tracert {
        match TracertProvider::new(
            config.network.target,
            config.tracert.max_hops,
            config.tracert.queries_per_hop,
        ) {
            Ok(t) => Some(t),
            Err(e) => {
                error!("Tracert {}: {}", INIT_FAILED, e);
                return Err(e);
            }
        }
    } else {
        None
    };
    Ok(tracert)
}
*/

async fn init_icmp_provider(config: &Config) -> Result<IcmpProvider, String> {
    //let tracert = init_tracert(&config)?;
    let timeout_ms = match config.strategy {
        Strategy::Independent => config.independent.ping.timeout_ms,
        Strategy::Synchronized => config.synchronized.ping.timeout_ms,
    };

    let icmp_provider =
        match IcmpProvider::new(config.network.target, timeout_ms, None) {
            Ok(provider) => provider,
            Err(e) => {
                let err = format!("{ICMP_PROVIDER} {INIT_FAILED}: {e}");
                error!("{}", err);
                return Err(err);
            }
        };

    info!("{ICMP_PROVIDER} {INIT_SUCCESSFULLY}");
    Ok(icmp_provider)
}

async fn init_snmp_provider(config: &Config) -> Result<SnmpProvider, String> {
    let (port, community, oids, timeout_ms) = match config.strategy {
        Strategy::Independent => (
            config.independent.snmp.port,
            config
                .independent
                .snmp
                .community
                .clone(),
            config.independent.snmp.oids.clone(),
            config.independent.snmp.timeout_ms,
        ),
        Strategy::Synchronized => (
            config.synchronized.snmp.port,
            config
                .synchronized
                .snmp
                .community
                .clone(),
            config.synchronized.snmp.oids.clone(),
            config.synchronized.snmp.timeout_ms,
        ),
    };

    let snmp_provider = match SnmpProvider::new(
        config.network.target,
        port,
        community,
        oids,
        timeout_ms,
    )
    .await
    {
        Ok(provider) => provider,
        Err(e) => {
            let err = format!("{SNMP_PROVIDER} {INIT_FAILED}: {e}");
            error!("{}", err);
            return Err(err);
        }
    };
    info!("{SNMP_PROVIDER} {INIT_SUCCESSFULLY}.");
    Ok(snmp_provider)
}

fn create_independent_poller(
    provider: Box<dyn Pollable>,
    config: &Config,
    tx: mpsc::Sender<Event>,
) -> IndependentPoller {
    let provider_whoami = provider.whoami();
    let poller_config = match provider_whoami {
        PollType::Ping => PollTimings {
            interval_ms: config.independent.ping.interval_ms,
            retries: config.independent.ping.retries,
            retries_delay_ms: config.independent.ping.retries_delay_ms,
        },
        PollType::Snmp => PollTimings {
            interval_ms: config.independent.snmp.interval_ms,
            retries: config.independent.snmp.retries,
            retries_delay_ms: config.independent.snmp.retries_delay_ms,
        },
    };

    let poller = IndependentPoller::new(provider, poller_config, tx);
    info!(
        "IndependentPoller created successfully. Provider: {provider_whoami}"
    );
    poller
}

fn spawn_independent_poll(
    poller: IndependentPoller,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(poller.run())
}

fn spawn_synchronized_poll(
    poller: SynchronizedPoller,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(poller.run())
}

#[tokio::main]
async fn main() {
    // Логгирование
    let _guard = logging::init_tracing();
    info!("{} Setup new monitor... {}", "#".repeat(40), "#".repeat(40));
    if let Err(user_err_message) = app().await {
        error!("Setup monitor stopped.");
        eprintln!("{user_err_message}");
        std::process::exit(1);
    }
}

async fn app() -> Result<(), String> {
    println!("Настраиваю монитор опроса...");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let session_id = get_session_id();
    info!("Session id={session_id}.");

    // Конфиг опроса
    let config = load_config(CONFIG_NAME)?;

    let json_sender =
        match JsonSender::new(&config.log, session_id.clone()).await {
            Ok(sender) => {
                info!("JsonSender create successfully.");
                sender
            }
            Err(e) => {
                error!("Failed to create JsonSender: {e}");
                return Err(CONFIG_ERROR_MSG.to_string());
            }
        };

    let senders: Senders = vec![Box::new(json_sender)];

    // Канал, которые принимает структуры PollEvent и отправляет их различным senders.
    // tx -> передатчик структур PollEvent в канал, rx -> приёмник структур PollEvent.
    let (tx, rx) = mpsc::channel::<Event>(256);
    tokio::spawn(event_loop::handle_events(rx, senders));

    let mut spawned_tasks = Vec::new();
    let current_strategy = config.strategy;
    println!("Выбрана стратегия мониторинга: {current_strategy}");
    info!("Setup {:?} strategy.", current_strategy);
    tokio::time::sleep(Duration::from_millis(200)).await;

    match current_strategy {
        Strategy::Independent => {
            let mut providers: Vec<Box<dyn Pollable>> = Vec::new();

            if config.independent.ping.enabled {
                providers.push(Box::new(init_icmp_provider(&config).await?));
            }
            if config.independent.snmp.enabled {
                providers.push(Box::new(init_snmp_provider(&config).await?));
            }

            let pollers: Vec<IndependentPoller> = providers
                .into_iter()
                .map(|p| create_independent_poller(p, &config, tx.clone()))
                .collect();

            let config_event = Event::Config {
                strategy: Strategy::Independent,
                details: ConfigStrategyDetails::Independent {
                    pollers: pollers
                        .iter()
                        .map(|poller| poller.dump())
                        .collect(),
                },
            };
            tx.send(config_event)
                .await
                .map_err(|e| e.to_string())?;

            for p in pollers {
                spawned_tasks.push(spawn_independent_poll(p));
            }
        }

        Strategy::Synchronized => {
            /*
                        let mut providers: Vec<Box<dyn Pollable>> = Vec::new();

                        for poll_type in &config.synchronized.jobs {
                            let provider: Box<dyn Pollable> = match poll_type {
                                PollType::Ping => {
                                    Box::new(init_icmp_provider(&config).await?)
                                }

                                PollType::Snmp => {
                                    Box::new(init_snmp_provider(&config).await?)
                                }
                            };
                            info!(
                                "Added {} provider for {} strategy.",
                                provider.whoami(),
                                &current_strategy
                            );

                            providers.push(provider);
                        }

                        let poller_config = IndependentPollerTimings {
                            interval_ms: config.synchronized.interval_ms,
                            retries: config.synchronized.retries,
                            retries_delay_ms: config.synchronized.retries_delay_ms,
                        };

                        let poller =
                            SynchronizedPoller::new(providers, poller_config, tx.clone());

                        info!("{} strtegy poller created successfully.", current_strategy);

                        let config_event = Event::Config {
                            strategy: current_strategy,
                            details: ConfigStrategyDetails::Synchronized {
                                data: poller.dump(),
                            },
                        };

                        tx.send(config_event)
                            .await
                            .map_err(|e| e.to_string())?;

                        spawned_tasks.push(spawn_synchronized_poll(poller));
            */
        }
    }

    if spawned_tasks.is_empty() {
        return Err("Нет задач для опроса. Монитор не запущен.".to_string());
    }
    let cnt_tasks = spawned_tasks.len();
    info!("Number of polling tasks: {cnt_tasks}");
    println!("Количество опросов: {cnt_tasks}");
    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("Monitor started.");
    println!("Монитор запущен.");
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("Файл для записи лога: {}", &config.log);

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("Нажмите Ctrl-C для остановки монитора.");
    tokio::time::sleep(Duration::from_millis(100)).await;

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutting down (Ctrl+C received)...");
            Ok(())
        }
        Err(e) => {
            error!("Failed to listen for Ctrl+C: {}", e);
            Err(format!("Ошибка системы: {}", e))
        }
    }
}
