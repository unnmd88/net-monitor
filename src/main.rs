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

use std::time::Duration;
use tokio::sync::mpsc::{self};
use tracing::{error, info};

use crate::{
    config::Config,
    icmp::{IcmpProvider, TracertProvider},
    models::{
        ConfigStrategyDetails, IndependentProviderConfig, LogEvent, PollEvent,
        PollType, Strategy, SynchronizedProviderConfig,
    },
    poller::{IndependentPoller, SynchronizedPoller},
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

fn init_icmp_provider(config: &Config) -> Result<IcmpProvider, String> {
    let tracert = init_tracert(&config)?;

    let icmp_provider = match IcmpProvider::new(
        config.network.target.clone(),
        config.ping.timeout_seconds,
        tracert,
    ) {
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

fn spawn_icmp_independent_poll(
    icmp_provider: IcmpProvider,
    interval_seconds: u64,
    tx: mpsc::Sender<PollEvent>,
) -> tokio::task::JoinHandle<()> {
    let icmp_poller =
        IndependentPoller::new(icmp_provider, interval_seconds, tx);

    tokio::spawn(icmp_poller.run())
}

async fn init_snmp_provider(config: &Config) -> Result<SnmpProvider, String> {
    let snmp_provider = match SnmpProvider::new(
        config.network.target,
        config.snmp.port,
        config.snmp.community.clone(),
        config.snmp.oids.clone(),
        config.snmp.timeout_seconds,
        config.snmp.retries,
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

fn spawn_snmp_independent_poll(
    snmp_provider: SnmpProvider,
    interval_seconds: u64,
    tx: mpsc::Sender<PollEvent>,
) -> tokio::task::JoinHandle<()> {
    let snmp_provider =
        IndependentPoller::new(snmp_provider, interval_seconds, tx);
    tokio::spawn(snmp_provider.run())
}

#[tokio::main]
async fn main() {
    // Логгирование
    let _guard = logging::init_tracing();
    info!("Setup new monitor...");
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
    let (tx, rx) = mpsc::channel::<PollEvent>(256);
    tokio::spawn(event_loop::handle_events(rx, senders));

    let mut spawned_tasks = Vec::new();
    let current_strategy = config.strategy;
    println!("Выбрана стратегия мониторинга: {current_strategy}");
    info!("Setup {:?} strategy.", current_strategy);
    tokio::time::sleep(Duration::from_millis(200)).await;

    match current_strategy {
        Strategy::Independent => {
            let mut providers = Vec::new();

            let icmp_provider = match config.independent.ping.enabled {
                true => {
                    let icmp = init_icmp_provider(&config)?;
                    providers.push(IndependentProviderConfig {
                        provider: icmp.dump(),
                        interval_seconds: config
                            .independent
                            .ping
                            .interval_seconds,
                    });
                    Some(icmp)
                }
                false => None,
            };
            let snmp_provider = match config.independent.snmp.enabled {
                true => {
                    let provider = init_snmp_provider(&config).await?;
                    providers.push(IndependentProviderConfig {
                        provider: provider.dump(),
                        interval_seconds: config
                            .independent
                            .snmp
                            .interval_seconds,
                    });
                    Some(provider)
                }
                false => None,
            };

            let config_event = LogEvent::Config {
                timestamp: get_timestamp_fmt(),
                session_id: session_id.clone(),
                target: config.network.target,
                strategy: Strategy::Independent,
                details: ConfigStrategyDetails::Independent { providers },
            };
            tx.send(PollEvent {
                step: 0,
                log_event: config_event,
                strategy: Strategy::Independent,
            })
            .await
            .map_err(|e| e.to_string())?;

            if let Some(p) = icmp_provider {
                spawned_tasks.push(spawn_icmp_independent_poll(
                    p,
                    config.independent.ping.interval_seconds,
                    tx.clone(),
                ));
            }
            if let Some(p) = snmp_provider {
                spawned_tasks.push(spawn_snmp_independent_poll(
                    p,
                    config.independent.snmp.interval_seconds,
                    tx.clone(),
                ));
            }
        }

        Strategy::Synchronized => {
            let mut tasks: Vec<Box<dyn Pollable>> = Vec::new();
            let mut providers = Vec::new();

            // --------//
            for task in &config.synchronized.jobs {
                let provider: Box<dyn Pollable> = match task {
                    PollType::Ping => {
                        let icmp_provider = init_icmp_provider(&config)
                            .map_err(|_| ERROR_INIT_ICMP.to_string())?;

                        providers.push(SynchronizedProviderConfig {
                            provider: icmp_provider.dump(),
                        });
                        info!(
                            "Added {} provider for {} strategy.",
                            icmp_provider.get_provider_name(),
                            &current_strategy
                        );
                        Box::new(icmp_provider)
                    }
                    PollType::Snmp => {
                        let snmp_provider =
                            init_snmp_provider(&config)
                                .await
                                .map_err(|_| ERROR_INIT_SNMP.to_string())?;
                        providers.push(SynchronizedProviderConfig {
                            provider: snmp_provider.dump(),
                        });
                        Box::new(snmp_provider)
                    }
                };
                tasks.push(provider);
            }

            let config_event = LogEvent::Config {
                timestamp: get_timestamp_fmt(),
                session_id: session_id.clone(),
                target: config.network.target,
                strategy: Strategy::Synchronized,
                details: ConfigStrategyDetails::Synchronized {
                    interval_seconds: config.synchronized.interval_seconds,
                    providers,
                },
            };
            tx.send(PollEvent {
                step: 0,
                log_event: config_event,
                strategy: Strategy::Synchronized,
            })
            .await
            .map_err(|e| e.to_string())?;

            let poller = SynchronizedPoller::new(
                tasks,
                config.synchronized.interval_seconds,
                tx.clone(),
            );
            spawned_tasks.push(tokio::spawn(poller.run()));
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
