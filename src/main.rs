mod config;
use clap::{Parser, builder::Str};
use serde_json::json;
mod cli;
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
    cli::{Cli, Command},
    config::{
        Config, IndependentPingInstance, IndependentSnmpInstance,
        IndependentTracertInstance,
    },
    icmp::{IcmpProvider, TracertProvider},
    models::{
        ConfigStrategyDetails,
        Event,
        IndependentPollerConfig,
        PollType,
        Strategy, //SynchronizedProviderConfig,
    },
    poller::{IndependentPoller, PollConfig, SynchronizedPoller},
    sender::{EventSender, JsonSender},
    snmp::SnmpProvider,
    traits::Pollable,
    utils::{get_session_id, get_timestamp_fmt},
};

type Senders = Vec<Box<dyn EventSender + Send>>;

const CONFIG_ERROR_MSG: &str = "Ошибка конфигурации.\n";
// const CONFIG_NAME: &str = "config.toml";
const INIT_SUCCESSFULLY: &str = "init successfully.";
const INIT_FAILED: &str = "init failed.";
const ICMP_PROVIDER: &str = "IcmpProvider";
const SNMP_PROVIDER: &str = "SnmpProvider";
const ERROR_READING_CONFIG: &str = "Error reading configuration file";
const ERROR_INIT_ICMP: &str = "❌ Ошибка инициализации ICMP";
const ERROR_INIT_SNMP: &str = "❌ Ошибка инициализации SNMP";

/*
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
*/

async fn init_icmp_provider(
    username: String,
    target: IpAddr,
    timeout_ms: u64,
) -> Result<IcmpProvider, String> {
    //let tracert = init_tracert(&config)?;

    let icmp_provider =
        match IcmpProvider::new(username, target, timeout_ms, None) {
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

async fn init_snmp_provider(
    username: String,
    target: IpAddr,
    timeout_ms: u64,
    port: u16,
    community: String,
    oids: Vec<String>,
) -> Result<SnmpProvider, String> {
    let snmp_provider = match SnmpProvider::new(
        username, target, port, community, oids, timeout_ms,
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
    interval_ms: u64,
    poll_config: PollConfig,
    tx: mpsc::Sender<Event>,
) -> IndependentPoller {
    let provider_whoami = provider.whoami();
    let poller = IndependentPoller::new(provider, interval_ms, poll_config, tx);
    info!(
        "IndependentPoller created successfully. Provider: {provider_whoami}",
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
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Analyze { log_file, detailed }) => {
            // Анализ логов
            // TODO
        }
        Some(Command::GenerateConfig {
            output,
            force,
            show,
        }) => {
            if let Err(e) = Config::generate_default(&output, force, show) {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        }
        None => {
            // Запускаем опросник по умолчанию
            let _guard = logging::init_tracing();
            info!("{} Setup new monitor... {}", "#".repeat(40), "#".repeat(40));
            println!("Настраиваю монитор опроса...");
            let session_id = get_session_id();
            info!("Session id={session_id}.");

            tokio::time::sleep(Duration::from_millis(800)).await;

            let config = match Config::from_file(&cli.config) {
                Ok(c) => c,
                Err(e) => {
                    error!("Setup monitor stopped: {}", e);
                    eprintln!("{}", e);
                    std::process::exit(1);
                }
            };

            if let Err(user_err_message) = app(config, session_id).await {
                error!("Setup monitor stopped.");
                eprintln!("{}", user_err_message);
                std::process::exit(1);
            }
        }
    }
}

async fn app(config: Config, session_id: String) -> Result<(), String> {
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
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut spawned_tasks = Vec::new();
    let current_strategy = config.strategy;
    println!("Выбрана стратегия мониторинга: {current_strategy}");
    info!("Setup {:?} strategy.", current_strategy);
    tokio::time::sleep(Duration::from_millis(200)).await;

    match current_strategy {
        Strategy::Independent => {
            // let mut providers: Vec<Box<dyn Pollable>> = Vec::new();
            let mut pollers: Vec<IndependentPoller> = Vec::new();

            for ping_instance in config.independent.ping {
                let provider = init_icmp_provider(
                    ping_instance.name,
                    config.network.target,
                    ping_instance.timeout_ms,
                )
                .await?;
                let poll_config = PollConfig {
                    retries: ping_instance.retries,
                    retries_delay_ms: ping_instance.retries_delay_ms,
                };
                let poller = create_independent_poller(
                    Box::new(provider),
                    ping_instance.interval_ms,
                    poll_config,
                    tx.clone(),
                );
                pollers.push(poller);
            }

            for snmp_instance in config.independent.snmp {
                let provider = init_snmp_provider(
                    snmp_instance.name,
                    config.network.target,
                    snmp_instance.timeout_ms,
                    snmp_instance.port,
                    snmp_instance.community.clone(),
                    snmp_instance.oids.clone(),
                )
                .await?;
                let timings = PollConfig {
                    retries: snmp_instance.retries,
                    retries_delay_ms: snmp_instance.retries_delay_ms,
                };

                let poller = create_independent_poller(
                    Box::new(provider),
                    snmp_instance.interval_ms,
                    timings,
                    tx.clone(),
                );
                pollers.push(poller);
            }

            let config_event = Event::Config {
                strategy: Strategy::Independent,
                details: ConfigStrategyDetails::Independent {
                    num_pollers: pollers.len() as u8,
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
            let mut providers: Vec<(Box<dyn Pollable>, PollConfig)> =
                Vec::new();

            for ping_instance in config.synchronized.ping {
                let provider = init_icmp_provider(
                    ping_instance.name,
                    config.network.target,
                    ping_instance.timeout_ms,
                )
                .await?;
                let poll_config = PollConfig {
                    retries: ping_instance.retries,
                    retries_delay_ms: ping_instance.retries_delay_ms,
                };
                let provider_whoami = provider.whoami();
                providers.push((Box::new(provider), poll_config));
                info!(
                    "Added {} provider for {} strategy.",
                    provider_whoami, &current_strategy
                );
            }

            for snmp_instance in config.synchronized.snmp {
                let provider = init_snmp_provider(
                    snmp_instance.name,
                    config.network.target,
                    snmp_instance.timeout_ms,
                    snmp_instance.port,
                    snmp_instance.community.clone(),
                    snmp_instance.oids.clone(),
                )
                .await?;
                let poll_config = PollConfig {
                    retries: snmp_instance.retries,
                    retries_delay_ms: snmp_instance.retries_delay_ms,
                };
                providers.push((Box::new(provider), poll_config));
            }

            let poller = SynchronizedPoller::new(
                providers,
                config.synchronized.interval_ms,
                tx.clone(),
            );

            info!("{} strtegy poller created successfully.", current_strategy);

            let config_event = Event::Config {
                strategy: current_strategy,
                details: ConfigStrategyDetails::Synchronized {
                    config: poller.dump(),
                },
            };

            tx.send(config_event)
                .await
                .map_err(|e| e.to_string())?;

            spawned_tasks.push(spawn_synchronized_poll(poller));
        }
    }

    if spawned_tasks.is_empty() {
        return Err("Нет задач для опроса. Монитор не запущен.".to_string());
    }
    let cnt_tasks = spawned_tasks.len();
    info!("Number of polling tasks: {cnt_tasks}");
    println!(
        "Файл для записи лога: {}\nКоличество опросов: {}",
        &config.log, cnt_tasks,
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("Monitor started.");
    println!("Монитор запущен.");
    tokio::time::sleep(Duration::from_millis(100)).await;

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
