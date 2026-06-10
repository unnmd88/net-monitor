mod config;
mod constants;
mod event_loop;
mod icmp;
mod models;
mod poller;
mod sender;
mod snmp;
mod traits;
mod utils;

use ftr::socket::factory;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use tracing_appender;
use tracing_subscriber;
use uuid::Uuid;

use crate::{
    config::{Config, Strategy},
    icmp::{IcmpPoller, TracertPoller},
    models::LogEvent,
    poller::Poller,
    sender::{EventSender, JsonSender},
    snmp::SnmpPoller,
};

fn load_config(path: &str) -> Result<Config, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("❌ Не удалось прочитать '{}': {}", path, e))?;

    toml::from_str(&contents).map_err(|e| {
        format!("❌ Ошибка в config.toml: {}", user_friendly_error(&e))
    })
}

fn user_friendly_error(e: &toml::de::Error) -> String {
    let msg = e.message();

    // Пропущенное поле
    if msg.contains("missing field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!("отсутствует поле '{}'", field);
        }
        return "отсутствует обязательное поле".to_string();
    }

    // Неизвестное поле
    if msg.contains("unknown field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!(
                "неизвестное поле '{}' — проверьте название",
                field
            );
        }
    }

    // Неверный тип
    if msg.contains("invalid type") {
        return "неверный тип значения (например, строка вместо числа)"
            .to_string();
    }

    // Всё остальное
    msg.to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_appender = tracing_appender::rolling::never(".", "app.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_env_filter("info,async_snmp=off")
        .with_writer(non_blocking)
        .with_target(false)
        .init();

    tracing::info!("Monitor starting");

    // Читаем конфиг
    let config = match load_config("config.toml") {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("\n{}", e);
            eprintln!("   Исправьте config.toml и перезапустите программу\n");
            error!("Некорректный config: {e}");
            std::process::exit(1);
        }
    };

    // Каналы
    let (tx_log, rx_log) = mpsc::channel::<LogEvent>(256);

    let session_id = Uuid::new_v4();

    let json_sender =
        match JsonSender::new(&config.log, session_id.to_string()).await {
            Ok(sender) => sender,
            Err(e) => {
                error!("{e}");
                std::process::exit(1);
            }
        };

    let senders: Vec<Box<dyn EventSender + Send>> = vec![Box::new(json_sender)];

    // Запускаем обработчик событий
    tokio::spawn(event_loop::handle_events(rx_log, senders));
    info!("Обработчик запросов запущен");

    match config.strategy {
        Strategy::Independent => {
            if config.independent.ping.enabled {
                let tracert = if config.ping.fallback_tracert {
                    match TracertPoller::new(
                        config.network.target.clone(),
                        config.tracert.max_hops,
                        config.tracert.probe_timeout_seconds,
                        config.tracert.queries_per_hop,
                    ) {
                        Ok(t) => Some(t),
                        Err(e) => {
                            error!("Ошибка инициализации Tracert: {}", e);
                            std::process::exit(1);
                        }
                    }
                } else {
                    None
                };

                let icmp = match IcmpPoller::new(
                    config.network.target.clone(),
                    config.ping.timeout_seconds,
                    tracert,
                ) {
                    Ok(poller) => poller,
                    Err(e) => {
                        error!("Ошибка инициализации IcmpPoller: {}", e);
                        std::process::exit(1);
                    }
                };
                let real_poller = Poller::new(
                    icmp,
                    config.independent.ping.interval_seconds,
                    tx_log.clone(),
                );

                tokio::spawn(real_poller.run());

                info!("Независимый ICMP опрос запущен");
            }

            if config.independent.snmp.enabled {
                let snmp_poller = match SnmpPoller::new(
                    config.network.target,
                    config.snmp.port,
                    config.snmp.community,
                    config.snmp.oids,
                    config.independent.snmp.interval_seconds,
                    config.snmp.timeout_seconds,
                    config.snmp.retries,
                )
                .await
                {
                    Ok(snmp_poller) => snmp_poller,
                    Err(e) => {
                        error!("Ошибка создания опроса snmp: {e}");
                        std::process::exit(1);
                    }
                };
                let real_snmp_poller = Poller::new(
                    snmp_poller,
                    config.independent.snmp.interval_seconds,
                    tx_log.clone(),
                );
                tokio::spawn(real_snmp_poller.run());
                info!("Независимый SNMP опрос запущен");
            }

            //  if config.independent.snmp.enabled {
            //      tokio::spawn(tasks::snmp::snmp_task(
            //          config.network.target.to_string(),
            //          config.snmp.port,
            //          config.snmp.community.clone(),
            //          config.snmp.oids.clone(),
            //          config.independent.snmp.interval_seconds,
            //          config.snmp.timeout_seconds,
            //          config.snmp.retries,
            //          tx_log.clone(),
            //      ));
            //      info!("SNMP задача запущена");
            //  }
        }
        Strategy::Synchronized => {
            info!("Sequential стратегия пока не реализована");
            // TODO: реализация
        }
    }

    // Бесконечное ожидание (правильный способ)
    tokio::signal::ctrl_c().await?;
    info!("Завершение работы...");
    Ok(())
}
