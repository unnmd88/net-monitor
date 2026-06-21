use clap::{Parser, Subcommand};

/// Сетевая диагностическая утилита
#[derive(Parser)]
#[command(name = "net-monitor")]
#[command(version = "1.0.0")]
#[command(about = "Сбор и анализ сетевых метрик", long_about = None)]
pub struct Cli {
    /// Путь к конфиг-файлу
    #[arg(short, long, global = true, default_value = "config.toml")]
    pub config: String,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Проанализировать логи
    Analyze {
        /// Путь к лог-файлу
        #[arg(short, long, default_value = "poller.log")]
        log_file: String,

        /// Детальный вывод
        #[arg(short, long, action)]
        detailed: bool,
    },

    /// Сгенерировать дефолтный конфиг
    GenerateConfig {
        /// Путь для сохранения
        #[arg(short, long, default_value = "config.toml")]
        output: String,

        /// Принудительно перезаписать существующий файл
        #[arg(short, long, action)]
        force: bool,

        /// Показать дефолтный конфиг в консоли (без сохранения)
        #[arg(long, short = 's', action)]
        show: bool,
    },
}
