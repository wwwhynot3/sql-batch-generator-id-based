use std::{
    env,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::LazyLock,
};

use anyhow::{Context, anyhow};
use clap::Parser;
use console::style;
use dialoguer::{Editor, Input, Select, theme::ColorfulTheme};
use regex::Regex;

const DEFAULT_BATCH_SIZE: usize = 50_000;
const DEFAULT_OUTPUT: &str = "id_slice.sql";

static TAILING_ORDER_LIMIT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)\s+(ORDER\s+BY|LIMIT)\b.*$")
        .expect("unable to create ORDER BY/LIMIT regex pattern")
});
static WHERE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\s+WHERE\s+").expect("unable to create WHERE regex"));

#[derive(Debug, Parser)]
#[command(
    name = "sql-id-slicer",
    version,
    about = "Split one SQL into multiple id-based batches"
)]
struct CliArgs {
    #[arg(long, short = 's')]
    start_id: Option<i128>,
    #[arg(long, short = 'e')]
    end_id: Option<i128>,
    #[arg(long, short = 'b', default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,
    #[arg(long, short = 'q', help = "Raw SQL text")]
    sql: Option<String>,
    #[arg(long, short = 'f', help = "Read SQL from file path")]
    sql_file: Option<PathBuf>,
    #[arg(long, short = 'o', default_value = DEFAULT_OUTPUT)]
    output: PathBuf,
}

#[derive(Debug)]
struct RunConfig {
    start_id: i128,
    end_id: i128,
    batch_size: usize,
    raw_sql: String,
    output_path: PathBuf,
}

struct IdSlicer {
    start_id: i128,
    end_id: i128,
    batch_size: usize,
}

impl IdSlicer {
    fn build(start_id: i128, end_id: i128, batch_size: usize) -> Result<Self, String> {
        if start_id > end_id {
            return Err("End Id must be bigger than Start Id".to_string());
        }
        if batch_size == 0 {
            return Err("Batch Size must be bigger than 0".to_string());
        }
        Ok(Self {
            start_id,
            end_id,
            batch_size,
        })
    }

    fn slice_id(&self) -> impl Iterator<Item = (i128, i128)> + '_ {
        let batch_size = self.batch_size as i128;
        // 按批次步进切分 ID 区间，并确保最后一批不会超过 end_id。
        (self.start_id..=self.end_id)
            .step_by(self.batch_size)
            .map(move |start| (start, (start + batch_size - 1).min(self.end_id)))
    }
}

struct SqlSlicerTemplate {
    prefix: String,
    condition: Option<String>,
}

impl SqlSlicerTemplate {
    fn from(raw_sql: &str) -> anyhow::Result<Self> {
        let trimmed = raw_sql.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("Input SQL must not be empty"));
        }

        // 先去掉结尾分号，再剔除尾部 ORDER BY/LIMIT，避免影响分批 SQL 的语义。
        let cleaned = TAILING_ORDER_LIMIT_RE
            .replace(trimmed.trim_end_matches(';'), "")
            .to_string();

        // 取最后一个 WHERE，将 SQL 拆分为「前缀」和「原条件」。
        if let Some(mat) = WHERE_RE.find_iter(&cleaned).last() {
            Ok(Self {
                prefix: cleaned[..mat.start()].trim().to_string(),
                condition: Some(cleaned[mat.end()..].trim().to_string()),
            })
        } else {
            Ok(Self {
                prefix: cleaned.trim().to_string(),
                condition: None,
            })
        }
    }

    fn render_to<W: Write>(&self, slice: &IdSlicer, mut writer: W) -> io::Result<()> {
        let suffix = self
            .condition
            .as_ref()
            .map(|condition| format!(" AND ({condition})"))
            .unwrap_or_default();

        // 按批次生成 SQL 并流式写入文件，避免一次性构造超大字符串。
        for (start, end) in slice.slice_id() {
            writeln!(
                writer,
                "{} WHERE id BETWEEN {} AND {}{};",
                self.prefix, start, end, suffix
            )?;
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let config = collect_config()?;
    run(config)
}

fn collect_config() -> anyhow::Result<RunConfig> {
    if env::args_os().len() == 1 {
        return collect_config_interactive();
    }
    collect_config_from_args(CliArgs::parse())
}

fn collect_config_from_args(args: CliArgs) -> anyhow::Result<RunConfig> {
    let start_id = args
        .start_id
        .ok_or_else(|| anyhow!("--start-id is required when using argument mode"))?;
    let end_id = args
        .end_id
        .ok_or_else(|| anyhow!("--end-id is required when using argument mode"))?;

    let raw_sql = read_sql_from_sources(args.sql, args.sql_file)?;
    Ok(RunConfig {
        start_id,
        end_id,
        batch_size: args.batch_size,
        raw_sql,
        output_path: args.output,
    })
}

fn collect_config_interactive() -> anyhow::Result<RunConfig> {
    let theme = ColorfulTheme::default();

    println!();
    println!(
        "{}",
        style(" SQL BATCH SLICER ")
            .black()
            .on_cyan()
            .bold()
            .underlined()
    );
    println!("{}", style("Split SQL safely by id range").dim());
    println!();

    let start_id: i128 = Input::with_theme(&theme)
        .with_prompt("Start ID")
        .interact_text()?;

    let end_id: i128 = Input::with_theme(&theme)
        .with_prompt("End ID")
        .validate_with(|value: &i128| {
            if *value < start_id {
                Err("End ID must be greater than or equal to Start ID")
            } else {
                Ok(())
            }
        })
        .interact_text()?;

    let batch_size: usize = Input::with_theme(&theme)
        .with_prompt("Batch size")
        .default(DEFAULT_BATCH_SIZE)
        .validate_with(|value: &usize| {
            if *value == 0 {
                Err("Batch size must be greater than 0")
            } else {
                Ok(())
            }
        })
        .interact_text()?;

    let source_options = ["Edit SQL in your editor", "Load SQL from file"];
    let source = Select::with_theme(&theme)
        .with_prompt("SQL source")
        .default(0)
        .items(&source_options)
        .interact()?;

    let raw_sql = if source == 0 {
        let edited = Editor::new()
            .extension(".sql")
            .edit("")?
            .ok_or_else(|| anyhow!("No SQL input detected from editor"))?;
        ensure_non_empty_sql(edited)?
    } else {
        let file_path: String = Input::with_theme(&theme)
            .with_prompt("SQL file path")
            .interact_text()?;
        read_sql_file(Path::new(file_path.trim()))?
    };

    let output_name: String = Input::with_theme(&theme)
        .with_prompt("Output file")
        .default(DEFAULT_OUTPUT.to_string())
        .interact_text()?;

    Ok(RunConfig {
        start_id,
        end_id,
        batch_size,
        raw_sql,
        output_path: PathBuf::from(output_name.trim()),
    })
}

fn read_sql_from_sources(sql: Option<String>, sql_file: Option<PathBuf>) -> anyhow::Result<String> {
    match (sql, sql_file) {
        (Some(_), Some(_)) => Err(anyhow!("Please provide only one of --sql or --sql-file")),
        (Some(sql_text), None) => ensure_non_empty_sql(sql_text),
        (None, Some(file_path)) => read_sql_file(&file_path),
        (None, None) => Err(anyhow!(
            "One of --sql or --sql-file is required when using argument mode"
        )),
    }
}

fn read_sql_file(path: &Path) -> anyhow::Result<String> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Unable to read SQL file: {}", path.display()))?;
    ensure_non_empty_sql(content)
}

fn ensure_non_empty_sql(sql: String) -> anyhow::Result<String> {
    if sql.trim().is_empty() {
        return Err(anyhow!("Input SQL must not be empty"));
    }
    Ok(sql)
}

fn run(config: RunConfig) -> anyhow::Result<()> {
    println!("{}", style("Generating batched SQL...").cyan());

    let id_slicer = IdSlicer::build(config.start_id, config.end_id, config.batch_size)
        .map_err(anyhow::Error::msg)?;
    let template = SqlSlicerTemplate::from(&config.raw_sql)?;

    let file = File::create(&config.output_path)
        .with_context(|| format!("Unable to create file: {}", config.output_path.display()))?;
    let mut writer = BufWriter::new(file);

    template.render_to(&id_slicer, &mut writer)?;
    writer.flush()?;

    println!(
        "{} {}",
        style("Generated SQL has been saved to").green(),
        style(config.output_path.display()).bold()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{IdSlicer, SqlSlicerTemplate};

    #[test]
    fn slice_id_caps_last_batch_at_end_id() {
        let slicer = IdSlicer::build(1, 105, 50).expect("id slicer should be built");
        let slices = slicer.slice_id().collect::<Vec<_>>();
        assert_eq!(slices, vec![(1, 50), (51, 100), (101, 105)]);
    }

    #[test]
    fn render_sql_with_existing_where() {
        let slicer = IdSlicer::build(1, 100, 50).expect("id slicer should be built");
        let template = SqlSlicerTemplate::from(
            "UPDATE users SET active = 0 WHERE created_at < '2023-01-01' ORDER BY id DESC LIMIT 10",
        )
        .expect("template should be parsed");

        let mut buffer = Vec::new();
        template
            .render_to(&slicer, &mut buffer)
            .expect("render should succeed");
        let actual = String::from_utf8(buffer).expect("utf8 output");

        assert_eq!(
            actual,
            "UPDATE users SET active = 0 WHERE id BETWEEN 1 AND 50 AND (created_at < '2023-01-01');\n\
             UPDATE users SET active = 0 WHERE id BETWEEN 51 AND 100 AND (created_at < '2023-01-01');\n"
        );
    }

    #[test]
    fn render_sql_without_existing_where() {
        let slicer = IdSlicer::build(10, 20, 6).expect("id slicer should be built");
        let template = SqlSlicerTemplate::from("DELETE FROM users ORDER BY id DESC LIMIT 5")
            .expect("template should be parsed");

        let mut buffer = Vec::new();
        template
            .render_to(&slicer, &mut buffer)
            .expect("render should succeed");
        let actual = String::from_utf8(buffer).expect("utf8 output");

        assert_eq!(
            actual,
            "DELETE FROM users WHERE id BETWEEN 10 AND 15;\n\
             DELETE FROM users WHERE id BETWEEN 16 AND 20;\n"
        );
    }

    #[test]
    fn parse_sql_keeps_orders_table_name() {
        let template = SqlSlicerTemplate::from("DELETE FROM orders WHERE status = 'stale';")
            .expect("template should be parsed");
        let slicer = IdSlicer::build(1, 1, 1).expect("id slicer should be built");

        let mut buffer = Vec::new();
        template
            .render_to(&slicer, &mut buffer)
            .expect("render should succeed");
        let actual = String::from_utf8(buffer).expect("utf8 output");

        assert_eq!(
            actual,
            "DELETE FROM orders WHERE id BETWEEN 1 AND 1 AND (status = 'stale');\n"
        );
    }
}
