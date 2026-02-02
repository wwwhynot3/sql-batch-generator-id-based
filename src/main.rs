use std::{
    fs::File,
    io::{self, BufWriter, Write},
};

use anyhow::Context;
use inquire::{CustomType, Editor};
use regex::Regex;

struct IdSlicer {
    start_id: i128,
    end_id: i128,
    batch_size: usize,
}

impl IdSlicer {
    fn build(start_id: i128, end_id: i128, batch_size: usize) -> Result<Self, String> {
        if start_id > end_id {
            return Err("End Id must be bigger than Start Id".to_string());
        } else if batch_size == 0 {
            return Err("Batch Size must be bigger than 0".to_string());
        }
        Ok(IdSlicer {
            start_id,
            end_id,
            batch_size,
        })
    }

    fn slice_id(&self) -> impl Iterator<Item = (i128, i128)> {
        let batch_size = self.batch_size as i128;
        (self.start_id..=self.end_id)
            .step_by(self.batch_size)
            .map(move |start| (start, start + batch_size - 1))
    }
}
struct SqlSlicerTempate {
    prefix: String,
    condition: Option<String>,
}
impl SqlSlicerTempate {
    fn from(raw_sql: &str) -> Self {
        let og = Regex::new(r"(?is)\s+(ORDER|LIMIT).*$")
            .expect("unable to create regex pattern")
            .replace(raw_sql.trim().trim_end_matches(";"), "")
            .to_string();
        let fd = Regex::new(r"(?is)\s+WHERE\s+").expect("unable to create regex pattern");
        if let Some(mat) = fd.find_iter(&og).last() {
            SqlSlicerTempate {
                prefix: og[..mat.start()].trim().to_string(),
                condition: Some(og[mat.end()..].trim().to_string()),
            }
        } else {
            SqlSlicerTempate {
                prefix: og,
                condition: None,
            }
        }
    }
    fn redener_to<W: Write>(&self, slice: &IdSlicer, mut writer: W) -> io::Result<()> {
        let suffix = self
            .condition
            .as_ref()
            .map(|c| format!(" AND ({})", c))
            .unwrap_or_default();
        slice
            .slice_id()
            .map(|(start, end)| {
                writeln!(
                    writer,
                    "{} WHERE id BETWEEN {} AND {}{};",
                    self.prefix, start, end, suffix
                )
            })
            .find(|res| res.is_err())
            .unwrap_or_else(|| Ok(()))
    }
}
fn main() -> anyhow::Result<()> {
    println!("========SQL SLICER BY ID==========");
    let start_id = CustomType::<i128>::new("Start Id:")
        .with_error_message("please type valid integer")
        .prompt()?;
    let end_id = CustomType::<i128>::new("End id:")
        .with_validator(move |&input: &i128| {
            if input < start_id {
                Ok(inquire::validator::Validation::Invalid(
                    "End Id must be bigger than Start Id".into(),
                ))
            } else {
                Ok(inquire::validator::Validation::Valid)
            }
        })
        .prompt()?;
    let batch_size = CustomType::<usize>::new("Batch Size:")
        .with_default(50000)
        .with_validator(|batch_size: &usize| {
            if *batch_size == 0 {
                Ok(inquire::validator::Validation::Invalid(
                    "Batch Size must be bigger than 0".into(),
                ))
            } else {
                Ok(inquire::validator::Validation::Valid)
            }
        })
        .prompt()?;
    let raw_sql = Editor::new("Input Orignal Sql")
        .with_file_extension(".sql")
        .with_help_message("save and exit editor after compeleting inputing")
        .prompt()?;

    if raw_sql.is_empty() {
        println!("no invalid sql detected, exit")
    }
    println!("-----------GENERATING-------------");
    let id_slicer =
        IdSlicer::build(start_id, end_id, batch_size).map_err(|msg| anyhow::Error::msg(msg))?;
    let template = SqlSlicerTempate::from(&raw_sql);
    let file = File::create("id_slice.sql").context("Unable to create file")?;

    let mut wirter = BufWriter::new(file);
    template.redener_to(&id_slicer, &mut wirter)?;
    wirter.flush()?;
    println!("Generated SQLs has been saved to id_slice.sql");
    anyhow::Ok(())
}
