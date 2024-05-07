mod reservoir;
mod filesplits;

use reservoir::Reservoir;
use filesplits::get_splits;
use rayon::prelude::*;
use clap::{CommandFactory, Parser, ArgAction, ValueEnum, error::ErrorKind};
use std::io::{self, stdin, stdout, BufRead, BufReader, Seek};
use std::fs::File;
use prettytable::{Table, Row, Cell, format};
use serde::Serialize;
use serde_json::to_writer_pretty;

#[derive(ValueEnum, Debug, Clone)]
enum DisplayFormat {
    Table,
    Json,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Reservoir sample size
    #[arg(short='n', long="num-samples", default_value="1000")]
    sample_size: usize,

    /// Display "top-k" items from sample histogram
    #[arg(short='k', long="num-results", default_value="10")]
    num_results: u32,

    /// Fields to sample, indexed from 0
    #[arg(short='f', long="field-index", action=ArgAction::Append)]
    fields: Vec<usize>,

    /// Field separator, if unspecified then `char::is_whitespace` will be used.
    #[arg(short='s', long="field-separator")]
    field_separator: Option<String>,

    /// Input file, if unspecified then read from stdin.
    #[arg(short='i', long="input-file")]
    input_file: Option<String>,

    /// Format the output as either a table (the default) or JSON.
    #[clap(value_enum, short='o', long="output-format", default_value="table")]
    output_format: DisplayFormat,

    /// For multi-threaded file processing, approximate size of each input chunk, in bytes.
    /// Ignored when `-i` is not present.
    #[clap(value_enum, short='c', long="split-size", default_value="33554432")]
    split_size: u64,
}

#[derive(Debug)]
struct SampledFields {
    /// The reservoirs built from reading the input data, one per field
    reservoirs: Vec<Reservoir<String>>,
    
    /// The number of fields in the input data that could not be totally processed
    /// (for example because the record wasn't long enough), counted separately per field.
    missing_field_counts: Vec<u64>,
}

impl SampledFields {
    /// Merges two `SampledFields`, creating a new struct with the combined results. Used to
    /// `reduce` the output of parallel calls to `process_reader`.
    fn merge(pr1: &SampledFields, pr2: &SampledFields) -> SampledFields {
        let reservoirs: Vec<Reservoir<String>> = pr1.reservoirs.iter().zip(pr2.reservoirs.iter()).map( |(r1, r2)| {
            Reservoir::merge(r1, r2)
        }).collect();
        let missing_field_counts: Vec<u64> = pr1.missing_field_counts.iter().zip(pr2.missing_field_counts.iter()).map(|(fc1, fc2)| {
            fc1 + fc2
        }).collect();
        SampledFields {
            reservoirs,
            missing_field_counts,
        }
    }
}

/// Build one or more reservoirs by reading line-separated records from a buffered reader.
/// 
/// This function is meant to be used with 2 sources:
/// - stdin, in which case this function should consume the whole stream and `read_limit` should not be specified
/// - a predetermined chunk of a file, in which case `reader` should be `seek`ed to the starting point and `read_limit` should
///   indicate the end of the chunk.
fn process_reader<T:BufRead>(reader: T, read_limit: Option<u64>, args: &Args) -> SampledFields {
    let mut read_count: u64 = 0;
    if args.fields.len() == 0 {
        // No fields were specified so just process the whole line in one reservoir.
        let mut reservoir = Reservoir::new(args.sample_size as usize);
        for record in reader.lines() {
            let record = record.unwrap();
            read_count += record.len() as u64;
            if read_limit.is_some() && read_count > read_limit.unwrap() {
                break;
            }
            reservoir.add(record);
        }
        SampledFields {
            reservoirs: vec![reservoir],
            missing_field_counts: vec![0],
        }
    } else {
        let mut reservoirs: Vec<Reservoir<String>> = (0..args.fields.len())
            .map(|_| Reservoir::new(args.sample_size))
            .collect();
        let mut missing_field_counts: Vec<u64> = vec![0; args.fields.len()];
        for record in reader.lines() {
            let record = record.unwrap();
            read_count += record.len() as u64;
            if read_limit.is_some() && read_count > read_limit.unwrap() {
                break;
            }
            let fields: Vec<&str> = match &args.field_separator {
                None => record.split_whitespace().collect(),
                Some(separator) => record.split(separator).collect(),
            };
            for (reservoir_index, field_index ) in args.fields.iter().enumerate() {
                if *field_index >= fields.len() {
                    missing_field_counts[reservoir_index] += 1;
                } else {
                    reservoirs[reservoir_index].add(fields[*field_index].to_string())
                }
            }
        }
        SampledFields {
            reservoirs,
            missing_field_counts,
        }
    }
}

/// Build one or more reservoirs by reading line-separated records from a file.
/// [Rayon](https://docs.rs/rayon/latest/rayon/) is used to process chunks of the file in parallel.
fn process_file(args: &Args) -> io::Result<SampledFields> {
    let filename = args.input_file.clone().unwrap();
    let src = BufReader::new(File::open(&filename)?);
    let splits = get_splits(src, args.split_size)?;
    let result = splits.par_iter().map(|range| {
        let mut split_source = BufReader::new(File::open(&filename).unwrap());
        split_source.seek(io::SeekFrom::Start(range.start)).unwrap();
        process_reader(split_source, Some(range.end - range.start), &args)
    }).reduce_with(|sr1, sr2| {
        SampledFields::merge(&sr1, &sr2)
    }).unwrap();
    Result::Ok(result)
}

#[derive(Serialize)]
struct ValueFrequency<'a> {
    val: &'a String,
    freq: f32,
}

/// Crop a reservoir to its top-k sampled values.
fn histogram_top_k(reservoir: &Reservoir<String>, k: u32) -> Vec<ValueFrequency> {
    let histogram = reservoir.to_histogram();
    let mut vals = histogram.iter().map(|(k, v)| { (*v, *k) }).collect::<Vec<_>>();
    vals.sort_by_cached_key(|&(freq, val)| (freq.to_bits(), val.clone()));
    vals.reverse();
    vals[0..usize::min(k as usize, vals.len())].iter().map(|(freq, val)| ValueFrequency {
        val: *val,
        freq: *freq,
    }).collect()
}

fn display_table(pr: &SampledFields, args: &Args) {
    let top_k_fields: Vec<Vec<ValueFrequency>> = pr.reservoirs.iter().map(|r| histogram_top_k(r, args.num_results)).collect();
    let mut table = Table::new();
    let row_width = top_k_fields.len();
    if args.fields.len() > 0 {
        // Header row: field indexes if defined
        let header_cells: Vec<Cell> = args.fields.iter().map(|field_index| {
            Cell::new(&format!("field {}", field_index)).with_hspan(2)
        }).collect();
        table.add_row(Row::new(header_cells));
    }
    for row_index in 0..args.num_results as usize {
        // Table body
        let mut cells = Vec::with_capacity(row_width);
        for value_list in &top_k_fields {
            if row_index >= value_list.len() {
                cells.push(Cell::new(""));
                cells.push(Cell::new(""));
            } else {
                cells.push(Cell::new(&format!("{:.5}", value_list[row_index].freq)));
                cells.push(Cell::new(&value_list[row_index].val));
            }
        }
        table.add_row(Row::new(cells));
    }
    if pr.missing_field_counts.iter().any(|c| *c > 0) {
        // Footer row: missing field counts
        table.add_empty_row();
        let missing_cells: Vec<Cell> = pr.missing_field_counts.iter().flat_map(|c| match c {
            0 => vec![Cell::new(""), Cell::new("")],
            c => vec![
                Cell::new(&c.to_string()).style_spec("bFr"),
                Cell::new("<no value>").style_spec("bFr"),
            ]
        }).collect();
        table.add_row(Row::new(missing_cells));
    }
    table.set_format(*format::consts::FORMAT_CLEAN);
    table.printstd();
}

#[derive(Serialize)]
struct JsonOut<'a> {
    top_k_fields: Vec<Vec<ValueFrequency<'a>>>,
    missing_field_counts: Vec<u64>,
}

fn display_json(pr: &SampledFields, args: &Args) {
    let top_k_fields: Vec<Vec<ValueFrequency>> = pr.reservoirs.iter().map(|r| histogram_top_k(r, args.num_results)).collect();
    to_writer_pretty(stdout(), &JsonOut {
        top_k_fields,
        missing_field_counts: pr.missing_field_counts.clone(),
    }).unwrap();
}

fn main() {
    let args = Args::parse();
    if args.num_results > args.sample_size as u32 {
        Args::command().error(
            ErrorKind::ArgumentConflict,
            "num-results must be <= num-samples",
        ).exit();
    }
    let pr: SampledFields = if args.input_file.is_none() {
        process_reader(stdin().lock(), None, &args)
    } else {
        process_file(&args).unwrap()
    };
    match args.output_format {
        DisplayFormat::Table => display_table(&pr, &args),
        DisplayFormat::Json => display_json(&pr, &args)
    }
}